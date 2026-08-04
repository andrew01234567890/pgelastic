/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

// InstanceQuiescer holds every client of one instance at the proxy, across every replica
// of the fleet, and gives them back afterwards.
//
// It is an interface so that the order the roll calls it in can be asserted rather than
// described, and so that an operator fronting no fleet can be given nothing: a role change
// with nobody holding the clients is simply the unheld one, which is what a headless
// deployment has always got.
type InstanceQuiescer interface {
	QuiesceInstance(ctx context.Context, pool client.ObjectKey, instance, holder string) error
	InstanceDrainStatus(ctx context.Context, pool client.ObjectKey, instance string) (proxy.InstanceDrain, error)
	ResumeInstance(ctx context.Context, pool client.ObjectKey, instance, holder string) error
	ReleaseInstance(ctx context.Context, pool client.ObjectKey, instance, holder string) error
}

// rollTarget is one member the roll owes a disruption to, and what asked for it.
type rollTarget struct {
	member string
	reason pgelasticv1alpha1.InstanceRollReason
}

// rollState is what the roll decided this pass, in the shape the status publishes.
type rollState struct {
	active    bool
	member    string
	reason    pgelasticv1alpha1.InstanceRollReason
	step      pgelasticv1alpha1.InstanceRollStep
	pending   int32
	startedAt *metav1.Time
	message   string
}

// rollRequeue paces the roll. It is far shorter than the provisioning ladder because two
// of the steps - a drain reaching zero, a promotion completing - are over in a second or
// two, and every reconcile spent not noticing is a second of clients held for nothing.
const rollRequeue = time.Second

// rollEvidenceMaxAge is how old the quorum record may be for the roll to remove a member on it.
//
// Much shorter than ha.MaxEvidenceAge, and for the opposite reason. That one gates a failover and
// is measured against the failing instant, because the primary is the only writer and its record
// stops being refreshed exactly when the failover needs it. Here the primary is alive and writing
// continuously, so "now" is the right reference and five minutes would be meaningless: the roll's
// own steps are seconds apart, and a record older than several of them describes a cluster that
// has already moved.
const rollEvidenceMaxAge = 30 * time.Second

// reconcileRoll restarts the members that are not running the current configuration, one
// at a time, replicas before the primary.
//
// The ordering is not a preference. Replicas first because the whole point is that the
// member holding the role is disrupted once and last, when the instance has already proven
// it can restart a member and come back; and because max_connections may only ever rise, so
// a standby must reach the higher value before the primary does or PostgreSQL refuses to
// start recovery on it. Most-lagged first because the member furthest behind is the one
// whose absence costs least and whose catch-up costs most, so it is given the longest run
// at it. The primary last, and never restarted underneath its clients: the role is handed
// away first, with the clients held at the proxy for the length of the handover.
//
// One member per pass, and no pass at all while the instance is short of a member. That is
// what keeps the synchronous quorum satisfied throughout: under dataDurability Required a
// second member going down while the first is still coming back stalls every commit on the
// instance, and a rolling restart that stalls commits is not one.
func (r *PgInstanceReconciler) reconcileRoll(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
	pods []corev1.Pod,
	decision ha.Decision,
) (rollState, error) {
	if settling, err := r.completeHandover(ctx, instance, pods); err != nil || settling {
		return r.stepping(instance, rollState{
			active:  true,
			member:  instance.Status.CurrentPrimary,
			reason:  rollReasonOf(instance),
			pending: pendingOf(instance),
		}, pgelasticv1alpha1.RollStepSwitchingOver,
			"waiting for the read-write Service to select "+instance.Status.CurrentPrimary), err
	}

	outstanding := r.outstandingMembers(ctx, instance, builder, pods)
	if len(outstanding) == 0 {
		return rollState{}, r.finishRoll(ctx, instance)
	}

	target := chooseTarget(instance.Status.Roll, outstanding)
	state := rollState{
		active:  true,
		member:  target.member,
		reason:  target.reason,
		pending: int32(len(outstanding)),
	}
	if target.member == instance.Status.CurrentPrimary {
		return r.rollPrimary(ctx, instance, pods, decision, state)
	}
	return r.rollMember(ctx, instance, pods, decision, state)
}

// rollMember restarts one member that is not the primary, by deleting its Pod.
//
// Deleting rather than asking the member to restart its postmaster in place, because the
// configuration reaches a member through a mounted ConfigMap and the kubelet refreshes
// that on its own schedule: a postmaster restarted in place can come back on exactly the
// file it stopped with, having spent a member's worth of redundancy to change nothing. A
// Pod created after the ConfigMap was written mounts the current one by construction.
func (r *PgInstanceReconciler) rollMember(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
	decision ha.Decision,
	state rollState,
) (rollState, error) {
	pod := podNamed(pods, state.member)
	if pod == nil || !pod.DeletionTimestamp.IsZero() {
		return r.stepping(instance, state, pgelasticv1alpha1.RollStepRestarting,
			state.member+" is being recreated on the current configuration"), nil
	}
	if fit, why := fitToRoll(instance, pods, decision, state.member); !fit {
		return r.stepping(instance, state, pgelasticv1alpha1.RollStepBlocked, why), nil
	}

	logf.FromContext(ctx).Info("rolling a member", "member", state.member, "reason", state.reason)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return state, err
	}
	return r.stepping(instance, state, pgelasticv1alpha1.RollStepRestarting,
		state.member+" is being recreated on the current configuration"), nil
}

// rollPrimary hands the role away before the member holding it is disrupted.
//
// The sequence is the one thing in this file that is not obvious, and each step exists
// because of what the step after it would otherwise do to a client:
//
//  1. hold every client of the instance at the proxy, and wait until nothing is in flight.
//     Draining is not politeness - it is the precondition that keeps the undecidable case
//     empty, because a commit that was forwarded and never answered is counted in flight
//     until it is answered, so a handover that waits for the drain cannot happen while one
//     is in the air.
//  2. name the member in the maintenance annotation. That, and only then, is what makes the
//     failover decision choose a successor, and what tells the member's own agent that the
//     stop it is about to be asked for was chosen rather than forced - which is the
//     difference between a clean shutdown its next start rewinds from and an immediate one
//     its next start crash-recovers from.
//  3. once another member holds the role, give the clients back. They resume against the
//     new primary on the same sockets they were holding.
//  4. only then recreate the old primary's Pod, which by now is a standby like any other.
//
// The order of 1 and 2 is the whole design: naming first would start the handover while
// clients were still admitting traffic, which is the case that drops them.
func (r *PgInstanceReconciler) rollPrimary(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
	decision ha.Decision,
	state rollState,
) (rollState, error) {
	log := logf.FromContext(ctx)
	if ha.UnderMaintenance(instance.GetAnnotations(), state.member) {
		return r.stepping(instance, state, pgelasticv1alpha1.RollStepSwitchingOver,
			fmt.Sprintf("%s is handing the primary role to %s",
				state.member, targetPrimaryAfter(instance, decision))), nil
	}
	if stalled := stillStalled(instance, state.member); stalled != nil {
		return *stalled, nil
	}
	if fit, why := fitToRoll(instance, pods, decision, state.member); !fit {
		return r.stepping(instance, state, pgelasticv1alpha1.RollStepBlocked, why), nil
	}

	drain, err := r.holdClients(ctx, instance)
	if err != nil {
		return state, err
	}
	if drain.Known && !drain.Drained {
		state = r.stepping(instance, state, pgelasticv1alpha1.RollStepQuiescing, fmt.Sprintf(
			"holding %d client transactions off %s; %d backends are still in flight",
			drain.Queued, state.member, drain.InFlight))
		return r.abandonIfStuck(ctx, instance, state, drain)
	}

	log.Info("handing the primary role away before restarting it",
		"member", state.member, "reason", state.reason, "queued", drain.Queued)
	if err := r.setMaintenance(ctx, instance, state.member); err != nil {
		return state, err
	}
	return r.stepping(instance, state, pgelasticv1alpha1.RollStepSwitchingOver,
		state.member+" is handing the primary role away"), nil
}

// abandonIfStuck gives the clients back when a drain is never going to arrive.
//
// A session that has pinned its backend - one holding temporary tables, a LISTEN
// registration or a session advisory lock - never returns it, so the instance never
// reports drained and the roll would otherwise queue every other client behind a handover
// that is not going to happen. The budget is the switchover timeout, which is what the API
// says bounds a role change end to end. Giving up releases the hold and says so: a roll
// that cannot proceed is an operator's decision to make, and it is made from a message
// rather than from a hang.
func (r *PgInstanceReconciler) abandonIfStuck(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	state rollState,
	drain proxy.InstanceDrain,
) (rollState, error) {
	budget := provision.SwitchoverTimeout(instance.Spec)
	if state.startedAt == nil || time.Since(state.startedAt.Time) < budget {
		return state, nil
	}
	if err := r.releaseClients(ctx, instance); err != nil {
		return state, err
	}
	state.step = pgelasticv1alpha1.RollStepStalled
	state.message = fmt.Sprintf(
		"%s could not be handed away: %d backends were still in flight after %s, so the "+
			"clients were given back. A session that pins its backend has to end before "+
			"this instance can be rolled; the next attempt is in %s",
		state.member, drain.InFlight, budget, rollStalledBackoff)
	state.startedAt = &metav1.Time{Time: time.Now()}
	return state, nil
}

// rollStalledBackoff is how long the roll leaves an instance alone after giving up on a
// drain.
//
// It is deliberately far longer than the drain budget rather than a little longer. Trying
// again immediately means holding every client on the instance for another whole budget,
// releasing them, and doing it again - the tenants would spend half their life queued
// behind a handover that a pinned session is never going to allow. Waiting is the correct
// answer because nothing the operator can do resolves it: the session has to end.
const rollStalledBackoff = 10 * time.Minute

// stillStalled keeps the roll's hands off an instance whose clients would not drain, until
// the backoff has run out. It returns the state to republish, or nil to carry on.
func stillStalled(
	instance *pgelasticv1alpha1.PgInstance,
	member string,
) *rollState {
	previous := instance.Status.Roll
	if previous == nil || previous.Member != member ||
		previous.Step != pgelasticv1alpha1.RollStepStalled || previous.StartedAt == nil {
		return nil
	}
	if time.Since(previous.StartedAt.Time) >= rollStalledBackoff {
		return nil
	}
	return &rollState{
		active:    true,
		member:    member,
		reason:    previous.Reason,
		step:      pgelasticv1alpha1.RollStepStalled,
		pending:   previous.Pending,
		startedAt: previous.StartedAt.DeepCopy(),
		message:   previous.Message,
	}
}

// completeHandover gives the clients back once another member holds the role, and reports
// whether it is still waiting to be able to.
//
// The wait is for the read-write Service, not for the promotion. The proxy opens every
// backend connection through that Service, and it does not retry a refused one - it
// answers the client. So a client released while the Service still selects the member that
// has just stopped is a client that gets an error out of a switchover whose whole claim is
// that it produces none. The endpoints are the last thing in the chain to move and the
// only one the operator can see move.
func (r *PgInstanceReconciler) completeHandover(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
) (bool, error) {
	named := ha.MaintenanceMembers(instance.GetAnnotations())
	primary := instance.Status.CurrentPrimary
	if len(named) == 0 || primary == "" || slices.Contains(named, primary) {
		return false, nil
	}
	if !r.readWriteEndpointsSettled(ctx, instance, pods) {
		return true, nil
	}
	logf.FromContext(ctx).Info("the primary role has moved; releasing the held clients",
		"from", named, "to", primary)
	if err := r.resumeClients(ctx, instance); err != nil {
		return false, err
	}
	return false, r.setMaintenance(ctx, instance)
}

// clusterReadTimeout bounds the two reads the roll makes outside its own objects.
//
// It exists because a cached client blocks until the informer for a type has synced, and an
// informer for a type the operator has not been granted retries forever rather than
// failing. That wedges the reconcile - and with one worker, every instance the controller
// owns. It is not hypothetical: a deployed operator whose ClusterRole predated these two
// reads stopped reconciling entirely, mid-roll, with the clients still held.
const clusterReadTimeout = 5 * time.Second

// readWriteEndpointsSettled reports whether the read-write Service now selects the current
// primary and nothing else. An instance with no EndpointSlices at all is one whose Service
// has not been observed yet, which is not settled.
//
// A read that fails falls back to the role label. That is a weaker answer - the label is
// the operator's own input to the Service rather than the Service's output - but it is the
// answer that keeps a roll moving, and a roll that cannot move is a roll holding every
// client on the instance until its lease runs out.
func (r *PgInstanceReconciler) readWriteEndpointsSettled(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
) bool {
	primary := podNamed(pods, instance.Status.CurrentPrimary)
	if primary == nil || primary.Status.PodIP == "" {
		return false
	}
	readCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
	defer cancel()

	endpointSlices := &discoveryv1.EndpointSliceList{}
	if err := r.List(readCtx, endpointSlices, client.InNamespace(instance.Namespace),
		client.MatchingLabels{
			discoveryv1.LabelServiceName: provision.PrimaryServiceName(instance.Name),
		}); err != nil {
		logf.FromContext(ctx).V(1).Info("falling back to the role label for the read-write Service",
			"error", err)
		return primary.Labels[provision.LabelRole] == string(pgelasticv1alpha1.InstanceRolePrimary)
	}
	var selected int
	for _, endpointSlice := range endpointSlices.Items {
		for _, endpoint := range endpointSlice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			if !slices.Contains(endpoint.Addresses, primary.Status.PodIP) {
				return false
			}
			selected++
		}
	}
	return selected > 0
}

// rollReasonOf recovers what the roll in progress was for, so that finishing a handover
// does not have to recompute a decision that has already been made.
func rollReasonOf(instance *pgelasticv1alpha1.PgInstance) pgelasticv1alpha1.InstanceRollReason {
	if roll := instance.Status.Roll; roll != nil && roll.Reason != "" {
		return roll.Reason
	}
	return pgelasticv1alpha1.RollReasonConfigChanged
}

// pendingOf carries the outstanding count across the one step that cannot recount it. The
// members left to roll are read from the Pods, and mid-handover the member being handed
// over is not yet in a state that count describes; publishing a fresh zero there would say
// the roll had finished.
func pendingOf(instance *pgelasticv1alpha1.PgInstance) int32 {
	if roll := instance.Status.Roll; roll != nil {
		return roll.Pending
	}
	return 1
}

// finishRoll is the end of a roll, and also the steady state of an instance that never
// needed one. It gives the clients back and unnames the member, in that order, because a
// member unnamed while the clients are still held is a handover nobody is going to finish.
func (r *PgInstanceReconciler) finishRoll(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) error {
	if holdingClients(instance) {
		if err := r.resumeClients(ctx, instance); err != nil {
			return err
		}
	}
	return r.setMaintenance(ctx, instance)
}

// holdingClients reads back whether the last published step was one that holds a lease.
//
// The status is the record rather than a field on the reconciler because the two things
// that end a hold - the roll finishing and the roll giving up - happen in later reconciles
// than the one that took it, and an operator restarted in between must still be able to
// tell that there is something to release.
func holdingClients(instance *pgelasticv1alpha1.PgInstance) bool {
	roll := instance.Status.Roll
	if roll == nil {
		return false
	}
	return roll.Step == pgelasticv1alpha1.RollStepQuiescing ||
		roll.Step == pgelasticv1alpha1.RollStepSwitchingOver
}

// outstandingMembers is every member the roll still owes a disruption to, in the order it
// intends to disrupt them.
func (r *PgInstanceReconciler) outstandingMembers(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
	pods []corev1.Pod,
) []rollTarget {
	desired := builder.DesiredStamp()
	primary := instance.Status.CurrentPrimary

	targets := make([]rollTarget, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		stamp := provision.StampOf(pod)
		switch {
		case stamp.ConfigHash != desired.ConfigHash:
			targets = append(targets, rollTarget{pod.Name, pgelasticv1alpha1.RollReasonConfigChanged})
		case stamp.RestartedAt != desired.RestartedAt:
			targets = append(targets, rollTarget{pod.Name, pgelasticv1alpha1.RollReasonRestartRequested})
		case pod.Name == primary && r.primaryNodeGoingAway(ctx, pods, primary):
			targets = append(targets, rollTarget{pod.Name, pgelasticv1alpha1.RollReasonNodeDraining})
		}
	}
	r.rankTargets(ctx, instance, pods, targets)
	return prependRecreating(instance, pods, targets)
}

// prependRecreating keeps the member whose Pod the roll has just deleted at the head of
// the list.
//
// A deleted Pod is not in the Pod list, so nothing above sees the member at all - and the
// roll would read that as one fewer member to do and move on to the next, which is the one
// thing it must never do while a member is missing. It is the member the roll is waiting
// for, so it is the member the roll is working on.
func prependRecreating(
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
	targets []rollTarget,
) []rollTarget {
	roll := instance.Status.Roll
	if roll == nil || roll.Member == "" || podNamed(pods, roll.Member) != nil {
		return targets
	}
	if slices.ContainsFunc(targets, func(t rollTarget) bool { return t.member == roll.Member }) {
		return targets
	}
	return append([]rollTarget{{roll.Member, rollReasonOf(instance)}}, targets...)
}

// rankTargets puts the replicas first, most-lagged first, and the primary last.
//
// Lag is read as the replay position rather than as a number of seconds: the position is
// what the member's own postmaster reports and what candidate selection already orders on,
// and a lag in seconds is zero for both a standby that is caught up and a standby whose
// primary has been idle.
func (r *PgInstanceReconciler) rankTargets(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
	targets []rollTarget,
) {
	replayed := map[string]ha.LSN{}
	members, _ := r.observeMembers(ctx, client.ObjectKeyFromObject(instance), pods)
	for _, member := range members {
		replayed[member.Name] = member.ReplayLSN
	}
	primary := instance.Status.CurrentPrimary
	slices.SortFunc(targets, func(a, b rollTarget) int {
		if (a.member == primary) != (b.member == primary) {
			if a.member == primary {
				return 1
			}
			return -1
		}
		if replayed[a.member] != replayed[b.member] {
			if replayed[a.member] < replayed[b.member] {
				return -1
			}
			return 1
		}
		return strings.Compare(a.member, b.member)
	})
}

// chooseTarget keeps working on the member the roll had already started, and otherwise
// takes the first of the ranked list.
//
// Sticking to it matters because the ranking moves: a standby that was furthest behind
// catches up while its Pod is being recreated, and a roll that re-ranked every pass would
// abandon a half-restarted member for whichever one is momentarily furthest behind.
func chooseTarget(current *pgelasticv1alpha1.InstanceRollStatus, outstanding []rollTarget) rollTarget {
	if current != nil {
		if i := slices.IndexFunc(outstanding, func(t rollTarget) bool {
			return t.member == current.Member
		}); i >= 0 {
			return outstanding[i]
		}
	}
	return outstanding[0]
}

// fitToRoll answers whether the instance can afford to disrupt target right now.
//
// Every clause is a way of already being one member short, or of being in the middle of
// deciding who the primary is. Disrupting a member in either state is how a rolling
// restart turns into an outage: the quorum "ANY 1" that a three-member instance commits
// against has exactly one member of slack, and the roll is spending it.
//
// The target is excused from its own clauses, and that is not a loophole. A member that
// has just handed its role away is down already; recreating its Pod now costs nothing that
// has not already been spent, and refusing until it has rewound itself back to Ready would
// disrupt the same member twice for one roll.
func fitToRoll(
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
	decision ha.Decision,
	target string,
) (bool, string) {
	replicas := replicasOf(instance)
	evidence := ha.EvidenceFrom(instance.Status.QuorumEvidence)
	rejoining := rejoiningMember(instance)
	switch {
	case decision.SplitBrain:
		return false, "two members report themselves out of recovery; no member is disrupted while that is true"
	case instance.Status.CurrentPrimary == "":
		return false, "no member holds the primary role yet"
	case ha.FailoverInProgress(instance.Status.CurrentPrimary, targetPrimaryAfter(instance, decision)):
		return false, "a failover is in flight"
	case int32(len(pods)) < replicas:
		return false, fmt.Sprintf("%d of %d members exist", len(pods), replicas)
	case otherTerminating(pods, target) != "":
		return false, otherTerminating(pods, target) + " is still terminating"
	case otherNotReady(pods, target) != "":
		return false, fmt.Sprintf("%d of %d members are Ready", readyMembers(pods), replicas)
	case rejoining != nil && rejoining.Name != target:
		return false, rejoining.Name + " is rebuilding itself onto the primary's history"
	case ha.WriteStalled(evidence):
		return false, "commits are already stalling on the synchronous quorum"
	case ha.EvidenceStale(evidence, time.Now(), rollEvidenceMaxAge):
		// The primary refreshes this record continuously while it is alive, so a stale one
		// during a roll means the picture is older than the roll's own steps. Deciding to
		// remove a member from it is deciding about a cluster that has since moved.
		return false, "the quorum evidence is too old to remove a member on"
	case ha.WouldStall(evidence, target):
		// The question the roll actually has, and not the one WriteStalled answers. With
		// ANY 1 over two standbys, WriteStalled stays false while either one streams - so a
		// member that has just been rolled and is Ready but not yet caught up leaves exactly
		// one streaming standby, and removing that one takes the instance to zero. Every
		// commit then blocks until a standby returns.
		return false, fmt.Sprintf(
			"%s is the last standby still streaming; removing it would stall every commit "+
				"until another has caught up", target)
	}
	return true, ""
}

func otherTerminating(pods []corev1.Pod, target string) string {
	for i := range pods {
		if pods[i].Name != target && !pods[i].DeletionTimestamp.IsZero() {
			return pods[i].Name
		}
	}
	return ""
}

func otherNotReady(pods []corev1.Pod, target string) string {
	for i := range pods {
		if pods[i].Name != target && !podReady(&pods[i]) {
			return pods[i].Name
		}
	}
	return ""
}

func podNamed(pods []corev1.Pod, name string) *corev1.Pod {
	for i := range pods {
		if pods[i].Name == name {
			return &pods[i]
		}
	}
	return nil
}

// primaryNodeGoingAway reports whether the primary sits on a node that has been made
// unschedulable while some other member does not.
//
// This is the drain trap, closed. The primary PodDisruptionBudget deliberately refuses to
// let the primary be evicted, so `kubectl drain` on its node blocks until a switchover -
// and until now nothing ever started one, so it blocked until a human intervened. Reading
// the node's own unschedulable flag is what turns that block into the switchover it was
// waiting for.
//
// The second half of the condition is what stops it becoming a loop. If every member is on
// a node that is going away, handing the role over moves it to a member with the same
// problem, and the pair would switch back and forth until the drain gave up. Refusing is
// correct there: no switchover can save an instance whose whole replica set is being
// evicted, and the roll says so in its status instead.
func (r *PgInstanceReconciler) primaryNodeGoingAway(
	ctx context.Context,
	pods []corev1.Pod,
	primary string,
) bool {
	pod := podNamed(pods, primary)
	if pod == nil || pod.Spec.NodeName == "" || !r.nodeUnschedulable(ctx, pod.Spec.NodeName) {
		return false
	}
	for i := range pods {
		other := &pods[i]
		if other.Name == primary || other.Spec.NodeName == "" || !podReady(other) {
			continue
		}
		if !r.nodeUnschedulable(ctx, other.Spec.NodeName) {
			return true
		}
	}
	return false
}

// nodeUnschedulable treats an unreadable node as schedulable. A roll that started a
// switchover because the operator could not read the API server would be reacting to its
// own failure - and the read is bounded because an unreadable node here means a cache that
// never syncs, which blocks rather than errors.
func (r *PgInstanceReconciler) nodeUnschedulable(ctx context.Context, name string) bool {
	readCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
	defer cancel()

	node := &corev1.Node{}
	if err := r.Get(readCtx, client.ObjectKey{Name: name}, node); err != nil {
		logf.FromContext(ctx).V(1).Info("could not read the node a member is on",
			"node", name, "error", err)
		return false
	}
	if node.Spec.Unschedulable {
		return true
	}
	return slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.Key == corev1.TaintNodeUnschedulable ||
			taint.Effect == corev1.TaintEffectNoExecute
	})
}

// holdClients takes the instance-scoped hold and reports what is still in flight behind
// it. A pool with no fleet in front of it answers that nothing is known, which the caller
// reads as "there are no clients to hold" and proceeds.
func (r *PgInstanceReconciler) holdClients(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) (proxy.InstanceDrain, error) {
	if r.Quiescer == nil {
		return proxy.InstanceDrain{}, nil
	}
	pool := poolKeyOf(instance)
	if err := r.Quiescer.QuiesceInstance(ctx, pool, instance.Name, rollHolder(instance)); err != nil {
		return proxy.InstanceDrain{}, err
	}
	return r.Quiescer.InstanceDrainStatus(ctx, pool, instance.Name)
}

func (r *PgInstanceReconciler) resumeClients(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) error {
	if r.Quiescer == nil {
		return nil
	}
	return r.Quiescer.ResumeInstance(ctx, poolKeyOf(instance), instance.Name, rollHolder(instance))
}

func (r *PgInstanceReconciler) releaseClients(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) error {
	if r.Quiescer == nil {
		return nil
	}
	return r.Quiescer.ReleaseInstance(ctx, poolKeyOf(instance), instance.Name, rollHolder(instance))
}

func poolKeyOf(instance *pgelasticv1alpha1.PgInstance) client.ObjectKey {
	return client.ObjectKey{Namespace: instance.Namespace, Name: instance.Spec.PoolRef.Name}
}

// rollHolder identifies the hold. It is stable for the whole of one instance's roll,
// because the renewal loop and the release are matched on it: a holder that changed
// between reconciles would take a second hold and orphan the first until its lease ran out.
func rollHolder(instance *pgelasticv1alpha1.PgInstance) string {
	return "roll/" + instance.Name
}

// setMaintenance names the members a roll is about to disrupt, or unnames them all.
//
// It is a merge patch of the annotation alone rather than an update of the object being
// reconciled, because the status apply later in the same pass is written from that object
// and an update would make the two race for its resource version.
func (r *PgInstanceReconciler) setMaintenance(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	members ...string,
) error {
	desired := strings.Join(members, ",")
	if instance.GetAnnotations()[ha.AnnotationMaintenance] == desired {
		return nil
	}
	patch := client.MergeFrom(instance.DeepCopy())
	annotations := instance.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	if desired == "" {
		delete(annotations, ha.AnnotationMaintenance)
	} else {
		annotations[ha.AnnotationMaintenance] = desired
	}
	instance.SetAnnotations(annotations)
	return r.Patch(ctx, instance, patch)
}

// stepping records what the roll is doing to the member it named, carrying the step's own
// start time forward for as long as the step has not changed. That timestamp is what tells
// a slow step from a stuck one, so restamping it every pass would erase the distinction.
func (r *PgInstanceReconciler) stepping(
	instance *pgelasticv1alpha1.PgInstance,
	state rollState,
	step pgelasticv1alpha1.InstanceRollStep,
	message string,
) rollState {
	state.step = step
	state.message = message
	state.startedAt = &metav1.Time{Time: time.Now()}
	if previous := instance.Status.Roll; previous != nil && previous.Member == state.member &&
		previous.Step == step && previous.StartedAt != nil {
		state.startedAt = previous.StartedAt.DeepCopy()
	}
	return state
}

// rollStatus is the roll's contribution to the status apply. An inactive roll contributes
// nothing at all, which is what removes the field from the object.
func rollStatus(state rollState) map[string]any {
	if !state.active {
		return nil
	}
	published := map[string]any{
		"member":  state.member,
		"reason":  string(state.reason),
		"step":    string(state.step),
		"pending": int64(state.pending),
		"message": state.message,
	}
	if state.startedAt != nil {
		published["startedAt"] = state.startedAt.UTC().Format(time.RFC3339)
	}
	return published
}

func rollCondition(instance *pgelasticv1alpha1.PgInstance, state rollState) map[string]any {
	if !state.active {
		return condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionRolling, false,
			instance.Generation, pgelasticv1alpha1.ReasonMembersCurrent,
			"every member is running the current configuration")
	}
	reason := pgelasticv1alpha1.ReasonRolling
	if state.step == pgelasticv1alpha1.RollStepBlocked ||
		state.step == pgelasticv1alpha1.RollStepStalled {
		reason = pgelasticv1alpha1.ReasonRollBlocked
	}
	return condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionRolling, true,
		instance.Generation, reason, state.message)
}
