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

package agent

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// crReadTimeout bounds the agent's read of its own PgInstance. It is short because the read
// happens on the observe tick and a slow API server must not stall the loop that answers
// the probes.
const crReadTimeout = 500 * time.Millisecond

// reconcileRole is the agent's whole share of the failover state machine.
//
// A primary holds the Lease and stops being a primary the moment it can prove it has lost
// it. A replica promotes itself only when the operator has named it in targetPrimary, and
// even then only behind the Lease and the quorum gate. Nothing here decides who should be
// the primary: that decision is the operator's alone, so a confused agent cannot promote
// itself and a dead operator cannot promote anybody.
func (s *Supervisor) reconcileRole(
	ctx context.Context,
	observation MemberObservation,
	instance *pgelasticv1alpha1.PgInstance,
) {
	if instance == nil {
		// Not being able to read the CR proves nothing about who the primary is. Acting on
		// it would be acting on the absence of evidence.
		return
	}
	switch observation.Role {
	case RolePrimary:
		s.holdPrimary(ctx, instance)
	case RoleReplica:
		s.promoteIfChosen(ctx, instance)
		s.followCurrentPrimary(ctx, instance)
		s.rejoinIfDiverged(ctx, observation, instance)
	case RoleUnknown:
	}
}

// divergenceGrace is how long a replica must hold the same position, with a primary that is
// not this member present, before divergence is evaluated at all.
//
// A standby reconnecting to a primary that has just moved stops advancing for a few seconds
// and is perfectly healthy. The grace is long enough that an ordinary reconnection never
// reaches the evaluation, and short enough that a member which can never reconnect is not
// left there: the diverged case does not resolve itself, it loops forever.
const divergenceGrace = 30 * time.Second

// rejoinIfDiverged is the trigger the split-brain catalogue's fourth row was missing.
//
// A member in recovery was previously assumed to be safe - its history had only ever been
// received, never written - and that assumption is wrong. A standby that received WAL past
// the point the new primary forked at holds records no other copy has; its WAL receiver is
// refused every time it asks, and it asks forever. So the trigger is a receiver that has
// stayed down, and the verdict is taken from the primary's own timeline history rather than
// from the shape of the error that produced it.
func (s *Supervisor) rejoinIfDiverged(
	ctx context.Context,
	observation MemberObservation,
	instance *pgelasticv1alpha1.PgInstance,
) {
	primary := instance.Status.CurrentPrimary
	if primary == "" || primary == s.options.Member ||
		ha.FailoverInProgress(primary, instance.Status.TargetPrimary) {
		s.clearStranded()
		return
	}
	// The clock is reset by WAL arriving, never by the WAL receiver being reported up. A
	// member that cannot stream has its receiver restarted every wal_retrieve_retry_interval
	// and killed again immediately, so pg_stat_wal_receiver holds a row often enough that a
	// check trusting the flag alone would keep resetting itself and never reach a verdict.
	// The flag is still worth consulting to skip the evaluation on a member that is plainly
	// fine.
	held := observation.HeldPosition()
	if s.noteProgress(held) || observation.WALReceiverActive || !s.strandedFor(divergenceGrace) {
		return
	}

	log := logf.FromContext(ctx)
	divergence, err := DetectDivergence(s.options.WALDir, held, PrimaryTimeline(instance, primary))
	if err != nil {
		log.Error(err, "could not read the timeline history while checking for divergence")
		return
	}
	if !divergence.Diverged {
		return
	}
	log.Info("this member's history has diverged from the primary's; rejoining",
		"member", s.options.Member, "primary", primary,
		"reason", divergence.Reason, "detail", divergence.Message)
	s.requestRejoin(ctx, primary)
}

// followCurrentPrimary repoints a running standby at whoever the primary is now.
//
// Without it a standby that was streaming from a member which has since been demoted keeps
// dialling that member forever. Once the old primary rejoins as a standby the connection
// even succeeds, as a cascading standby - which is worse than failing, because the new
// primary then never counts this member towards its quorum and every commit stalls against
// a topology that looks healthy from the outside.
//
// primary_conninfo is PGC_SIGHUP, so this is a file write and a reload; PostgreSQL restarts
// the WAL receiver on the change by itself.
func (s *Supervisor) followCurrentPrimary(ctx context.Context, instance *pgelasticv1alpha1.PgInstance) {
	primary := instance.Status.CurrentPrimary
	if primary == "" || primary == s.options.Member ||
		ha.FailoverInProgress(primary, instance.Status.TargetPrimary) {
		return
	}

	log := logf.FromContext(ctx)
	replication := CurrentReplicationConfig(s.options.DataDir)
	desired := PrimaryConnInfo(PeerHost(primary, s.options.PeerService, s.options.Namespace),
		s.options.Member, s.options.ReplicationPassword)
	if replication.PrimaryConnInfo == desired {
		return
	}

	replication.Standby = true
	replication.PrimaryConnInfo = desired
	replication.PrimarySlotName = provision.ReplicationSlotName(s.options.Member)
	if err := s.writeConfig(ctx, &replication); err != nil {
		log.Error(err, "could not repoint this standby at the new primary", "primary", primary)
		return
	}
	if err := s.tools.Reload(ctx); err != nil {
		log.Error(err, "could not reload after repointing at the new primary")
		return
	}
	log.Info("repointed at the new primary", "primary", primary)
}

// fetchInstance reads this member's own PgInstance, or nil when the API server did not
// answer inside the tick's budget.
func (s *Supervisor) fetchInstance(ctx context.Context) *pgelasticv1alpha1.PgInstance {
	if s.options.Client == nil {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, crReadTimeout)
	defer cancel()

	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: s.options.Namespace, Name: s.options.Instance}
	if err := s.options.Client.Get(readCtx, key, instance); err != nil {
		logf.FromContext(ctx).V(1).Info("could not read the instance", "error", err)
		return nil
	}
	return instance
}

// holdPrimary keeps this member's claim to the role, or gives it up.
func (s *Supervisor) holdPrimary(ctx context.Context, instance *pgelasticv1alpha1.PgInstance) {
	log := logf.FromContext(ctx)
	target := instance.Status.TargetPrimary
	if target != "" && target != ha.TargetPrimaryPending && target != s.options.Member {
		log.Info("the operator has named another member; stopping the postmaster",
			"targetPrimary", target, "member", s.options.Member)
		s.fence(ctx)
		return
	}

	renewCtx, cancel := context.WithTimeout(ctx, s.leaseConfig().RenewDeadline)
	defer cancel()
	switch outcome := s.leaseManager().Renew(renewCtx); outcome {
	case ha.RenewOK:
		s.noteRenewal(time.Time{})
	case ha.RenewLost:
		// The API server answered and named somebody else. That is proof, not suspicion,
		// and a primary that has provably lost the lease must stop before the member that
		// holds it starts writing.
		log.Info("the promotion lease is held by another member; stopping the postmaster")
		s.fence(ctx)
	case ha.RenewUnverified:
		// Deliberately not terminal. From here an operator having a bad day and a node
		// alone in the dark look identical, and fencing on that evidence turns routine
		// control-plane maintenance into simultaneous self-immolation across the fleet. The
		// liveness probe, which asks the peers rather than the API server, is what fences a
		// node that is genuinely isolated.
		s.noteRenewal(time.Now())
	}
}

// promoteIfChosen runs the promotion sequence when, and only when, the operator has written
// this member's name into targetPrimary.
func (s *Supervisor) promoteIfChosen(ctx context.Context, instance *pgelasticv1alpha1.PgInstance) {
	if instance.Status.TargetPrimary != s.options.Member ||
		instance.Status.CurrentPrimary == s.options.Member {
		return
	}
	if !s.beginPromotion() {
		return
	}

	options := s.options
	go func() {
		defer s.endPromotion()
		log := logf.FromContext(ctx)
		result, err := Promote(ctx, options)
		if err != nil {
			s.refusePromotion()
			// A held lease is the expected outcome of a race between two candidates, and a
			// denied quorum gate is a refusal rather than a fault. Neither is forced.
			if errors.Is(err, ErrLeaseHeld) || errors.Is(err, ErrQuorumDenied) {
				log.Info("the promotion was refused; will try again",
					"member", options.Member, "reason", err.Error())
				return
			}
			log.Error(err, "the promotion failed", "member", options.Member)
			return
		}
		s.notePromotion(result.Epoch)
	}()
}

// fence stops the postmaster through the supervisor's own inner loop rather than calling
// pg_ctl from here, so that the shutdown, the wait and the probe state stay owned by the
// goroutine that owns the postmaster.
func (s *Supervisor) fence(ctx context.Context) {
	select {
	case s.commands <- CommandFence:
	default:
		logf.FromContext(ctx).V(1).Info("a shutdown is already queued")
	}
}

func (s *Supervisor) leaseConfig() ha.LeaseConfig {
	return LeaseConfigOf(s.options.Config)
}

func (s *Supervisor) leaseManager() *LeaseManager {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.lease == nil {
		s.lease = LeaseManagerFor(s.options)
	}
	return s.lease
}

func (s *Supervisor) noteRenewal(failingSince time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.leaseUnverifiedSince = failingSince
}

// noteProgress records this member's position and reports whether it moved since the last
// reading, resetting the stranded clock when it did.
func (s *Supervisor) noteProgress(position ha.TimelinePosition) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	moved := position.Timeline > s.heldPosition.Timeline || position.LSN > s.heldPosition.LSN
	s.heldPosition = position
	if moved {
		s.strandedSince = time.Time{}
	}
	return moved
}

// strandedFor starts the clock on the first call and reports whether it has run out.
func (s *Supervisor) strandedFor(grace time.Duration) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.strandedSince.IsZero() {
		s.strandedSince = time.Now()
	}
	return time.Since(s.strandedSince) >= grace
}

func (s *Supervisor) clearStranded() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.strandedSince = time.Time{}
}

// requestRejoin queues the rejoin and records who it is onto, so the supervisor's own
// goroutine owns the stop, the rewind and the start rather than the observe tick.
func (s *Supervisor) requestRejoin(ctx context.Context, primary string) {
	s.mutex.Lock()
	s.rejoinPrimary = primary
	s.mutex.Unlock()
	select {
	case s.commands <- CommandRejoin:
	default:
		logf.FromContext(ctx).V(1).Info("a shutdown is already queued")
	}
}

func (s *Supervisor) rejoinTarget() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.rejoinPrimary
}

// noteRejoin publishes which of the two ways back this member is on, to the probes and to
// the CR. Both matter: the startup probe must stop restarting a Pod through a rewind that
// outlives every kubelet deadline, and the operator must not count the burst headroom of an
// instance that is rebuilding a member as available.
func (s *Supervisor) noteRejoin(ctx context.Context, method RejoinMethod) {
	s.update(func(state *ProbeState) { state.Rejoin = method })
	if s.options.Client == nil {
		return
	}
	if err := s.reporter().Report(ctx, s.ProbeState().Observation, false, method); err != nil {
		logf.FromContext(ctx).Error(err, "could not report the rejoin", "method", method)
	}
}

// promotionRetryDelay paces a promotion that has been refused.
//
// A denied quorum gate is a lasting state rather than a transient one - it lasts until a
// standby comes back - so retrying it on every two-second observe tick achieves nothing
// except a lease acquisition and a log line per tick. The delay is long enough to stop that
// and short enough that recovery is measured in seconds.
const promotionRetryDelay = 10 * time.Second

// beginPromotion admits one promotion at a time, and not immediately after a refusal. It
// runs off the observe tick, which fires every couple of seconds, while a promotion can take
// longer than that to acquire the lease alone.
func (s *Supervisor) beginPromotion() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.promoting || time.Since(s.promotionRefusedAt) < promotionRetryDelay {
		return false
	}
	s.promoting = true
	return true
}

func (s *Supervisor) refusePromotion() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.promotionRefusedAt = time.Now()
}

func (s *Supervisor) endPromotion() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.promoting = false
}

// notePromotion records the epoch this member was promoted at, so that every later
// configuration rewrite keeps publishing it. Without it the next reload would re-render the
// epoch the operator's ConfigMap still carries, and the fence token would go backwards -
// which the proxy reads as a fence trigger rather than as new information.
func (s *Supervisor) notePromotion(epoch int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.promotedEpoch = max(s.promotedEpoch, epoch)
}

func (s *Supervisor) publishedEpoch() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return max(s.promotedEpoch, s.options.Config.Postgres.PrimaryEpoch)
}
