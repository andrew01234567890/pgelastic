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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// anySource is pg_hba's spelling of "from anywhere". It is a default rather than a
// confinement: the CIDR a pod dials from is a property of the cluster's CNI that the operator
// cannot derive, so a real deployment configures it.
const anySource = "all"

// fieldName is the map key a status entry is identified by, spelled once so a listType=map
// merge key cannot be lost to a typo.
const fieldName = "name"

// requeueInterval paces the provisioning ladder. Members are created one per reconcile and
// each waits for the one before it, so the loop needs a heartbeat rather than only edges.
const requeueInterval = 5 * time.Second

// Environment overrides for the images and the placement strictness, read once at setup.
const (
	envPostgresImage   = "PGELASTIC_POSTGRES_IMAGE"
	envAgentImage      = "PGELASTIC_AGENT_IMAGE"
	envPodAntiAffinity = "PGELASTIC_POD_ANTI_AFFINITY"
)

// PgInstanceReconciler provisions one PostgreSQL instance and its replica set.
//
// It owns ordering, naming and recreation itself rather than delegating to a StatefulSet:
// rolling by replication lag, promoting a named member and recreating a Pod onto storage
// that already exists are all things a StatefulSet cannot express.
type PgInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// PostgresImage carries PostgreSQL 18 and pgBackRest.
	PostgresImage string
	// AgentImage carries the instance manager, and is copied into each pod by an init
	// container that does nothing but cp.
	AgentImage string
	// AntiAffinity selects how strictly members are kept apart.
	AntiAffinity provision.AntiAffinityPolicy
	// ProxySources and PeerSources are the pg_hba source addresses.
	ProxySources []string
	PeerSources  []string
	// Prober asks each member to describe itself. It is an interface so a test can drive
	// the failover state machine without three real postmasters; production leaves it nil
	// and gets a direct HTTP poll of each Pod IP.
	Prober MemberProber
	// ProbeTTL is how long one round of member observations is reused for. Zero polls every
	// member on every reconcile, which is what a test driving the reconciler by hand needs:
	// it changes what a member reports and then expects the very next reconcile to see it.
	ProbeTTL time.Duration
	// Quiescer holds this instance's clients at the proxy across a planned role change.
	// Nil is the headless deployment: no fleet fronts the pool, so there is nobody to hold
	// and the handover is simply the unheld one.
	Quiescer InstanceQuiescer

	// ControllerName is this operator's identity. An instance reaches a PgElasticClass
	// through its pool, and one naming a different controller is left entirely alone.
	ControllerName string
	// Now is the clock the backup schedule is evaluated against. Nil is the real one; a
	// test supplies its own so a cron slot can be reached without waiting for it.
	Now func() time.Time

	// observations reuses one round of member polls across the burst of reconciles that
	// each member's own status write produces.
	observations observationCache
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services;configmaps;secrets;serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile converges one PgInstance.
func (r *PgInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &pgelasticv1alpha1.PgInstance{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, stop, err := unclaimed(ctx, r.ownership(), instance); stop {
		return result, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, instance)
	}
	// Held before the first PVC exists. A guard added after the volumes are there leaves a
	// window in which deleting the instance destroys the only copy of every tenant on it.
	if controllerutil.AddFinalizer(instance, pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer) {
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	class, err := pgconf.LookupSizingClass(instance.Spec.Class)
	if err != nil {
		return ctrl.Result{}, r.publishInvalid(ctx, instance, err)
	}
	builder := r.builderFor(instance, class)

	if err := r.ensureSupportingObjects(ctx, instance, builder); err != nil {
		return ctrl.Result{}, err
	}
	groups, err := r.ensurePVCGroups(ctx, instance, builder)
	if err != nil {
		return ctrl.Result{}, err
	}
	pods, err := r.ensurePods(ctx, instance, builder, groups)
	if err != nil {
		return ctrl.Result{}, err
	}
	// The failover decision runs before the role labels are reconciled, because phase one
	// of a failover is precisely a decision to take a label away, and re-applying it from
	// the members' own stale reports in the same pass would undo it.
	decision := r.reconcileFailover(ctx, instance, pods)
	if err := r.reconcileRoleLabels(ctx, instance, decision, pods); err != nil {
		return ctrl.Result{}, err
	}
	// The roll runs after the role labels for the same reason the failover decision runs
	// before them: it acts on which member is serving, and the label is what makes that
	// true. It runs before the status apply because the status is where it publishes.
	roll, err := r.reconcileRoll(ctx, instance, builder, pods, decision)
	if err != nil {
		return ctrl.Result{}, err
	}
	backups := r.reconcileBackups(ctx, instance, decision)
	if err := r.publishStatus(ctx, instance, builder, groups, pods, decision, roll, backups); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: reconcileRequeue(decision, roll, backups)}, nil
}

// now is the clock, defaulting to the real one.
func (r *PgInstanceReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// reconcileRequeue paces the reconcile. A roll in progress is waiting on things that take
// a second or two - a drain reaching zero, a promotion completing, a Pod coming back - so
// it asks for a faster heartbeat than the provisioning ladder's.
func reconcileRequeue(decision ha.Decision, roll rollState, backups backupState) time.Duration {
	if backups.requeue > 0 {
		return min(backups.requeue, reconcileRequeueWithout(decision, roll))
	}
	return reconcileRequeueWithout(decision, roll)
}

// reconcileRequeueWithout is the pacing before the backup schedule is taken into account.
func reconcileRequeueWithout(decision ha.Decision, roll rollState) time.Duration {
	if roll.active {
		return min(rollRequeue, failoverRequeue(decision))
	}
	return failoverRequeue(decision)
}

func (r *PgInstanceReconciler) builderFor(
	instance *pgelasticv1alpha1.PgInstance,
	class pgconf.SizingClass,
) provision.Builder {
	return provision.Builder{
		Instance:      instance,
		PostgresImage: r.PostgresImage,
		AgentImage:    r.AgentImage,
		Capacity: pgconf.DeriveCapacity(
			class.AllocatableConnections,
			provision.ConcurrentDumps(instance.Spec),
			replicasOf(instance),
			migrationSlotHeadroom,
		),
		AntiAffinity: r.AntiAffinity,
		ProxySources: r.ProxySources,
		PeerSources:  r.PeerSources,
		PrimaryEpoch: primaryEpochOf(instance),
	}
}

// migrationSlotHeadroom is the number of operator-owned migration slots max_wal_senders is
// sized for on top of the standbys. Under-sizing it means a migration cannot start, and
// max_wal_senders is PGC_POSTMASTER.
const migrationSlotHeadroom int32 = 4

// Status fields both apply paths stamp, spelled once so the two cannot drift.
const (
	fieldObservedGeneration = "observedGeneration"
	fieldPhase              = "phase"
	fieldConditions         = "conditions"
)

func replicasOf(instance *pgelasticv1alpha1.PgInstance) int32 {
	if highAvailability := instance.Spec.HighAvailability; highAvailability != nil &&
		highAvailability.Replicas != nil {
		return *highAvailability.Replicas
	}
	return 3
}

// ensureSupportingObjects creates everything that is not a volume or a Pod.
func (r *PgInstanceReconciler) ensureSupportingObjects(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
) error {
	if err := r.ensureCredentials(ctx, instance); err != nil {
		return err
	}

	configMap, err := builder.ConfigMap(builder.AgentConfig())
	if err != nil {
		return err
	}
	objects := []client.Object{
		builder.ServiceAccount(),
		builder.Role(),
		builder.RoleBinding(),
		configMap,
		builder.PeerService(),
		builder.RoleService(provision.PrimaryServiceName(instance.Name),
			pgelasticv1alpha1.InstanceRolePrimary),
		builder.RoleService(provision.ReplicaServiceName(instance.Name),
			pgelasticv1alpha1.InstanceRoleReplica),
	}
	for _, budget := range builder.PodDisruptionBudgets() {
		objects = append(objects, budget)
	}
	for _, object := range objects {
		if err := r.ensure(ctx, instance, object); err != nil {
			return err
		}
	}
	return nil
}

// finalize refuses to let an instance go while tenants are still bound to it.
//
// Every PVC of an instance carries an owner reference to it, so deleting the object is
// deleting the data of every tenant living on it - not eventually, and with no confirmation
// beyond the one kubectl already asked for. `kubectl delete pginstance` is a plausible thing
// to type at a cluster somebody believes is idle, and the tenants that make it not idle are
// invisible from the instance itself.
//
// So the finalizer holds, and says which tenants are holding it. Releasing them is a
// migration or an explicit release, both of which are things a person does deliberately.
func (r *PgInstanceReconciler) finalize(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance,
		pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer) {
		return ctrl.Result{}, nil
	}

	bound := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, bound,
		client.InNamespace(instance.Namespace),
		client.MatchingFields{index.TenantByInstance: instance.Name},
	); err != nil {
		return ctrl.Result{}, err
	}
	if len(bound.Items) > 0 {
		names := make([]string, 0, len(bound.Items))
		for i := range bound.Items {
			names = append(names, bound.Items[i].Name)
		}
		slices.Sort(names)
		logf.FromContext(ctx).Info("refusing to delete an instance that still has tenants on it",
			"instance", instance.Name, "tenants", strings.Join(names, ","))
		return ctrl.Result{RequeueAfter: drainRecheck}, r.publishDraining(ctx, instance, names)
	}

	controllerutil.RemoveFinalizer(instance,
		pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer)
	return ctrl.Result{}, r.Update(ctx, instance)
}

// drainRecheck paces the wait for somebody to move the tenants off. Nothing about it is made
// faster by asking more often, and a deletion that is blocked is blocked until a person acts.
const drainRecheck = 30 * time.Second

// ensureCredentials creates the two role passwords once and never rotates them here.
// The postgres superuser is deliberately absent: it is never given a password at all, and
// is reachable only by peer authentication over the Unix socket.
func (r *PgInstanceReconciler) ensureCredentials(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) error {
	name := provision.CredentialsSecretName(instance.Name)
	secret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: name}, secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	// A recovered instance's catalogue is its source's, copied verbatim: pgbackrest restores
	// pg_authid along with everything else, so its roles keep the source's passwords. Minting
	// fresh ones here would leave a Secret nothing in the cluster it describes has ever seen -
	// every standby would fail SCRAM cloning from the primary, and anything dialling the
	// recovered instance over the network would be refused.
	//
	// So this waits rather than minting, and the PgRestore controller hands the credentials
	// over. It is not merely a matter of which controller is tidier: spec.restore is a plain
	// field on a namespaced object, so copying a Secret named by it here would let anybody who
	// can create a PgInstance name any other instance as their source and be handed that
	// instance's replication, ops and rewind passwords - all three of which are live against
	// the instance they were taken from. The restore controller reaches the same copy only
	// after it has resolved a completed backup that belongs to that source.
	if instance.Spec.Restore != nil {
		logf.FromContext(ctx).Info("waiting for the restore to hand over the source's "+
			"credentials", "instance", instance.Name,
			"source", instance.Spec.Restore.SourceInstanceName)
		return nil
	}

	replication, err := randomPassword()
	if err != nil {
		return err
	}
	ops, err := randomPassword()
	if err != nil {
		return err
	}
	rewind, err := randomPassword()
	if err != nil {
		return err
	}
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			provision.SecretKeyReplicationPassword: replication,
			provision.SecretKeyOpsPassword:         ops,
			provision.SecretKeyRewindPassword:      rewind,
		},
	}
	return r.ensure(ctx, instance, secret)
}

func randomPassword() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

// ensure creates an object owned by the instance, or updates the fields the operator owns
// on an object that already exists.
func (r *PgInstanceReconciler) ensure(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	desired client.Object,
) error {
	if err := controllerutil.SetControllerReference(instance, desired, r.Scheme); err != nil {
		return err
	}
	existing, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("object %T is not a client.Object", desired)
	}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Only the ConfigMap is rewritten in place. Services, budgets and RBAC are created
	// once; a Pod's volumes and a PVC's size are handled by their own paths, where the
	// ordering constraints live.
	updated, ok := desired.(*corev1.ConfigMap)
	if !ok {
		return nil
	}
	current, ok := existing.(*corev1.ConfigMap)
	if !ok || equalStringMaps(current.Data, updated.Data) {
		return nil
	}
	current.Data = updated.Data
	return r.Update(ctx, current)
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

// ensurePVCGroups creates the missing volume pairs and keeps their status annotation in
// step with the claim's binding.
//
// The two claims of a serial are created together and a Pod is never scheduled onto a
// group with only one of them, because a member whose pg_wal landed on the data volume is
// the exact failure the separate WAL volume exists to prevent.
func (r *PgInstanceReconciler) ensurePVCGroups(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
) ([]provision.Group, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, claims, client.InNamespace(instance.Namespace),
		client.MatchingLabels{provision.LabelInstanceName: instance.Name}); err != nil {
		return nil, err
	}
	groups := provision.GroupsOf(claims.Items)

	for _, serial := range provision.MissingSerials(groups, builder.Replicas()) {
		for _, role := range []string{provision.PVCRoleData, provision.PVCRoleWAL} {
			if err := r.ensure(ctx, instance, builder.PVC(serial, role)); err != nil {
				return nil, err
			}
		}
	}

	if err := r.List(ctx, claims, client.InNamespace(instance.Namespace),
		client.MatchingLabels{provision.LabelInstanceName: instance.Name}); err != nil {
		return nil, err
	}
	groups = provision.GroupsOf(claims.Items)
	for i := range groups {
		if err := r.markGroupReady(ctx, groups[i]); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

func (r *PgInstanceReconciler) markGroupReady(ctx context.Context, group provision.Group) error {
	if !group.Bound() {
		return nil
	}
	for _, claim := range []*corev1.PersistentVolumeClaim{group.Data, group.WAL} {
		if claim.Annotations[provision.AnnotationPVCStatus] == provision.PVCStatusReady {
			continue
		}
		updated := claim.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[provision.AnnotationPVCStatus] = provision.PVCStatusReady
		if err := r.Update(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

// ensurePods creates one member per reconcile, in serial order.
//
// Serial one is created first and nothing follows it until a primary has actually been
// elected, because every later member bootstraps by cloning that primary. After that the
// gate is per-member readiness, so a clone that is still running does not have a second
// clone started alongside it competing for the same walsender budget.
func (r *PgInstanceReconciler) ensurePods(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
	groups []provision.Group,
) ([]corev1.Pod, error) {
	log := logf.FromContext(ctx)
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(instance.Namespace),
		client.MatchingLabels{provision.LabelInstanceName: instance.Name}); err != nil {
		return nil, err
	}
	existing := map[int32]*corev1.Pod{}
	for i := range pods.Items {
		if serial, ok := provision.SerialOf(pods.Items[i].Labels); ok {
			existing[serial] = &pods.Items[i]
		}
	}

	for _, group := range groups {
		if group.Serial > builder.Replicas() {
			continue
		}
		if _, ok := existing[group.Serial]; ok {
			continue
		}
		if !group.Complete() {
			log.Info("waiting for a complete PVC group", "serial", group.Serial)
			return pods.Items, nil
		}
		if group.Serial > 1 && !r.previousMemberReady(existing, group.Serial, instance) {
			return pods.Items, nil
		}
		pod := builder.Pod(group.Serial, builder.DesiredStamp())
		if err := r.ensure(ctx, instance, pod); err != nil {
			return nil, err
		}
		log.Info("created a member", "member", pod.Name)
		return pods.Items, nil
	}
	return pods.Items, nil
}

// previousMemberReady gates the next member on the one before it, and gates every member
// after the first on a primary existing at all.
func (r *PgInstanceReconciler) previousMemberReady(
	existing map[int32]*corev1.Pod,
	serial int32,
	instance *pgelasticv1alpha1.PgInstance,
) bool {
	if instance.Status.CurrentPrimary == "" {
		return false
	}
	previous, ok := existing[serial-1]
	return ok && podReady(previous)
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reconcileRoleLabels flips the label the two Services and the two PodDisruptionBudgets
// select on.
//
// The primary label follows ha.Decision.ServingPrimary and nothing else, because the
// question "which member may the read-write Service select" is not the question "is a
// failover in progress". The replica label is still frozen for the whole of a failover: a
// member's own last report is the half-converged state the sentinel exists to disregard,
// and nothing needs the read-only Service to converge mid-failover.
func (r *PgInstanceReconciler) reconcileRoleLabels(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
	pods []corev1.Pod,
) error {
	if decision.SplitBrain {
		return nil
	}
	if err := r.applyPrimaryLabel(ctx, pods, decision.ServingPrimary); err != nil {
		return err
	}
	if ha.FailoverInProgress(instance.Status.CurrentPrimary, targetPrimaryAfter(instance, decision)) {
		return nil
	}
	roles := map[string]pgelasticv1alpha1.InstanceRole{}
	for _, member := range instance.Status.Instances {
		roles[member.Name] = member.Role
	}
	for i := range pods {
		pod := &pods[i]
		// A member's own report is enough to make it a replica and never enough to make it
		// the primary. The primary label belongs to applyPrimaryLabel alone, which takes its
		// answer from status.currentPrimary: a demoted primary whose last report still claims
		// the role would otherwise be handed it straight back, putting two members behind one
		// Service, which is the failure this whole file exists to prevent.
		if roles[pod.Name] != pgelasticv1alpha1.InstanceRoleReplica ||
			pod.Labels[provision.LabelRole] == string(pgelasticv1alpha1.InstanceRoleReplica) ||
			pod.Name == decision.ServingPrimary {
			continue
		}
		updated := pod.DeepCopy()
		updated.Labels[provision.LabelRole] = string(pgelasticv1alpha1.InstanceRoleReplica)
		if err := r.Update(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

// applyPrimaryLabel puts the read-write Service's selector on the member that is genuinely
// serving, and takes it off every other member that still carries it.
//
// Both halves matter and they are not symmetrical. Granting it late is an endpoint-less
// Service refusing connections a healthy primary could have served; leaving it on a demoted
// member is two members behind one Service, which is worse. ha.Decision.ServingPrimary is
// the single answer both halves are driven from, so they cannot disagree.
func (r *PgInstanceReconciler) applyPrimaryLabel(
	ctx context.Context,
	pods []corev1.Pod,
	serving string,
) error {
	for i := range pods {
		pod := &pods[i]
		labelled := pod.Labels[provision.LabelRole] == string(pgelasticv1alpha1.InstanceRolePrimary)
		switch {
		case pod.Name == serving && !labelled:
			updated := pod.DeepCopy()
			if updated.Labels == nil {
				updated.Labels = map[string]string{}
			}
			updated.Labels[provision.LabelRole] = string(pgelasticv1alpha1.InstanceRolePrimary)
			if err := r.Update(ctx, updated); err != nil {
				return err
			}
		case pod.Name != serving && labelled:
			if err := r.stripRoleLabel(ctx, pods, pod.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// publishStatus applies the fields the operator owns.
//
// It is a server-side apply under the operator's own field manager, and it deliberately
// does not touch the members' own reports: each agent owns its entry in the instances list
// and writes it from inside its own pod, which is the only vantage point from which
// pg_is_in_recovery() can be read without a network hop the failure may have removed. The
// operator adds only inSyncSet, which no member can know about itself.
func (r *PgInstanceReconciler) publishStatus(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	builder provision.Builder,
	groups []provision.Group,
	pods []corev1.Pod,
	decision ha.Decision,
	roll rollState,
	backups backupState,
) error {
	capacity := builder.Capacity
	status := map[string]any{
		fieldObservedGeneration: instance.Generation,
		fieldPhase:              string(phaseOf(instance, groups, pods, decision, builder.Replicas())),
		"capacity": map[string]any{
			"maxConnections":   int64(capacity.MaxConnections),
			"reservedForAdmin": int64(capacity.SuperuserReserved + capacity.Reserved),
			"replicationSlots": int64(capacity.ReplicationSlots),
			"allocatable":      int64(allocatableOf(instance, capacity, roll, decision)),
		},
		"storage": map[string]any{
			"allocated": instance.Spec.Storage.Size.String(),
		},
		"instances": syncSetEntries(instance),
		fieldConditions: append(conditionsFor(instance, groups, pods, decision, builder.Replicas()),
			rollCondition(instance, roll)),
	}
	if published := rollStatus(roll); published != nil {
		status["roll"] = published
	}
	// Both are omitted rather than emptied when absent. This is a server-side apply, so a
	// field the operator stops including is removed - which is exactly how an election is
	// withdrawn once the backup it named is no longer waiting to be taken.
	if pending := backups.pending; pending != nil {
		status["pendingBackup"] = map[string]any{
			fieldName:     pending.Name,
			"member":      pending.Member,
			"requestedAt": pending.RequestedAt.UTC().Format(time.RFC3339),
		}
	}
	if last := backups.last; last != nil {
		summary := map[string]any{
			"type":                 string(last.Type),
			"sizeBytes":            last.SizeBytes,
			"verified":             last.Verified,
			"sourceMaxConnections": last.SourceMaxConnections,
		}
		if last.At != nil {
			summary["at"] = last.At.UTC().Format(time.RFC3339)
		}
		status["lastBackup"] = summary
	}
	maps.Copy(status, failoverStatus(instance, decision))
	// primaryEpoch is deliberately absent. It belongs to whichever member holds the role,
	// which derives it from the Lease's transition counter and publishes it together with
	// currentPrimary in a single write. An operator that re-applied it from a copy of the
	// object it read some moments ago would drive the fence token backwards, and a lower
	// epoch is what the proxy treats as a fence trigger rather than as new information.

	object := statusApplyObject(instance)
	object.Object["status"] = status
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(object),
		client.FieldOwner("pgelastic-operator"), client.ForceOwnership)
}

// allocatableOf publishes zero while the instance is not yet serving all its members.
// A re-cloning or half-built instance must not have its headroom counted as available:
// quorum-gated failover is impossible while it is at reduced redundancy, so promising
// tenants that capacity would be promising something the instance cannot honour.
//
// A rolling restart is the one exception, and it is not a nicety. This number is part of
// the proxy fleet's *structural* configuration, so every time it moves the fleet's Pod
// template changes and the rollout that follows drops every client on the pool - including
// the clients of instances the roll never touched. An instance whose allocatable collapsed
// to zero for each member restart would roll the fleet several times per roll, which is a
// rolling restart that produces exactly the outcome it exists to prevent. It ran, and the
// held probes recorded 1426 dropped statements on the rolled instance and 857 on its
// untouched neighbour before this exception existed.
//
// Publishing the capacity through a roll is also the truthful answer. A roll takes one
// member away at a time, on purpose, for as long as one restart takes; the instance is
// serving every tenant it had, on the same number of connections, throughout. What the
// zero is for is a member rebuilding itself for minutes to hours with no end in sight.
// The test is which phases *withhold* the capacity, not which ones publish it. Listing the
// publishing phases leaves every transient phase zeroing a structural value, and a roll
// passes through several of them: the window between the last member restarting and the
// instance reporting Ready again zeroed this, rewrote the fleet's Pod template, and churned
// proxy Pods through the end of every roll. Observed directly — the rendered document
// gained and lost `backendConnections = 50` every few seconds while the fleet rolled.
func allocatableOf(
	instance *pgelasticv1alpha1.PgInstance,
	capacity pgconf.Capacity,
	roll rollState,
	decision ha.Decision,
) int32 {
	_ = roll
	if decision.SplitBrain {
		return 0
	}
	// Ready, Degraded and FailingOver all describe an instance that is carrying its tenants
	// right now, on the same number of connections. A roll passes through the latter two on
	// every member, so admitting only Ready made this value flap for the length of every
	// roll. Anything else — half-built, rebuilding, being taken apart, or a phase this
	// function has not been taught — withholds, because an unrecognised state is not
	// evidence of capacity.
	switch instance.Status.Phase {
	case pgelasticv1alpha1.InstancePhaseReady,
		pgelasticv1alpha1.InstancePhaseDegraded,
		pgelasticv1alpha1.InstancePhaseFailingOver:
		return capacity.Allocatable
	default:
		return 0
	}
}

// primaryEpochOf keeps the epoch monotonic. It is seeded once at bootstrap; every later
// bump belongs to the promotion path, which derives it from the Lease's LeaderTransitions
// counter.
func primaryEpochOf(instance *pgelasticv1alpha1.PgInstance) int64 {
	return max(instance.Status.PrimaryEpoch, ha.InitialPrimaryEpoch)
}

// targetPrimaryOf seeds the operator's decision at bootstrap and otherwise leaves it
// alone. Rewriting it is the failover state machine's job, and it does so through the
// reserved "pending" sentinel so that targetPrimary != currentPrimary is a total signal.
func targetPrimaryOf(instance *pgelasticv1alpha1.PgInstance) string {
	if instance.Status.TargetPrimary != "" {
		return instance.Status.TargetPrimary
	}
	if instance.Status.CurrentPrimary != "" {
		return instance.Status.CurrentPrimary
	}
	return provision.MemberName(instance.Name, 1)
}

// syncSetEntries records which members the primary counts towards the synchronous quorum.
// It is sourced from the quorum evidence the primary read back out of its own postmaster,
// never from the spec, so a partially applied reload cannot make a member look like a
// voter when PostgreSQL never loaded it as one.
func syncSetEntries(instance *pgelasticv1alpha1.PgInstance) []any {
	voters := map[string]bool{}
	if evidence := instance.Status.QuorumEvidence; evidence != nil {
		for _, member := range evidence.VotingMembers {
			voters[member] = true
		}
	}
	entries := make([]any, 0, len(instance.Status.Instances))
	for _, member := range instance.Status.Instances {
		entries = append(entries, map[string]any{
			"name":      member.Name,
			"inSyncSet": voters[member.Name],
		})
	}
	return entries
}

func phaseOf(
	instance *pgelasticv1alpha1.PgInstance,
	groups []provision.Group,
	pods []corev1.Pod,
	decision ha.Decision,
	replicas int32,
) pgelasticv1alpha1.InstancePhase {
	if len(groups) == 0 {
		return pgelasticv1alpha1.InstancePhasePending
	}
	if instance.Status.CurrentPrimary == "" || int32(len(pods)) < replicas {
		return pgelasticv1alpha1.InstancePhaseBootstrapping
	}
	if ha.FailoverInProgress(instance.Status.CurrentPrimary, targetPrimaryAfter(instance, decision)) ||
		decision.SplitBrain {
		return pgelasticv1alpha1.InstancePhaseFailingOver
	}
	if rejoiningMember(instance) != nil {
		return pgelasticv1alpha1.InstancePhaseRecloning
	}
	if readyMembers(pods) < replicas {
		return pgelasticv1alpha1.InstancePhaseDegraded
	}
	return pgelasticv1alpha1.InstancePhaseReady
}

// rejoiningMember is the first member rebuilding itself onto the primary's history, nil
// when none is.
//
// It moves the whole instance out of Ready, which is what stops allocatableOf publishing
// any headroom: a member rewinding or re-cloning leaves the instance at two thirds
// redundancy, and quorum-gated failover is impossible while it is there, so promising
// tenants that capacity would be promising something the instance cannot honour.
func rejoiningMember(instance *pgelasticv1alpha1.PgInstance) *pgelasticv1alpha1.InstanceMemberStatus {
	for i, member := range instance.Status.Instances {
		if member.Rejoining != "" {
			return &instance.Status.Instances[i]
		}
	}
	return nil
}

func readyMembers(pods []corev1.Pod) int32 {
	var ready int32
	for i := range pods {
		if podReady(&pods[i]) {
			ready++
		}
	}
	return ready
}

func conditionsFor(
	instance *pgelasticv1alpha1.PgInstance,
	groups []provision.Group,
	pods []corev1.Pod,
	decision ha.Decision,
	replicas int32,
) []any {
	phase := phaseOf(instance, groups, pods, decision, replicas)
	ready := phase == pgelasticv1alpha1.InstancePhaseReady

	conditions := make([]any, 0, 3)
	conditions = append(conditions,
		condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionReady, ready,
			instance.Generation, readyReason(phase),
			fmt.Sprintf("%d of %d members are ready", readyMembers(pods), replicas)),
		condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionProgressing, !ready,
			instance.Generation, progressingReason(phase), progressingMessage(instance, phase)),
	)
	degraded := phase == pgelasticv1alpha1.InstancePhaseDegraded
	conditions = append(conditions, condition(instance.Status.Conditions,
		pgelasticv1alpha1.ConditionDegraded, degraded, instance.Generation,
		degradedReason(degraded), quorumMessage(instance)))
	if archiving := archivingCondition(instance); archiving != nil {
		conditions = append(conditions, archiving)
	}
	return append(conditions, failoverConditions(instance, decision)...)
}

// archivingCondition projects what the primary published about WAL archiving.
//
// The operator reports it rather than measuring it: archiving happens on the primary, the
// primary is the only member whose pg_stat_archiver describes anything, and it already
// writes what it saw into status.archiveHealth on the same patch that carries the quorum
// evidence. Polling for it separately would put a second, later reading beside the first.
//
// Nothing is reported for an instance with no repository. An absent condition and a False
// one are different claims: the first says nobody asked for an archive, the second says one
// was asked for and is not happening.
func archivingCondition(instance *pgelasticv1alpha1.PgInstance) map[string]any {
	if instance.Spec.Backup == nil {
		return nil
	}
	health := instance.Status.ArchiveHealth
	if health == nil {
		return condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionArchiving,
			false, instance.Generation, pgelasticv1alpha1.ReasonArchiveDegraded,
			"a repository is configured and no member has reported on archiving yet")
	}
	return condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionArchiving,
		health.Healthy, instance.Generation, archivingReason(health.Healthy),
		archivingMessage(health))
}

func archivingReason(healthy bool) string {
	if healthy {
		return pgelasticv1alpha1.ReasonArchiveHealthy
	}
	return pgelasticv1alpha1.ReasonArchiveDegraded
}

// archivingMessage says which of the two ways archiving is failing, because they call for
// different actions: a repository refusing writes is a credential or a bucket, and a queue
// that is not draining behind a last success is an archive_command that has stopped
// returning.
func archivingMessage(health *pgelasticv1alpha1.ArchiveHealthStatus) string {
	switch {
	case health.Healthy:
		return fmt.Sprintf("archived through %s", health.LastArchivedWAL)
	case health.LastFailureMessage != "":
		return health.LastFailureMessage
	case health.ReadyBacklog > 0:
		return fmt.Sprintf("%d segments are waiting to be archived and none has been archived recently",
			health.ReadyBacklog)
	default:
		return "WAL archiving is not working"
	}
}

func readyReason(phase pgelasticv1alpha1.InstancePhase) string {
	if phase == pgelasticv1alpha1.InstancePhaseReady {
		return pgelasticv1alpha1.ReasonReady
	}
	return pgelasticv1alpha1.ReasonPending
}

// progressingReason names a re-cloning instance as such, because that reason is what the
// autoscaler and the rebalancer refuse to act through: moving tenants onto an instance that
// is rebuilding a member compounds a change already in flight.
func progressingReason(phase pgelasticv1alpha1.InstancePhase) string {
	switch phase {
	case pgelasticv1alpha1.InstancePhaseReady:
		return pgelasticv1alpha1.ReasonStable
	case pgelasticv1alpha1.InstancePhaseRecloning:
		return pgelasticv1alpha1.ReasonRecloning
	default:
		return pgelasticv1alpha1.ReasonPending
	}
}

func progressingMessage(instance *pgelasticv1alpha1.PgInstance, phase pgelasticv1alpha1.InstancePhase) string {
	member := rejoiningMember(instance)
	if phase != pgelasticv1alpha1.InstancePhaseRecloning || member == nil {
		return string(phase)
	}
	return fmt.Sprintf("%s is %s onto the primary's history", member.Name, member.Rejoining)
}

func degradedReason(degraded bool) string {
	if degraded {
		return pgelasticv1alpha1.ReasonQuorumLost
	}
	return pgelasticv1alpha1.ReasonQuorumHealthy
}

func quorumMessage(instance *pgelasticv1alpha1.PgInstance) string {
	evidence := instance.Status.QuorumEvidence
	if evidence == nil || evidence.SynchronousStandbyNames == "" {
		return "no quorum evidence has been reported yet"
	}
	return "the primary loaded synchronous_standby_names " + evidence.SynchronousStandbyNames
}

// condition renders one condition for the status apply, carrying its transition time
// forward whenever the status has not in fact transitioned.
//
// Stamping the current instant unconditionally does two things, both wrong. It makes
// lastTransitionTime mean "when this was last reconciled", which is the one thing the field
// must not mean. And because the apply then differs from the stored object on every pass,
// it re-enqueues the PgInstance through its own watch and fans that write out to every pool
// and tenant watching it, for an instance where nothing happened.
func condition(
	existing []metav1.Condition,
	conditionType string,
	ok bool,
	generation int64,
	reason, message string,
) map[string]any {
	status := conditionStatus(ok)
	transitioned := metav1.Now()
	if previous := meta.FindStatusCondition(existing, conditionType); previous != nil &&
		previous.Status == status && !previous.LastTransitionTime.IsZero() {
		transitioned = previous.LastTransitionTime
	}
	return map[string]any{
		"type":               conditionType,
		"status":             string(status),
		"reason":             reason,
		"message":            message,
		"observedGeneration": generation,
		"lastTransitionTime": transitioned.UTC().Format(time.RFC3339),
	}
}

func statusApplyObject(instance *pgelasticv1alpha1.PgInstance) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{}}
	object.SetAPIVersion(pgelasticv1alpha1.SchemeGroupVersion.String())
	object.SetKind("PgInstance")
	object.SetNamespace(instance.Namespace)
	object.SetName(instance.Name)
	return object
}

// publishInvalid reports a spec the operator refuses to provision from. An unknown class
// is not a transient error and retrying cannot fix it, so it is surfaced as a condition
// rather than as a requeue loop.
func (r *PgInstanceReconciler) publishInvalid(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	cause error,
) error {
	object := statusApplyObject(instance)
	object.Object["status"] = map[string]any{
		fieldObservedGeneration: instance.Generation,
		fieldPhase:              string(pgelasticv1alpha1.InstancePhasePending),
		fieldConditions: []any{
			condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionReady, false,
				instance.Generation, pgelasticv1alpha1.ReasonInvalidSpec, cause.Error()),
		},
	}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(object),
		client.FieldOwner("pgelastic-operator"), client.ForceOwnership)
}

// publishDraining says which tenants are keeping a deleted instance alive.
//
// The names are the whole point. "Deletion is blocked" tells somebody nothing they can act
// on; the list of tenants still living on the instance is exactly the work that has to
// happen before it can go.
func (r *PgInstanceReconciler) publishDraining(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	tenants []string,
) error {
	object := statusApplyObject(instance)
	object.Object["status"] = map[string]any{
		fieldObservedGeneration: instance.Generation,
		fieldPhase:              string(pgelasticv1alpha1.InstancePhaseTerminating),
		fieldConditions: []any{
			condition(instance.Status.Conditions, pgelasticv1alpha1.ConditionReady, false,
				instance.Generation, pgelasticv1alpha1.ReasonTenantsStillBound,
				fmt.Sprintf("deletion is held: %d tenant(s) are still on this instance and "+
					"their data lives on its volumes - %s. Migrate or release them first.",
					len(tenants), strings.Join(tenants, ", "))),
		},
	}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(object),
		client.FieldOwner("pgelastic-operator"), client.ForceOwnership)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgInstanceReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

func (r *PgInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.PostgresImage == "" {
		r.PostgresImage = envOrDefault(envPostgresImage, "pgelastic/postgres:18")
	}
	if r.AgentImage == "" {
		r.AgentImage = envOrDefault(envAgentImage, "pgelastic/instance:latest")
	}
	if r.AntiAffinity == "" {
		r.AntiAffinity = provision.AntiAffinityPolicy(
			envOrDefault(envPodAntiAffinity, string(provision.AntiAffinityRequired)))
	}
	if r.ProbeTTL == 0 {
		r.ProbeTTL = defaultProbeTTL
	}
	if r.PeerSources == nil {
		// "all" until the operator is told the pod CIDR. It admits only the replication
		// and ops roles, both of which authenticate with SCRAM; the deny-by-default
		// catch-all still refuses every tenant role from every address.
		r.PeerSources = []string{anySource}
	}
	if r.ProxySources == nil {
		// Unset until now, which meant RenderHBA emitted no proxy rule at all and the
		// deny-by-default catch-all refused every client. That was survivable only while
		// tenant roles were passwordless and nothing ever authenticated as one; the moment
		// the proxy dials backends as the tenant, a missing rule is a pool nobody can reach.
		//
		// "all" is a default rather than a confinement, and it is deliberately not presented
		// as one: the CIDR the proxy's pods dial from is a property of the cluster's CNI that
		// the operator cannot derive, so it has to be configured. What this default buys is
		// that tenant roles authenticate at all, over SCRAM, which is strictly better than a
		// listener nothing can reach - and the containment those roles actually rest on is
		// the per-database ACL, not this list.
		r.ProxySources = []string{anySource}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgInstance{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Named("pginstance").
		Complete(r)
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
