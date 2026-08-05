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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
	"github.com/andrew01234567890/pgelastic/internal/tenantuser"
	"github.com/andrew01234567890/pgelastic/internal/tracing"
)

// PgTenantUserFinalizer holds a login open until the cluster-global role it minted is gone.
//
// A cleanup trigger rather than a refusal: it exists to act on the world, not to stop a
// deletion. So when ownership cannot be resolved at all - the tenant has gone, and with it the
// route to a class - it is released rather than held, because a login that outlives its tenant
// could never satisfy it and would make its namespace undeletable for ever.
const PgTenantUserFinalizer = "pgelastic.io/tenant-user-role"

// ReasonNoCredentials means a login that is meant to authenticate has no credentials Secret,
// so the proxy has nothing to challenge it with.
const ReasonNoCredentials = "NoCredentials"

// PgTenantUserReconciler makes one login's PostgreSQL role match its spec.
type PgTenantUserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// SQL reaches PostgreSQL as the bootstrap superuser over the hosting member's socket.
	SQL tenantuser.SQL
	// ControllerName is this operator's identity, for the ownership gate.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantusers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

// Reconcile brings one login's role into the shape its spec asks for.
func (r *PgTenantUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	user := &pgelasticv1alpha1.PgTenantUser{}
	if err := r.Get(ctx, req.NamespacedName, user); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// releaseOnly, because this finalizer is a cleanup trigger rather than a refusal: it
	// exists to drop a role on the world, not to stop a deletion. A login being deleted
	// alongside its tenant resolves to Unresolved - the tenant is the only route to a class -
	// and holding on then would hold for ever, since only this controller can clear it. The
	// gate releases instead, which is the whole reason the two kinds are distinguished.
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, releaseOnly, user); stop {
		return result, err
	}
	if !user.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, user)
	}

	status := pgelasticv1alpha1.PgTenantUserStatus{
		ObservedGeneration: user.Generation,
		RoleName:           user.Status.RoleName,
		// Cloned, not aliased. meta.SetStatusCondition mutates the slice it is given in
		// place, so sharing the backing array with user.Status.Conditions means publish's
		// DeepEqual compares the new conditions against themselves and finds no change - and
		// a pass whose only work was a condition, a reason or a message is dropped entirely
		// whenever the phase and the observed generation happen to match.
		Conditions: slices.Clone(user.Status.Conditions),
	}
	// Published as soon as the object resolves, before the role exists. It is derived rather
	// than observed, and what it is for - getting from a role in pg_stat_activity back to the
	// object - is needed most when that object is Failed.
	status.RoleName = migration.TenantUserRoleName(user.Namespace, user.Spec.TenantRef.Name, user.Name)

	if err := r.converge(ctx, user, &status); err != nil {
		return ctrl.Result{}, err
	}
	status.Phase = userPhase(status.Conditions)
	// One requeue interval for every outcome, stated once. Every path through converge that
	// is not an error wants the same thing - look again in a while - and returning it from
	// each of them made seven copies of one decision and hid that they were all the same.
	return ctrl.Result{RequeueAfter: placementRetryInterval}, r.publish(ctx, user, status)
}

// converge resolves where the login lives and makes its role match.
//
// Every reason it cannot proceed yet is a condition rather than an error: a tenant that is not
// bound, an instance that is not Ready and a missing credential are all ordinary states of the
// cluster, and returning an error for them would retry with backoff while saying nothing.
func (r *PgTenantUserReconciler) converge(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
	status *pgelasticv1alpha1.PgTenantUserStatus,
) error {
	tenant := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: user.Namespace, Name: user.Spec.TenantRef.Name}
	if err := r.Get(ctx, key, tenant); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		r.pending(status, user.Generation, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("PgTenant %q does not exist", user.Spec.TenantRef.Name))
		return nil
	}
	setCondition(&status.Conditions, user.Generation, pgelasticv1alpha1.ConditionAccepted,
		metav1.ConditionTrue, pgelasticv1alpha1.ReasonAccepted,
		fmt.Sprintf("this login belongs to PgTenant %q", tenant.Name))

	if tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
		r.notReady(status, user.Generation, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("PgTenant %q is not bound to an instance yet", tenant.Name))
		return nil
	}
	instanceName := tenant.Status.Binding.InstanceRef.Name

	if r.SQL == nil {
		r.notReady(status, user.Generation, tenantuser.ReasonProvisioningFailed,
			"no PostgreSQL transport is configured, so this login's role cannot be created")
		return nil
	}

	members, owned, missing, err := r.memberRoles(ctx, user, tenant)
	if err != nil {
		return err
	}
	if missing != "" {
		// Never created here: a role minted by another object's reconcile carries no
		// finalizer of its own and nothing would ever drop it.
		r.notReady(status, user.Generation, pgelasticv1alpha1.ReasonPending, fmt.Sprintf(
			"waiting for the login %q named in memberOf to be provisioned", missing))
		return nil
	}

	if err := r.hold(ctx, user); err != nil {
		return err
	}

	spec := tenantuser.Spec{
		Role:     status.RoleName,
		Database: tenant.Spec.DatabaseName,
		Login:    loginAllowed(user),
		MemberOf: members,
		Owned:    owned,
	}
	// Read off the tenant's published status rather than resolved again here: it is the same
	// value the tenant controller put on the owner role, so the two cannot drift into a login
	// that is bounded differently from the tenant it belongs to.
	if effective := tenant.Status.Effective; effective != nil {
		spec.StatementTimeout = durationSetting(effective.StatementTimeout)
		spec.TempFileLimit = quantitySetting(effective.TempFileLimit)
	}
	// Provisioned even without a credential. Another login's memberOf may name this one, and
	// a role that does not exist cannot be granted.
	credentialled := spec.Login && user.Spec.CredentialsSecretRef != nil
	if credentialled {
		credential, err := r.ensureUserBackendCredential(
			ctx, user, status.RoleName, scramIterationsOfPool(ctx, r.Client, tenant))
		if err != nil {
			return err
		}
		spec.Verifier = credential.Verifier
	}

	if _, err := tenantuser.Ensure(ctx, r.SQL, tenantEndpoint(tenant, instanceName), spec); err != nil {
		// Logged as well as conditioned. A failure that only reaches the status is invisible
		// in the place anybody debugging actually looks, and a permanent one then presents as
		// a controller doing nothing at all - which is how a broken catalog query cost two
		// sessions before anyone saw the error text PostgreSQL had been returning all along.
		logf.FromContext(ctx).Error(err, "Could not provision the login's role",
			"role", status.RoleName, "instance", instanceName)
		r.notReady(status, user.Generation, tenantuser.ReasonProvisioningFailed, err.Error())
		return nil
	}

	// Decided from this pass's own facts, never read back out of the condition list. The
	// conditions are seeded from the previous reconcile, so peeking at them meant that once a
	// login had been seen without a credentialsSecretRef it reported NoCredentials for ever -
	// after the Secret was added the role existed, the backend credential existed and clients
	// authenticated, while the object still said Ready=False with a message that was no
	// longer true.
	if spec.Login && !credentialled {
		r.notReady(status, user.Generation, ReasonNoCredentials,
			"this login has no credentials Secret, so the proxy has nothing to challenge a "+
				"client with")
		return nil
	}
	setCondition(&status.Conditions, user.Generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionTrue, pgelasticv1alpha1.ReasonReady, fmt.Sprintf(
			"role %q is serving on PgInstance %q: it holds CONNECT on database %q and nothing "+
				"else, so grant the rest by connecting as the tenant",
			status.RoleName, instanceName, tenant.Spec.DatabaseName))
	return nil
}

// memberRoles resolves spec.memberOf to the roles it names, and reports the fence.
//
// A membership may only name a login of the same tenant - the webhook says so and this agrees,
// because the webhook cannot see two objects admitted concurrently. `owned` is every login of
// the tenant, which is what revocation is confined to.
func (r *PgTenantUserReconciler) memberRoles(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
	tenant *pgelasticv1alpha1.PgTenant,
) (members, owned []string, missing string, err error) {
	siblings := &pgelasticv1alpha1.PgTenantUserList{}
	if err := r.List(ctx, siblings, client.InNamespace(user.Namespace),
		client.MatchingFields{index.TenantUserByTenant: tenant.Name}); err != nil {
		return nil, nil, "", err
	}

	byName := map[string]*pgelasticv1alpha1.PgTenantUser{}
	for i := range siblings.Items {
		sibling := &siblings.Items[i]
		owned = append(owned, migration.TenantUserRoleName(
			sibling.Namespace, sibling.Spec.TenantRef.Name, sibling.Name))
		byName[sibling.Spec.UserName] = sibling
	}
	slices.Sort(owned)

	for _, name := range user.Spec.MemberOf {
		sibling, ok := byName[name]
		if !ok || sibling.Status.RoleName == "" {
			return nil, owned, name, nil
		}
		members = append(members, sibling.Status.RoleName)
	}
	return members, owned, "", nil
}

// finalize drops the login's role before letting the object go.
func (r *PgTenantUserReconciler) finalize(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(user, PgTenantUserFinalizer) {
		return ctrl.Result{}, nil
	}
	// A role that cannot be dropped holds the object open and says so. Leaking a
	// cluster-global login is worse than a slow delete: it outlives the record of why it
	// exists, and nothing left will ever drop it.
	if err := r.reclaim(ctx, user); err != nil {
		if reportErr := r.reportReclaimFailure(ctx, user, err); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{RequeueAfter: placementRetryInterval}, nil
	}
	controllerutil.RemoveFinalizer(user, PgTenantUserFinalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, user))
}

// reclaim drops the role, rehoming anything it owns onto the tenant's owner first.
func (r *PgTenantUserReconciler) reclaim(ctx context.Context, user *pgelasticv1alpha1.PgTenantUser) error {
	tenant := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: user.Namespace, Name: user.Spec.TenantRef.Name}
	if err := r.Get(ctx, key, tenant); err != nil {
		// A tenant that has gone took its database - and, under reclaimPolicy Delete, its
		// roles - with it. There is nothing left to drop and nothing to hold this open for.
		return client.IgnoreNotFound(err)
	}
	if tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
		return nil
	}
	// Derived rather than read back from status. Reconcile writes that field as exactly this
	// call over three immutable inputs, so the two can never disagree - and `hold` adds the
	// finalizer before the first status is ever published, which is the window where reading
	// status would have found nothing and the error message would have named an empty role.
	role := migration.TenantUserRoleName(user.Namespace, user.Spec.TenantRef.Name, user.Name)
	if r.SQL == nil {
		return fmt.Errorf("no PostgreSQL transport is configured, so role %q cannot be dropped",
			role)
	}
	return tenantuser.Drop(ctx,
		r.SQL,
		tenantEndpoint(tenant, tenant.Status.Binding.InstanceRef.Name),
		tenantuser.Spec{Role: role, Database: tenant.Spec.DatabaseName},
		migration.BackendRoleName(tenant.Namespace, tenant.Name))
}

// hold adds the finalizer before the first CREATE, so a login deleted mid-provision still has
// something holding it open long enough to drop what it made.
func (r *PgTenantUserReconciler) hold(ctx context.Context, user *pgelasticv1alpha1.PgTenantUser) error {
	if !controllerutil.AddFinalizer(user, PgTenantUserFinalizer) {
		return nil
	}
	return r.Update(ctx, user)
}

func (r *PgTenantUserReconciler) publish(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
	status pgelasticv1alpha1.PgTenantUserStatus,
) error {
	if equality.Semantic.DeepEqual(user.Status, status) {
		return nil
	}
	user.Status = status
	return client.IgnoreNotFound(r.Status().Update(ctx, user))
}

func (r *PgTenantUserReconciler) reportReclaimFailure(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
	cause error,
) error {
	status := *user.Status.DeepCopy()
	setCondition(&status.Conditions, user.Generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, tenantuser.ReasonReclaimFailed, cause.Error())
	status.Phase = pgelasticv1alpha1.PgTenantUserPhaseFailed
	return r.publish(ctx, user, status)
}

func (r *PgTenantUserReconciler) pending(
	status *pgelasticv1alpha1.PgTenantUserStatus,
	generation int64,
	reason, message string,
) {
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionAccepted,
		metav1.ConditionFalse, reason, message)
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, reason, message)
}

func (r *PgTenantUserReconciler) notReady(
	status *pgelasticv1alpha1.PgTenantUserStatus,
	generation int64,
	reason, message string,
) {
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, reason, message)
}

func (r *PgTenantUserReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

// loginAllowed reads spec.login, which the CRD defaults to true - so an unset pointer means the
// field was never rendered rather than that somebody asked for a group role.
func loginAllowed(user *pgelasticv1alpha1.PgTenantUser) bool {
	return user.Spec.Login == nil || *user.Spec.Login
}

// userPhase is a pure projection of the conditions, and nothing may read it back.
func userPhase(conditions []metav1.Condition) pgelasticv1alpha1.PgTenantUserPhase {
	accepted := findCondition(conditions, pgelasticv1alpha1.ConditionAccepted)
	ready := findCondition(conditions, pgelasticv1alpha1.ConditionReady)
	switch {
	case accepted == nil || accepted.Status != metav1.ConditionTrue:
		return pgelasticv1alpha1.PgTenantUserPhasePending
	case ready != nil && ready.Status == metav1.ConditionTrue:
		return pgelasticv1alpha1.PgTenantUserPhaseReady
	case ready != nil && isFailureReason(ready.Reason):
		return pgelasticv1alpha1.PgTenantUserPhaseFailed
	default:
		return pgelasticv1alpha1.PgTenantUserPhasePending
	}
}

func isFailureReason(reason string) bool {
	return reason == tenantuser.ReasonProvisioningFailed || reason == tenantuser.ReasonReclaimFailed
}

// SetupWithManager wires the login controller.
func (r *PgTenantUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgTenantUser{}).
		// A tenant's binding moving is what puts a login on a different instance, where its
		// role does not exist yet.
		Watches(&pgelasticv1alpha1.PgTenant{},
			handler.EnqueueRequestsFromMapFunc(r.usersOfTenant)).
		// A login named in another's memberOf becoming Ready is what unblocks that other one.
		Watches(&pgelasticv1alpha1.PgTenantUser{},
			handler.EnqueueRequestsFromMapFunc(r.siblingsOf)).
		Named("pgtenantuser").
		Complete(tracing.Wrap("PgTenantUser", r))
}

func (r *PgTenantUserReconciler) usersOfTenant(ctx context.Context, object client.Object) []reconcile.Request {
	return r.requestsForTenant(ctx, object.GetNamespace(), object.GetName())
}

func (r *PgTenantUserReconciler) siblingsOf(ctx context.Context, object client.Object) []reconcile.Request {
	user, ok := object.(*pgelasticv1alpha1.PgTenantUser)
	if !ok {
		return nil
	}
	return r.requestsForTenant(ctx, user.Namespace, user.Spec.TenantRef.Name)
}

func (r *PgTenantUserReconciler) requestsForTenant(
	ctx context.Context,
	namespace, tenant string,
) []reconcile.Request {
	if tenant == "" {
		return nil
	}
	users := &pgelasticv1alpha1.PgTenantUserList{}
	if err := r.List(ctx, users, client.InNamespace(namespace),
		client.MatchingFields{index.TenantUserByTenant: tenant}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(users.Items))
	for i := range users.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&users.Items[i]),
		})
	}
	return requests
}
