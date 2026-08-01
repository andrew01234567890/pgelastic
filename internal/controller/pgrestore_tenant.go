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
	"path/filepath"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// recoverySuffix names the throwaway instance a tenant restore recovers into.
//
// A tenant restore is a whole-instance point-in-time recovery with one database lifted out
// of it, because there is no such thing as restoring one database out of a physical backup:
// WAL is instance-wide, and replaying it to a moment replays every tenant on the instance to
// that moment. The instance is throwaway because the other tenants on it are copies of live
// customers' pasts and must never be reachable.
const recoverySuffix = "-recovery"

// cleanupBudget bounds the defers that undo a copy. They run on a detached context, so nothing
// else will stop them, and they run while the operator is most likely shutting down.
const cleanupBudget = 30 * time.Second

// recoveryInstanceName is the instance a tenant restore recovers into.
func recoveryInstanceName(restore *pgelasticv1alpha1.PgRestore) string {
	return restore.Name + recoverySuffix
}

// reconcileTenantRestore puts one tenant back without touching its neighbours.
//
// The shape is: recover the whole instance to the moment asked for, into an instance nobody
// can reach; lift the one database out of it; load it over the live one; throw the recovery
// instance away. Everything after the recovery is the offline migration path run in place -
// the same pg_dump and pg_restore, the same fencing, the same scratch directory on the
// volume whose headroom was already measured - because moving a tenant between two live
// instances and replacing a tenant from a dead one are the same copy with different ends.
func (r *PgRestoreReconciler) reconcileTenantRestore(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
) (time.Duration, error) {
	log := logf.FromContext(ctx)

	if r.SQL == nil || r.Shell == nil {
		return 0, fmt.Errorf(
			"this operator was started without the SQL and shell ports a tenant restore " +
				"copies through")
	}

	tenant, reason, err := r.tenantUnderRestore(ctx, restore)
	if err != nil || reason != "" {
		status.Error = reason
		return restoreRequeue, err
	}

	recovery := &pgelasticv1alpha1.PgInstance{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace, Name: recoveryInstanceName(restore),
	}, recovery)
	switch {
	case apierrors.IsNotFound(err):
		return r.startTenantRecovery(ctx, restore, status)
	case err != nil:
		return 0, err
	}

	if !meta.IsStatusConditionTrue(recovery.Status.Conditions, pgelasticv1alpha1.ConditionReady) {
		status.Phase = pgelasticv1alpha1.RestorePhaseRecovering
		return restoreRequeue, nil
	}

	status.Phase = pgelasticv1alpha1.RestorePhaseExtracting
	if err := r.replaceTenant(ctx, restore, status, tenant, recovery); err != nil {
		if status.CopyStartedAt == nil {
			// Nothing has been written yet - reading a collation, resolving a credential,
			// granting reads on the throwaway recovery instance. These fail for reasons that
			// pass, and a recovery instance that has only just gone Ready is the commonest of
			// them. Failing terminally here would strand a restore that had done nothing
			// wrong, and an immutable spec means the only way back is to create another one.
			log.Error(err, "the tenant restore could not start its copy; retrying")
			status.Error = err.Error()
			return restoreRequeue, nil
		}
		// Past the clearance there is no way to tell a failure that touched nothing from one
		// that had already begun loading with --clean, and retrying the second kind drops
		// objects out of a database somebody now needs to look at. The recovery instance is
		// deliberately left standing: it holds the only copy of what the tenant was supposed
		// to end up containing.
		log.Error(err, "the tenant could not be replaced from the recovered instance")
		status.Phase = pgelasticv1alpha1.RestorePhaseFailed
		status.Error = err.Error()
		return 0, nil
	}

	status.Phase = pgelasticv1alpha1.RestorePhaseCompleted
	status.Error = ""
	// The recovery instance holds every other tenant of the source at the restored moment.
	// Leaving it up is leaving a readable copy of other customers' data behind, so it is torn
	// down on the way out. A failure to delete it is not a failure of the restore, though: the
	// copy is done and the tenant is correct, and reporting otherwise would tell somebody
	// their production database might be half-replaced when it is not. The PgRestore's own
	// finalizer removes the instance when the restore object goes.
	if err := r.Delete(ctx, recovery); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "the recovery instance was left behind", "instance", recovery.Name)
	}
	return 0, nil
}

// startTenantRecovery mints the throwaway instance, planned exactly as an instance-scope
// restore is, and named after the restore so a second reconcile finds it rather than
// creating another.
func (r *PgRestoreReconciler) startTenantRecovery(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
) (time.Duration, error) {
	plan, reason, err := r.planRestore(ctx, restore)
	if err != nil {
		return 0, err
	}
	if reason != "" {
		status.Error = reason
		return restoreRequeue, nil
	}
	plan.Name = recoveryInstanceName(restore)
	status.BackupID = plan.Spec.Restore.BackupID
	status.Error = ""
	status.Phase = pgelasticv1alpha1.RestorePhaseRecovering

	if err := r.Create(ctx, plan); err != nil && !apierrors.IsAlreadyExists(err) {
		status.Error = err.Error()
		return restoreRequeue, nil
	}
	if err := r.handOverCredentials(
		ctx, restore.Spec.SourceInstanceRef.Name, plan); err != nil {
		status.Error = err.Error()
	}
	return restoreRequeue, nil
}

// replaceTenant lifts one database out of the recovered instance and loads it over the live
// one.
//
// The tenant is fenced for the whole copy. A dump taken while writes continue is behind by
// whatever was written during it, and unlike a migration there is no replication stream to
// close that gap with - so the alternative to a pause is a restore that silently keeps some
// of the data it was asked to discard.
// Every failure it returns is terminal to the caller. It is only reached once the restore
// has recorded, durably, that a copy was cleared to start, and past that point a failure
// that touched nothing looks exactly like one that had begun loading with --clean.
func (r *PgRestoreReconciler) replaceTenant(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
	tenant *pgelasticv1alpha1.PgTenant,
	recovery *pgelasticv1alpha1.PgInstance,
) (copyErr error) {
	live := migration.Endpoint{
		Namespace: restore.Namespace,
		Instance:  tenant.Status.Binding.InstanceRef.Name,
		Database:  tenant.Spec.DatabaseName,
	}
	recovered := migration.Endpoint{
		Namespace: restore.Namespace,
		Instance:  recovery.Name,
		Database:  tenant.Spec.DatabaseName,
	}

	// Restoring under a different collation produces indexes silently inconsistent with
	// their heap ordering: no error, wrong results, discovered by a customer. The recovery
	// instance inherits its collation from the backup it was restored from, so a source
	// whose contract has since changed is caught here rather than at the first index scan.
	if err := r.checkCollationMatches(ctx, recovered, live); err != nil {
		return err
	}

	// SourceConnInfo is the only thing that points pg_dump at the recovered instance. Both
	// commands run inside the live target's own Pod, so plan.Source addresses nothing on the
	// offline path: without a connection string libpq falls back to its defaults and dumps
	// the target's own local database over the top of itself.
	//
	// The credential is the recovery instance's, and it works because a restored instance is
	// given its source's credentials rather than fresh ones - its catalogue is the source's,
	// copied verbatim by pgbackrest, so nothing else would authenticate.
	password, err := replicationPassword(ctx, r.Client, restore.Namespace, recovery.Name)
	if err != nil {
		return fmt.Errorf("could not read the recovered instance's replication credential: %w", err)
	}

	plan := migration.Plan{
		Source:         recovered,
		Target:         live,
		SourceConnInfo: sourceConnInfo(recovery, tenant.Spec.DatabaseName, password),
		Concurrency:    migration.DefaultDumpJobs,
		DumpDir:        filepath.Join(migration.ScratchDir, restore.Namespace+"_"+restore.Name),
	}

	// The replication role can authenticate against the recovered instance and cannot read a
	// thing on it. A tenant's database revokes PUBLIC and grants CONNECT back to its own roles
	// and the ops role only, and the recovered copy inherits exactly those grants, so pg_dump
	// is refused before it reads a row. The migration path issues the same grants before its
	// own offline copy.
	//
	// They do not die with the recovery instance, however throwaway it is. pg_dump captures
	// ACLs and pg_restore writes them into what it loads, so these grants ride the dump into
	// the live tenant - which would leave a credential that lives in every member's
	// environment holding SELECT on a customer's data. The migration path revokes them on the
	// far side for exactly this reason; so does the deferred revoke below.
	if err := migration.GrantSourceReads(
		ctx, r.SQL, recovered, provision.ReplicationRole); err != nil {
		return fmt.Errorf("could not give the dump read access to the recovered tenant: %w", err)
	}

	// The cleanup defers run on a context that outlives this reconcile's. What they exist to
	// undo is the copy being killed - a rolled Pod, a lost lease - and both of those cancel
	// ctx, so a readmit issued on it could not send a single statement: the undo would be
	// registered and unrunnable in exactly the case it was written for.
	cleanup, stopCleanup := context.WithTimeout(context.WithoutCancel(ctx), cleanupBudget)
	defer stopCleanup()

	// Registered before the unfence so that it runs after it: the tenant is fenced for the
	// whole copy, and a fenced database refuses connections, so the revoke has nothing to
	// connect to until the unfence has run. Deferred rather than sequential because a copy
	// that failed half way can still have written the ACLs.
	// Failing the restore rather than logging. These reads are on the live tenant's own
	// tables, and a restore that reported Completed with them still in place would never be
	// looked at again - isTerminalRestore sees to that - so the grant would outlive everyone
	// who could have noticed it. The migration path fails for the same reason.
	defer func() {
		if err := migration.RevokeReplicationReads(cleanup, r.SQL, live); err != nil {
			logf.FromContext(ctx).Error(err, "the replication role was left holding reads on "+
				"the restored tenant", "tenant", tenant.Name)
			if copyErr == nil {
				copyErr = fmt.Errorf("the copy finished and the replication role's reads on "+
					"%s could not be taken back: %w", tenant.Spec.DatabaseName, err)
			}
		}
	}()

	// The roles to hold out come from the recovered copy, which carries the same ones: the
	// live database is about to be rewritten from it, and enumerating there means the answer
	// does not depend on a live database that is halfway through being replaced.
	tenantRoles, err := migration.EnumerateTenantRoles(ctx, r.SQL, recovered)
	if err != nil {
		return fmt.Errorf("could not read the roles the tenant's database depends on: %w", err)
	}

	// The last moment at which stopping costs nothing: everything above this reads the
	// recovered instance or the catalogue, and the next statement is the first that alters the
	// live tenant. The clearance is written and flushed here so that it is on the API server
	// before anything is touched, and the copy runs in this same pass - a clearance recorded
	// by one pass and acted on by the next would be indistinguishable from one left behind by
	// a pass that died, which is exactly what converge refuses on.
	//
	// If the flush fails, the marker is taken back off so the caller treats this as the
	// nothing-happened failure it is.
	status.CopyStartedAt = ptr.To(metav1.Now())
	if err := r.publish(ctx, restore, status); err != nil {
		status.CopyStartedAt = nil
		return fmt.Errorf("could not record that the copy was about to start: %w", err)
	}

	// Readmission runs on every exit, successful or not, and is registered before the
	// hold-out rather than after it. A hold-out that fails part way through has already
	// revoked from some of the roles, and returning with nothing registered leaves those
	// roles locked out of a live production database permanently, recoverable only by a
	// human issuing GRANT by hand. held is what was read before any revoke, so it is
	// complete whether or not the revokes were.
	var held []migration.Held
	defer func() {
		if err := migration.ReadmitTenant(cleanup, r.SQL, live, held); err != nil {
			logf.FromContext(ctx).Error(err, "the tenant was left unable to connect",
				"tenant", tenant.Name)
			// Failing the restore rather than only logging. A restore that reported Completed
			// would be terminal, so nothing would ever put these grants back, and the roles
			// would hold nothing on their own database while every client authenticating as
			// them got "permission denied". That is a live outage, and a louder one than the
			// leftover reads the defer above fails for.
			if copyErr == nil {
				copyErr = fmt.Errorf("the copy finished but %s was left unable to connect: %w",
					tenant.Spec.DatabaseName, err)
			}
		}
	}()
	held, err = migration.HoldTenantOut(ctx, r.SQL, live, tenantRoles)
	if err != nil {
		return fmt.Errorf("could not hold the tenant still for the copy: %w", err)
	}
	// The dump is the size of the tenant and lands on the target's data volume. Leaving one
	// behind is how the next restore fails its headroom check for reasons nobody can find.
	defer func() {
		if err := migration.DiscardDump(cleanup, r.Shell, plan); err != nil {
			logf.FromContext(ctx).Error(err, "the staged dump was left behind",
				"dumpDir", plan.DumpDir)
		}
	}()

	status.Phase = pgelasticv1alpha1.RestorePhaseLoading
	copyErr = migration.CopyOffline(ctx, r.Shell, plan)
	return copyErr
}

// checkCollationMatches refuses a copy between two databases whose text-handling identity
// differs.
func (r *PgRestoreReconciler) checkCollationMatches(
	ctx context.Context,
	source, target migration.Endpoint,
) error {
	from, err := migration.ReadCollation(ctx, r.SQL, source)
	if err != nil {
		return fmt.Errorf("could not read the recovered database's collation tuple: %w", err)
	}
	to, err := migration.ReadCollation(ctx, r.SQL, target)
	if err != nil {
		return fmt.Errorf("could not read the live database's collation tuple: %w", err)
	}
	if from != to {
		return fmt.Errorf(
			"the recovered database's collation tuple (%v) differs from the live one (%v); "+
				"loading across that difference produces indexes inconsistent with their heap "+
				"ordering, which is wrong results and no error", from, to)
	}
	return nil
}

// tenantUnderRestore resolves the tenant being put back, and says why it cannot be when it
// cannot.
func (r *PgRestoreReconciler) tenantUnderRestore(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
) (*pgelasticv1alpha1.PgTenant, string, error) {
	if restore.Spec.TenantRef == nil {
		return nil, "a tenant-scoped restore has to name the tenant it is putting back", nil
	}
	tenant := &pgelasticv1alpha1.PgTenant{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace, Name: restore.Spec.TenantRef.Name,
	}, tenant)
	switch {
	case apierrors.IsNotFound(err):
		return nil, fmt.Sprintf("no tenant named %s exists in this namespace",
			restore.Spec.TenantRef.Name), nil
	case err != nil:
		return nil, "", err
	case tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef.Name == "":
		// A tenant with no database to load over is a tenant that was never provisioned.
		// Creating one here would turn a restore into a provisioning path with none of the
		// admission accounting that goes with it.
		return nil, fmt.Sprintf("%s is not bound to an instance, so there is nothing to "+
			"restore over", tenant.Name), nil
	}
	return tenant, "", nil
}

// deleteRecoveryInstance tears the throwaway instance down.
//
// It is also the deletion path: a tenant restore abandoned halfway leaves a full copy of
// every tenant on the source instance running, and nothing else would ever remove it.
func (r *PgRestoreReconciler) deleteRecoveryInstance(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
) error {
	recovery := &pgelasticv1alpha1.PgInstance{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace, Name: recoveryInstanceName(restore),
	}, recovery)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, recovery))
}
