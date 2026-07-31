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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// defaultSourceUtilizationCeiling mirrors the CRD default for
// forbidMoveWhenSourceUtilizationAbovePercent, so a spec that omits the field and a spec
// that spells the default out behave identically.
const defaultSourceUtilizationCeiling int32 = 65

// resolveStrategy turns Auto into a concrete strategy.
//
// Online is the answer whenever the pool has not said otherwise, and it is the answer the
// whole product is built around: its pause is a queued sub-second stall, so it is permitted
// during business hours, which is what makes reactive rebalancing viable at all. Offline is
// the fallback, and choosing it is a decision an operator makes rather than one a controller
// drifts into.
func resolveStrategy(
	requested pgelasticv1alpha1.TenantMigrationStrategy,
	pool *pgelasticv1alpha1.PgElasticPool,
) pgelasticv1alpha1.TenantMigrationStrategy {
	switch requested {
	case pgelasticv1alpha1.TenantMigrationOnline, pgelasticv1alpha1.TenantMigrationOffline:
		return requested
	}
	if pool != nil && pool.Spec.Migration != nil &&
		pool.Spec.Migration.DefaultStrategy == pgelasticv1alpha1.MigrationOffline {
		return pgelasticv1alpha1.TenantMigrationOffline
	}
	return pgelasticv1alpha1.TenantMigrationOnline
}

// sourceConnInfo is the libpq string the subscriber and pg_dump reach the source with.
//
// It addresses the source's read-write Service rather than a member, so a failover during
// the migration moves the connection with the primary. The credential is the replication
// role: the superuser has no password and is reachable only over a Unix socket, by design.
func sourceConnInfo(source *pgelasticv1alpha1.PgInstance, database, password string) string {
	return fmt.Sprintf("host=%s.%s.svc port=%d user=%s password=%s dbname=%s",
		provision.PrimaryServiceName(source.Name), source.Namespace, provision.PostgresPort,
		provision.ReplicationRole, password, database)
}

// replicationPassword reads an instance's replication credential, which is what the
// subscriber and pg_dump dial it as. The superuser cannot be used for either: it has no
// password at all and is reachable only over a Unix socket.
//
// It is a free function rather than a method because both a migration and a tenant-scoped
// restore dial an instance this way, and a restore's source is an instance the restore
// itself created.
func replicationPassword(
	ctx context.Context, reader client.Reader, namespace, instance string,
) (string, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: provision.CredentialsSecretName(instance)}
	if err := reader.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("credentials Secret for %q: %w", instance, err)
	}
	password, ok := secret.Data[provision.SecretKeyReplicationPassword]
	if !ok {
		return "", fmt.Errorf("the credentials Secret for %q has no %s",
			instance, provision.SecretKeyReplicationPassword)
	}
	return string(password), nil
}

// preflightInput assembles the gate's inputs. Storage headroom and the tenant's coldness
// come from the control plane's own observations rather than from PostgreSQL, because
// neither is a question a database can answer about itself.
func preflightInput(
	object *pgelasticv1alpha1.PgTenantMigration,
	tenant *pgelasticv1alpha1.PgTenant,
	source, target *pgelasticv1alpha1.PgInstance,
	pool *pgelasticv1alpha1.PgElasticPool,
	run migration.Run,
) migration.PreflightInput {
	input := migration.PreflightInput{
		Source:                      run.Plan.Source,
		Target:                      run.Plan.Target,
		SourceStandbys:              standbysOf(source),
		Online:                      run.Strategy == pgelasticv1alpha1.TenantMigrationOnline,
		RequireReplicaIdentity:      true,
		ForbidPreparedTransactions:  true,
		RequireColdTenant:           true,
		MaxSourceUtilizationPercent: sourceUtilizationCeiling(object, pool),
		TargetFreeBytes:             freeBytes(target),
		// The allowlist is what the tenant asked for and admission accepted. Anything
		// installed on the source that is not on it got there some other way.
		AllowedExtensions: tenant.Spec.Extensions,
		ExpectedCollation: migration.ContractPair{
			SourceRecorded: contractOf(source),
			TargetRecorded: contractOf(target),
		},
	}
	if tenant.Status.Utilization != nil {
		input.TenantIsCold = tenant.Status.Utilization.IsCold
	}
	if spec := object.Spec.Preflight; spec != nil {
		input.RequireReplicaIdentity = ptr.Deref(spec.RequireReplicaIdentity, true)
		input.ForbidPreparedTransactions = ptr.Deref(spec.ForbidPreparedTransactions, true)
		input.RequireColdTenant = ptr.Deref(spec.RequireColdTenant, true)
	}
	return input
}

func sourceUtilizationCeiling(
	object *pgelasticv1alpha1.PgTenantMigration, pool *pgelasticv1alpha1.PgElasticPool,
) int32 {
	if spec := object.Spec.Preflight; spec != nil && spec.ForbidMoveWhenSourceUtilizationAbovePercent != nil {
		return *spec.ForbidMoveWhenSourceUtilizationAbovePercent
	}
	if pool != nil && pool.Spec.Rebalancing != nil &&
		pool.Spec.Rebalancing.ForbidMoveWhenSourceUtilizationAbovePercent != nil {
		return *pool.Spec.Rebalancing.ForbidMoveWhenSourceUtilizationAbovePercent
	}
	return defaultSourceUtilizationCeiling
}

// standbysOf lists the members that are not the primary. Each of them carries half of the
// failover-slot contract, so each has to be asked directly.
func standbysOf(instance *pgelasticv1alpha1.PgInstance) []string {
	standbys := make([]string, 0, len(instance.Status.Instances))
	for _, member := range instance.Status.Instances {
		if member.Name != instance.Status.CurrentPrimary {
			standbys = append(standbys, member.Name)
		}
	}
	return standbys
}

// freeBytes is the target's unused data volume, which is the headroom the tenant has to fit
// into twice over. An instance that has published no storage status yet reports zero, and
// zero fails the check rather than passing it: unknown headroom is not headroom.
func freeBytes(instance *pgelasticv1alpha1.PgInstance) int64 {
	storage := instance.Status.Storage
	if storage == nil || storage.Allocated == nil {
		return 0
	}
	allocated := storage.Allocated.Value()
	if storage.Used == nil {
		return allocated
	}
	free := allocated - storage.Used.Value()
	if free < 0 {
		return 0
	}
	return free
}

// contractOf renders an instance's recorded collation contract as one comparable string.
func contractOf(instance *pgelasticv1alpha1.PgInstance) string {
	contract := instance.Status.CollationContract
	if contract == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%d|%t",
		contract.Encoding, contract.Collate, contract.Ctype, contract.LocaleProvider,
		contract.Locale, contract.ICURules, contract.WALSegmentSize, contract.DataChecksums)
}

func poolVerification(pool *pgelasticv1alpha1.PgElasticPool) pgelasticv1alpha1.MigrationVerification {
	if pool == nil || pool.Spec.Migration == nil {
		return pgelasticv1alpha1.MigrationVerifyRowCounts
	}
	return pool.Spec.Migration.Verification
}

func sequencePlan(spec *pgelasticv1alpha1.TenantMigrationSequenceHandling) migration.SequencePlan {
	plan := migration.SequencePlan{
		Mode:      pgelasticv1alpha1.SequenceHandlingSetvalWithGap,
		SafetyGap: migration.DefaultSafetyGap,
	}
	if spec == nil {
		return plan
	}
	if spec.Mode != "" {
		plan.Mode = spec.Mode
	}
	if spec.SafetyGap != nil {
		plan.SafetyGap = *spec.SafetyGap
	}
	return plan
}

// quiesceStart is when the tenant's clients were first queued, read back off the Quiesced
// condition. The condition rather than a field carries it because a condition's
// lastTransitionTime survives a controller restart, and the pause has to be measured across
// one rather than reset by it.
func quiesceStart(status *pgelasticv1alpha1.PgTenantMigrationStatus) *time.Time {
	condition := meta.FindStatusCondition(status.Conditions, migration.ConditionQuiesced)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		return nil
	}
	return ptr.To(condition.LastTransitionTime.Time)
}

// sourceDropped reads back the one irreversible act a completed migration performs. It is
// read from the condition the controller wrote when it did it, because the alternative -
// asking PostgreSQL whether the database exists - would answer "no, so drop it" about a
// database a later migration had legitimately recreated under the same name.
func sourceDropped(status *pgelasticv1alpha1.PgTenantMigrationStatus) bool {
	condition := meta.FindStatusCondition(status.Conditions, migration.ConditionSucceeded)
	return condition != nil && condition.Reason == migration.ReasonSourceDropped
}

// faultingSince is when the current run of failures began, read back off the Retrying
// condition so the budget survives a controller restart rather than being reset by one.
func faultingSince(status *pgelasticv1alpha1.PgTenantMigrationStatus) *time.Time {
	condition := meta.FindStatusCondition(status.Conditions, migration.ConditionRetrying)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		return nil
	}
	return ptr.To(condition.LastTransitionTime.Time)
}

func retryingReason(fault error) string {
	if fault != nil {
		return migration.ReasonRetrying
	}
	return migration.ReasonProgressing
}

func retryingMessage(fault error) string {
	if fault != nil {
		return fault.Error()
	}
	return "the current phase is not failing"
}

func rollbackWindowClosed(status *pgelasticv1alpha1.PgTenantMigrationStatus, now time.Time) bool {
	return status.RollbackDeadline != nil && !now.Before(status.RollbackDeadline.Time)
}

// rollbackWindow is how long the source database is kept intact after the flip. The
// migration may narrow the pool's window but never widen it: the window is capacity the
// pool is lending, since the tenant occupies storage on two instances for its whole length.
func rollbackWindow(
	object *pgelasticv1alpha1.PgTenantMigration, pool *pgelasticv1alpha1.PgElasticPool,
) time.Duration {
	window := time.Hour
	if pool != nil && pool.Spec.Migration != nil && pool.Spec.Migration.RollbackWindow != nil {
		window = pool.Spec.Migration.RollbackWindow.Duration
	}
	if object.Spec.RollbackWindow != nil && object.Spec.RollbackWindow.Duration < window {
		window = object.Spec.RollbackWindow.Duration
	}
	return window
}

func durationOr(value *metav1.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	return value.Duration
}

// requeueFor is how long to wait before looking at a migration again. The quiesced phases
// are polled an order of magnitude faster than the rest, because every millisecond spent
// between reconciles there is a millisecond of the pause clients were promised.
func requeueFor(
	phase pgelasticv1alpha1.TenantMigrationPhase, strategy pgelasticv1alpha1.TenantMigrationStrategy,
) time.Duration {
	switch {
	case migration.Terminal(phase):
		return migrationSettledInterval
	case migration.Quiesced(phase, strategy):
		return migrationPausePollInterval
	default:
		return migrationPollInterval
	}
}

func onlineReason(online bool) string {
	if online {
		return migration.ReasonOnlineChosen
	}
	return migration.ReasonOfflineChosen
}

func preflightReason(passed bool) string {
	if passed {
		return migration.ReasonPreflightPassed
	}
	return migration.ReasonPreflightRefused
}

func verifiedReason(equivalent bool) string {
	if equivalent {
		return migration.ReasonVerified
	}
	return migration.ReasonNotEquivalent
}

func strategyMessage(strategy pgelasticv1alpha1.TenantMigrationStrategy) string {
	if strategy == pgelasticv1alpha1.TenantMigrationOnline {
		return "moving by logical replication, whose cutover pause is a queued sub-second stall"
	}
	return "moving by pg_dump and pg_restore, whose pause is measured in tens of seconds"
}

// servingMessage states, on every terminal and non-terminal outcome alike, which instance
// the tenant's clients are reaching. It is the one sentence an operator reading a failed
// migration needs first.
func servingMessage(decision migration.Decision, run migration.Run) string {
	instance := run.Plan.Source.Instance
	if decision.Serving == migration.ServingTarget {
		instance = run.Plan.Target.Instance
	}
	return fmt.Sprintf("%s; the tenant is serving from %s (%s)",
		decision.Message, decision.Serving, instance)
}
