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

package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Run is one migration's resolved inputs: everything the engine needs that does not come
// from PostgreSQL itself.
type Run struct {
	// Migration and Tenant identify the two objects involved.
	Migration TenantRef
	Tenant    TenantRef

	Phase    Phase
	Strategy Strategy
	Plan     Plan

	Preflight    PreflightInput
	Sequences    SequencePlan
	Verification VerificationLevel

	// ReplicationRole is the role the subscriber and pg_dump dial the source as, and the
	// role the cleanup ladder revokes from.
	ReplicationRole string
	// Owner is the role that owns the tenant's database on the target.
	Owner string

	AbortRequested       bool
	AbortMessage         string
	RollbackRequested    bool
	RollbackWindowClosed bool
	// SourceDropped records that the source database has already been dropped, which makes
	// the migration final.
	SourceDropped bool

	// QuiesceStartedAt is when the clients were first queued. The pause is measured from
	// here to the routing flip, which for the offline strategy deliberately spans the dump
	// and the restore.
	QuiesceStartedAt *time.Time
	DrainTimeout     time.Duration
	CutoverTimeout   time.Duration
	// FaultingSince is when the current run of failures began, and RetryBudget is how long it
	// may go on before the migration gives up. A transient failure - a refused connection
	// while a Service reselects its endpoints, a member restarting - must not end a move that
	// is otherwise proceeding.
	FaultingSince *time.Time
	RetryBudget   time.Duration
	// RollbackWindow is how long the source database is kept intact and connection-refusing
	// after a successful cutover.
	RollbackWindow time.Duration

	// MaxLagBytes is the lag under which Catchup may advance. Nil demands a fully caught-up
	// subscription, which trades a longer Catchup for the shortest possible pause.
	MaxLagBytes *int64
}

// StepResult is one step's observation plus the evidence worth publishing on the CR.
type StepResult struct {
	Observation Observation

	Preflight    *PreflightResult
	Verification *VerificationResult
	LagBytes     *int64
	Copied       *int32
	Total        *int32
	// PauseMillis is set once, on the step that flips routing.
	PauseMillis *int64
}

// Engine performs the effect of whichever phase a migration is in and reports what it
// observed. Nothing here decides a phase: that is Decide's job, and keeping the two apart
// is what makes every abort path testable without a database.
type Engine struct {
	SQL    SQL
	Shell  Shell
	Router Router
	// Now is injectable so the pause clock can be driven in tests.
	Now func() time.Time
}

func (e Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// Step runs the current phase's effect. An error is reported as a fault on the observation
// rather than returned, because a fault is a legitimate outcome the machine has a rule for
// - Failed, with the cleanup ladder run and the tenant still on the source - while a
// returned error would only make the controller requeue and try the same thing again.
func (e Engine) Step(ctx context.Context, run Run) StepResult {
	result := StepResult{Observation: Observation{
		Strategy:             run.Strategy,
		AbortRequested:       run.AbortRequested,
		AbortMessage:         run.AbortMessage,
		RollbackRequested:    run.RollbackRequested,
		RollbackWindowClosed: run.RollbackWindowClosed,
		SourceDropped:        run.SourceDropped,
	}}
	if run.AbortRequested || Terminal(run.Phase) {
		e.applyPauseBudget(&result, run)
		return result
	}

	var err error
	switch run.Phase {
	case pgelasticv1alpha1.TenantMigrationPhasePreflight:
		// Preflight cannot fault. A check that could not be evaluated is a refusal like any
		// other, and reporting it as an error would end the migration in Failed rather than
		// leaving it in Preflight where a human can read what went wrong and retry.
		e.preflight(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhaseProvisioning:
		err = e.provision(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhasePreWarm:
		err = e.preWarm(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhaseCopying:
		err = e.copy(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhaseCatchup:
		err = e.catchup(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhaseQuiescing:
		err = e.quiesce(ctx, run, &result)
	case pgelasticv1alpha1.TenantMigrationPhaseCutover:
		err = e.cutover(ctx, run, &result)
	default:
		err = fmt.Errorf("unknown phase %q", run.Phase)
	}
	result.Observation.Fault = err
	result.Observation.FaultBudgetExceeded = e.faultBudgetExceeded(run, err)
	e.applyPauseBudget(&result, run)
	return result
}

// faultBudgetExceeded reports whether this phase has been failing for longer than the retry
// budget. The clock starts at the first failure of the run rather than at this one, so a
// phase that fails, succeeds and fails again gets the full budget each time.
func (e Engine) faultBudgetExceeded(run Run, err error) bool {
	if err == nil {
		return false
	}
	if run.RetryBudget <= 0 {
		return true
	}
	if run.FaultingSince == nil {
		// This is the first failure of the run, so the budget has not started being spent.
		return false
	}
	return e.now().Sub(*run.FaultingSince) > run.RetryBudget
}

// applyPauseBudget converts the two timeouts into observations. They are evaluated on every
// step rather than only on the step that could exceed them, so a controller that was not
// running for a minute still sees the budget as spent when it comes back.
func (e Engine) applyPauseBudget(result *StepResult, run Run) {
	if run.QuiesceStartedAt == nil {
		return
	}
	elapsed := e.now().Sub(*run.QuiesceStartedAt)
	if run.Phase == pgelasticv1alpha1.TenantMigrationPhaseQuiescing &&
		run.DrainTimeout > 0 && elapsed > run.DrainTimeout && !result.Observation.Drained {
		result.Observation.DrainDeadlineExceeded = true
	}
	if run.CutoverTimeout > 0 && elapsed > run.CutoverTimeout && !result.Observation.CutoverComplete {
		result.Observation.CutoverDeadlineExceeded = true
	}
}

func (e Engine) preflight(ctx context.Context, run Run, result *StepResult) {
	verdict := RunPreflight(ctx, e.SQL, run.Preflight)
	result.Preflight = &verdict
	result.Observation.PreflightPassed = verdict.Passed()
}

func (e Engine) provision(ctx context.Context, run Run, result *StepResult) error {
	if err := GrantSourceReads(ctx, e.SQL, run.Plan.Source, run.ReplicationRole); err != nil {
		return err
	}
	online := run.Strategy == pgelasticv1alpha1.TenantMigrationOnline
	if err := ProvisionTarget(ctx, e.SQL, e.Shell, run.Plan, run.Owner, online); err != nil {
		return err
	}
	if online {
		if err := StartReplication(ctx, e.SQL, run.Plan); err != nil {
			return err
		}
	}
	result.Observation.Provisioned = true
	return nil
}

func (e Engine) preWarm(ctx context.Context, run Run, result *StepResult) error {
	if err := e.Router.PreWarm(ctx, run.Tenant, run.Plan.Target.Instance); err != nil {
		return err
	}
	result.Observation.PreWarmed = true
	return nil
}

func (e Engine) copy(ctx context.Context, run Run, result *StepResult) error {
	if run.Strategy == pgelasticv1alpha1.TenantMigrationOffline {
		if err := CopyOffline(ctx, e.Shell, run.Plan); err != nil {
			return err
		}
		if err := DiscardDump(ctx, e.Shell, run.Plan); err != nil {
			return err
		}
		result.Observation.CopyComplete = true
		return nil
	}

	progress, err := ReadCopyProgress(ctx, e.SQL, run.Plan)
	if err != nil {
		return err
	}
	result.Copied, result.Total = &progress.Copied, &progress.Total
	result.Observation.CopyComplete = progress.Done()
	return nil
}

func (e Engine) catchup(ctx context.Context, run Run, result *StepResult) error {
	lag, err := ReadLagBytes(ctx, e.SQL, run.Plan)
	if err != nil {
		return err
	}
	result.LagBytes = &lag
	threshold := int64(0)
	if run.MaxLagBytes != nil {
		threshold = *run.MaxLagBytes
	}
	result.Observation.CaughtUp = lag <= threshold
	return nil
}

// quiesce queues the tenant's clients and then establishes that the source has actually
// gone quiet.
//
// The drain evidence is read from pg_stat_activity rather than taken from the proxy. A
// proxy that believes it has drained and a database with a backend still in a transaction
// are two different claims, and it is the second one the cutover depends on.
func (e Engine) quiesce(ctx context.Context, run Run, result *StepResult) error {
	if err := e.Router.Quiesce(ctx, run.Tenant, run.Migration.String()); err != nil {
		return err
	}
	inFlight, err := scalarInt64(ctx, e.SQL, run.Plan.Source, inFlightQuery)
	if err != nil {
		return err
	}
	result.Observation.Drained = inFlight == 0
	return nil
}

// inFlightQuery counts the tenant's own backends that are still inside a transaction.
// Idle connections are not in flight: the proxy is holding those sockets on purpose.
const inFlightQuery = `SELECT count(*)::text FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend'
  AND pid <> pg_backend_pid() AND state <> 'idle'`

// cutover is the only place the tenant's routing changes, and every step before the flip is
// a gate on it.
func (e Engine) cutover(ctx context.Context, run Run, result *StepResult) error {
	if run.Strategy == pgelasticv1alpha1.TenantMigrationOnline {
		caught, err := e.awaitFinalLSN(ctx, run)
		if err != nil {
			return err
		}
		if !caught {
			return nil
		}
	}

	if _, err := run.Sequences.Reconcile(ctx, e.SQL, run.Plan.Source, run.Plan.Target); err != nil {
		return err
	}

	verdict, err := Verifier{SQL: e.SQL, Level: run.Verification}.Verify(ctx, run.Plan.Source, run.Plan.Target)
	if err != nil {
		// A verdict that could not be gathered is not a verdict. Publishing the half-filled
		// one would report Verified=True off evidence the verifier never managed to collect.
		return fmt.Errorf("verification could not be completed: %w", err)
	}
	result.Verification = &verdict
	if !verdict.Equivalent() {
		return errors.New("verification refused the cutover: " + verdict.Message())
	}

	if err := FenceSource(ctx, e.SQL, run.Plan.Source); err != nil {
		return err
	}
	if err := e.Router.Route(ctx, run.Tenant, run.Plan.Target.Instance); err != nil {
		return err
	}
	if run.QuiesceStartedAt != nil {
		pause := e.now().Sub(*run.QuiesceStartedAt).Milliseconds()
		result.PauseMillis = &pause
	}
	result.Observation.CutoverComplete = true
	return nil
}

// awaitFinalLSN holds the cutover until the subscriber has confirmed everything the source
// had written when the quiesce took hold. Flipping before that point loses whatever is
// still in flight, which is the one loss no verification afterwards could detect: the rows
// were never on the target to be compared.
func (e Engine) awaitFinalLSN(ctx context.Context, run Run) (bool, error) {
	lsn, err := CurrentWALLSN(ctx, e.SQL, run.Plan)
	if err != nil {
		return false, err
	}
	return ConfirmedThrough(ctx, e.SQL, run.Plan, lsn)
}

// Apply performs the effects a decision calls for, in the order that keeps the tenant
// reachable: routing is restored before the physical objects behind it are removed.
func (e Engine) Apply(ctx context.Context, run Run, decision Decision) error {
	var problems []error
	if decision.Serving == ServingSource {
		if err := e.restoreSource(ctx, run); err != nil {
			problems = append(problems, err)
		}
	}
	if decision.ReleaseQuiesce {
		if err := e.Router.Release(ctx, run.Tenant); err != nil {
			problems = append(problems, fmt.Errorf("releasing the quiesce: %w", err))
		}
	}
	if decision.Cleanup {
		if err := Cleanup(ctx, e.SQL, run.Plan, run.ReplicationRole); err != nil {
			problems = append(problems, err)
		}
		if run.Strategy == pgelasticv1alpha1.TenantMigrationOffline && e.Shell != nil {
			if err := DiscardDump(ctx, e.Shell, run.Plan); err != nil {
				problems = append(problems, fmt.Errorf("discarding the staged dump: %w", err))
			}
		}
	}
	if decision.DiscardTarget {
		if err := DropTargetDatabase(ctx, e.SQL, run.Plan.Target); err != nil {
			problems = append(problems, fmt.Errorf("discarding the half-built target: %w", err))
		}
	}
	if decision.DropSource {
		if err := DropSourceDatabase(ctx, e.SQL, run.Plan.Source); err != nil {
			problems = append(problems, err)
		}
	}
	return errors.Join(problems...)
}

// restoreSource is the invariant made executable: whatever went wrong, the tenant ends up
// reachable on the instance it started on, with its database admitting connections again.
func (e Engine) restoreSource(ctx context.Context, run Run) error {
	routed, err := e.Router.RoutedTo(ctx, run.Tenant)
	if err != nil {
		return err
	}
	if routed == run.Plan.Source.Instance {
		return nil
	}
	if err := UnfenceSource(ctx, e.SQL, run.Plan.Source); err != nil {
		return fmt.Errorf("readmitting connections to the source: %w", err)
	}
	return e.Router.Route(ctx, run.Tenant, run.Plan.Source.Instance)
}
