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
	"slices"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Phase and Strategy are the API's own vocabulary. They are aliased rather than redeclared
// so that a phase written to the CR and a phase the machine reasons about cannot drift.
type (
	Phase    = pgelasticv1alpha1.TenantMigrationPhase
	Strategy = pgelasticv1alpha1.TenantMigrationStrategy
)

// Serving names the instance the tenant's clients reach. It is the migration's single
// safety invariant: every phase except Completed leaves it on the source, so an abort at
// any boundary is a decision about physical objects rather than about availability.
type Serving string

const (
	// ServingSource means clients reach the instance the tenant was bound to on entry.
	ServingSource Serving = "Source"
	// ServingTarget means the routing table has been flipped.
	ServingTarget Serving = "Target"
)

// Condition types published on a PgTenantMigration. They are declared here rather than
// alongside the shared condition vocabulary because they describe this machine only.
const (
	// ConditionPreflightPassed reports the hard gate's verdict. Its message names every
	// failing check and the objects that failed it, because a refusal a human cannot act on
	// is indistinguishable from a silent degrade.
	ConditionPreflightPassed = "PreflightPassed"
	// ConditionOnline reports which strategy the migration resolved to, so a spec of Auto
	// cannot silently mean Online on one reconcile and Offline on the next.
	ConditionOnline = "Online"
	// ConditionRetrying reports that the current phase is failing and being retried. Its
	// lastTransitionTime is when the run of failures began, which is what the retry budget is
	// measured against.
	ConditionRetrying = "Retrying"
	// ConditionQuiesced reports that the tenant's clients are queued. Its
	// lastTransitionTime is the start of the pause clock, which is why the pause survives a
	// controller restart rather than being reset by one.
	ConditionQuiesced = "Quiesced"
	// ConditionVerified reports the verifier's verdict on the evidence gathered inside the
	// cutover pause.
	ConditionVerified = "Verified"
	// ConditionSucceeded reports the terminal outcome.
	ConditionSucceeded = "Succeeded"
)

// Condition reasons this machine can publish.
const (
	ReasonPreflightPassed  = "PreflightPassed"
	ReasonPreflightRefused = "PreflightRefused"
	ReasonProgressing      = "Progressing"
	ReasonWaiting          = "Waiting"
	ReasonCutoverComplete  = "CutoverComplete"
	ReasonAbortRequested   = "AbortRequested"
	ReasonFaulted          = "Faulted"
	ReasonRetrying         = "Retrying"
	ReasonDrainTimedOut    = "DrainTimedOut"
	ReasonCutoverTimedOut  = "CutoverTimedOut"
	ReasonRolledBack       = "RolledBack"
	ReasonSourceRetained   = "SourceRetained"
	ReasonSourceDropped    = "SourceDropped"
	ReasonOnlineChosen     = "OnlineChosen"
	ReasonOfflineChosen    = "OfflineChosen"
	ReasonVerified         = "Verified"
	ReasonNotEquivalent    = "NotEquivalent"
	ReasonUnresolved       = "Unresolved"
	ReasonInvalidSpec      = "InvalidSpec"
)

// onlineOrder is the phase sequence of a logical-replication move. The copy and the
// catch-up both happen while the tenant is still serving, which is what keeps the pause to
// the quiesce and the flip.
var onlineOrder = []Phase{
	pgelasticv1alpha1.TenantMigrationPhasePreflight,
	pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
	pgelasticv1alpha1.TenantMigrationPhasePreWarm,
	pgelasticv1alpha1.TenantMigrationPhaseCopying,
	pgelasticv1alpha1.TenantMigrationPhaseCatchup,
	pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
	pgelasticv1alpha1.TenantMigrationPhaseCutover,
	pgelasticv1alpha1.TenantMigrationPhaseCompleted,
}

// offlineOrder moves Copying to after Quiescing and drops Catchup entirely.
//
// pg_dump of a database still taking writes would produce a copy that is behind by
// whatever was written during the dump, and offline has no replication stream to close
// that gap with. So the copy happens inside the pause, which is exactly why the offline
// pause is measured in tens of seconds and why it is confined to a nightly window.
var offlineOrder = []Phase{
	pgelasticv1alpha1.TenantMigrationPhasePreflight,
	pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
	pgelasticv1alpha1.TenantMigrationPhasePreWarm,
	pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
	pgelasticv1alpha1.TenantMigrationPhaseCopying,
	pgelasticv1alpha1.TenantMigrationPhaseCutover,
	pgelasticv1alpha1.TenantMigrationPhaseCompleted,
}

// PhaseOrder is the sequence one strategy advances through.
func PhaseOrder(strategy Strategy) []Phase {
	if strategy == pgelasticv1alpha1.TenantMigrationOffline {
		return offlineOrder
	}
	return onlineOrder
}

// Terminal reports whether a phase is an end state.
func Terminal(phase Phase) bool {
	switch phase {
	case pgelasticv1alpha1.TenantMigrationPhaseCompleted,
		pgelasticv1alpha1.TenantMigrationPhaseFailed,
		pgelasticv1alpha1.TenantMigrationPhaseAborted,
		pgelasticv1alpha1.TenantMigrationPhaseRolledBack:
		return true
	default:
		return false
	}
}

// Quiesced reports whether the tenant's clients are queued in this phase. It is what the
// pause clock is measured over, and for the offline strategy it deliberately includes
// Copying.
func Quiesced(phase Phase, strategy Strategy) bool {
	order := PhaseOrder(strategy)
	quiesceIndex := slices.Index(order, pgelasticv1alpha1.TenantMigrationPhaseQuiescing)
	index := slices.Index(order, phase)
	return index >= quiesceIndex && index < len(order)-1
}

// Observation is everything one step of the machine decides from. It is assembled by the
// engine from PostgreSQL and the proxy; nothing here is read back out of the CR, so a
// stale status can never advance a phase.
type Observation struct {
	// Strategy is the resolved strategy. Auto is resolved before the machine runs, because
	// a strategy that could change between reconciles would change the phase order under a
	// migration that has already committed to physical objects.
	Strategy Strategy

	// AbortRequested is set by a human, by the sweeper, or by a guard the controller
	// applies before the effect runs.
	AbortRequested bool
	// AbortMessage explains the abort in the terminal condition.
	AbortMessage string
	// RollbackRequested asks a Completed migration to put the tenant back on the source
	// while its database is still intact.
	RollbackRequested bool
	// Fault is the error the current phase's effect returned.
	Fault error
	// FaultBudgetExceeded reports that this phase has been faulting for longer than the
	// retry budget. Until it does, a fault is retried rather than fatal: a refused TCP
	// connection while a Service reselects its endpoints is not a reason to abandon a move
	// that is otherwise proceeding, and every retry leaves the tenant on the source anyway.
	FaultBudgetExceeded bool

	// The per-phase readiness signals. Each is the answer to "may this phase be left", and
	// each is observed rather than assumed.
	PreflightPassed bool
	Provisioned     bool
	PreWarmed       bool
	CopyComplete    bool
	CaughtUp        bool
	Drained         bool
	CutoverComplete bool

	// DrainDeadlineExceeded is the drainTimeout budget spent in Quiescing.
	DrainDeadlineExceeded bool
	// CutoverDeadlineExceeded is the cutoverTimeout budget spent across the whole pause.
	CutoverDeadlineExceeded bool
	// RollbackWindowClosed means the source database may now be dropped.
	RollbackWindowClosed bool
	// SourceDropped means it already has been. Past that point the migration is final and
	// must never act on either instance again: the database it used to call "the source" is a
	// name another migration is free to reuse, and a finished migration that goes on dropping
	// it would be deleting a live tenant on a schedule.
	SourceDropped bool
}

// Decision is one step's outcome: the phase to publish and the effects the controller has
// to apply before publishing it.
type Decision struct {
	// Phase is the phase to publish.
	Phase Phase
	// Reason and Message go on the Succeeded or Progressing condition.
	Reason  string
	Message string
	// Serving is the instance the tenant's clients reach once this decision is applied.
	Serving Serving
	// Cleanup runs the cleanup ladder over the physical objects the migration created.
	Cleanup bool
	// ReleaseQuiesce lets queued clients through again, against whatever instance Serving
	// names. It is set on every abort from a quiesced phase: a migration that fails while
	// holding the tenant's sockets has converted a move into an outage.
	ReleaseQuiesce bool
	// DropSource drops the source database. It is set exactly once, when the rollback
	// window closes on a Completed migration.
	DropSource bool
	// Settled marks a decision that changes nothing. It is what stops a terminal migration's
	// conditions being rewritten on every reconcile: the message that says why a migration
	// failed is the most valuable thing it leaves behind, and overwriting it with "this
	// migration is finished" destroys the only record of the cause.
	Settled bool
	// DiscardTarget drops the half-built copy on the target. It is set on every departure
	// from the happy path and never on success. Leaving the copy behind is not merely untidy:
	// it is a database that stopped receiving changes at an arbitrary moment, and the next
	// migration of the same tenant would either fail on objects that already exist or, worse,
	// replicate into stale data nobody knows is stale.
	DiscardTarget bool
}

// Decide is the whole state machine, as a pure function.
//
// The ordering of the guards is the safety property: an abort request and a fault are both
// answered before any progress is considered, so no phase can advance past a request to
// stop, and every answer other than a successful cutover leaves Serving on the source.
func Decide(current Phase, observation Observation) Decision {
	switch current {
	case pgelasticv1alpha1.TenantMigrationPhaseFailed,
		pgelasticv1alpha1.TenantMigrationPhaseAborted,
		pgelasticv1alpha1.TenantMigrationPhaseRolledBack:
		return Decision{Phase: current, Serving: ServingSource, Settled: true,
			Reason:  reasonFor(current),
			Message: "the migration is finished and the tenant is serving from the source"}
	case pgelasticv1alpha1.TenantMigrationPhaseCompleted:
		return decideCompleted(observation)
	}

	if observation.AbortRequested {
		return abort(pgelasticv1alpha1.TenantMigrationPhaseAborted, ReasonAbortRequested,
			orDefault(observation.AbortMessage, "the migration was aborted by request"), current, observation)
	}
	if observation.Fault != nil {
		if !observation.FaultBudgetExceeded {
			return Decision{
				Phase: current, Serving: ServingSource, Reason: ReasonRetrying,
				Message: "phase " + string(current) + " is being retried after: " + observation.Fault.Error(),
			}
		}
		return abort(pgelasticv1alpha1.TenantMigrationPhaseFailed, ReasonFaulted,
			"phase "+string(current)+" kept failing for longer than the retry budget: "+
				observation.Fault.Error(), current, observation)
	}
	if current == pgelasticv1alpha1.TenantMigrationPhaseQuiescing && observation.DrainDeadlineExceeded {
		return abort(pgelasticv1alpha1.TenantMigrationPhaseFailed, ReasonDrainTimedOut,
			"in-flight transactions did not drain inside drainTimeout", current, observation)
	}
	if Quiesced(current, observation.Strategy) && observation.CutoverDeadlineExceeded {
		return abort(pgelasticv1alpha1.TenantMigrationPhaseFailed, ReasonCutoverTimedOut,
			"the cutover exceeded cutoverTimeout, which is the pause budget clients were promised",
			current, observation)
	}

	if !ready(current, observation) {
		return Decision{
			Phase: current, Serving: ServingSource, Reason: ReasonWaiting,
			Message: "waiting for " + string(current) + " to finish",
		}
	}

	next := nextPhase(current, observation.Strategy)
	if next == pgelasticv1alpha1.TenantMigrationPhaseCompleted {
		// The ladder runs on success as well as on every failure. The subscription has
		// delivered everything the fenced source will ever produce, so the slot behind it is
		// from this moment on nothing but a hold on the source primary's WAL.
		return Decision{
			Phase: next, Serving: ServingTarget, ReleaseQuiesce: true, Cleanup: true,
			Reason:  ReasonCutoverComplete,
			Message: "routing was flipped to the target and queued clients were released against it",
		}
	}
	return Decision{
		Phase: next, Serving: ServingSource, Reason: ReasonProgressing,
		Message: "entering " + string(next),
	}
}

// decideCompleted governs the only window in which a finished migration still has a
// decision left: routing can go back to the source until the source database is dropped.
func decideCompleted(observation Observation) Decision {
	switch {
	case observation.RollbackRequested && !observation.RollbackWindowClosed:
		return Decision{
			Phase:   pgelasticv1alpha1.TenantMigrationPhaseRolledBack,
			Serving: ServingSource, Cleanup: true, DiscardTarget: true, ReleaseQuiesce: true,
			Reason:  ReasonRolledBack,
			Message: "routing was flipped back to the source inside rollbackWindow",
		}
	case observation.RollbackWindowClosed && observation.SourceDropped:
		return Decision{
			Phase:   pgelasticv1alpha1.TenantMigrationPhaseCompleted,
			Serving: ServingTarget, Settled: true,
			Reason:  ReasonSourceDropped,
			Message: "rollbackWindow closed, the source database was dropped, and this migration is final",
		}
	case observation.RollbackWindowClosed:
		return Decision{
			Phase:   pgelasticv1alpha1.TenantMigrationPhaseCompleted,
			Serving: ServingTarget, DropSource: true,
			Reason:  ReasonSourceDropped,
			Message: "rollbackWindow closed and the source database was dropped",
		}
	default:
		return Decision{
			Phase:   pgelasticv1alpha1.TenantMigrationPhaseCompleted,
			Serving: ServingTarget,
			Reason:  ReasonSourceRetained,
			Message: "the tenant serves from the target; the source is kept connection-refusing until rollbackDeadline",
		}
	}
}

// abort is the one constructor for every departure from the happy path, so that the
// cleanup ladder and the quiesce release can never be forgotten on one path and remembered
// on another.
func abort(phase Phase, reason, message string, from Phase, observation Observation) Decision {
	return Decision{
		Phase:          phase,
		Serving:        ServingSource,
		Cleanup:        true,
		DiscardTarget:  true,
		ReleaseQuiesce: Quiesced(from, observation.Strategy),
		Reason:         reason,
		Message:        message,
	}
}

func ready(phase Phase, observation Observation) bool {
	switch phase {
	case pgelasticv1alpha1.TenantMigrationPhasePreflight:
		return observation.PreflightPassed
	case pgelasticv1alpha1.TenantMigrationPhaseProvisioning:
		return observation.Provisioned
	case pgelasticv1alpha1.TenantMigrationPhasePreWarm:
		return observation.PreWarmed
	case pgelasticv1alpha1.TenantMigrationPhaseCopying:
		return observation.CopyComplete
	case pgelasticv1alpha1.TenantMigrationPhaseCatchup:
		return observation.CaughtUp
	case pgelasticv1alpha1.TenantMigrationPhaseQuiescing:
		return observation.Drained
	case pgelasticv1alpha1.TenantMigrationPhaseCutover:
		return observation.CutoverComplete
	default:
		return false
	}
}

// nextPhase advances one place in the strategy's order. A phase that is not in the order -
// Catchup under the offline strategy, say - cannot be advanced from and stays put, which
// makes a mis-set phase visible rather than silently skipping the rest of the machine.
func nextPhase(current Phase, strategy Strategy) Phase {
	order := PhaseOrder(strategy)
	index := slices.Index(order, current)
	if index < 0 || index+1 >= len(order) {
		return current
	}
	return order[index+1]
}

func reasonFor(phase Phase) string {
	switch phase {
	case pgelasticv1alpha1.TenantMigrationPhaseAborted:
		return ReasonAbortRequested
	case pgelasticv1alpha1.TenantMigrationPhaseRolledBack:
		return ReasonRolledBack
	default:
		return ReasonFaulted
	}
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
