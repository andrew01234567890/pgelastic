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
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// StopCause is why a shutdown was asked for. The three causes translate to different
// PostgreSQL shutdown modes because they are answering different questions: a kubelet
// SIGTERM is "finish your work", a switchover is "stop being the primary, cleanly, because
// we chose this moment", and a fence is "stop being the primary now, whatever it costs".
type StopCause string

const (
	// CauseKubelet is a SIGTERM from the kubelet: a drain, an eviction, a rolling update.
	CauseKubelet StopCause = "Kubelet"
	// CauseSwitchover is a stop this control plane asked for at a moment it chose: a
	// planned role change, an in-place restart for a parameter that needs one, or a
	// diverged standby about to be rewound. Every one of them is followed by this member
	// starting again from the data directory it is stopping with.
	CauseSwitchover StopCause = "Switchover"
	// CauseFence is this node taking itself out of service, because it lost the promotion
	// Lease or found itself isolated.
	CauseFence StopCause = "Fence"
)

// StopPlan is the translated shutdown: a first mode, a deadline, and the harsher mode to
// escalate to when that deadline passes.
type StopPlan struct {
	// Mode is the shutdown mode attempted first.
	Mode pgtool.StopMode
	// Timeout bounds the first attempt.
	Timeout time.Duration
	// EscalateTo is the mode used once Timeout has passed.
	EscalateTo pgtool.StopMode
	// EscalateTimeout bounds the escalated attempt.
	EscalateTimeout time.Duration
	// Checkpoint requires an explicit CHECKPOINT before the shutdown begins. On a primary
	// it is not optional: without it, pg_rewind on this node after somebody else is
	// promoted computes the wrong divergence point.
	Checkpoint bool
}

// StopTimeouts are the deadlines admission has already validated against the Pod's
// terminationGracePeriodSeconds.
type StopTimeouts struct {
	// SmartShutdown is how long clients are given to disconnect on their own.
	SmartShutdown time.Duration
	// MaxStop is the whole budget, equal to terminationGracePeriodSeconds. Admission
	// enforces terminationGracePeriodSeconds == MaxStop > SmartShutdown, so the escalated
	// attempt always has time to run before the kubelet reaches for SIGKILL.
	MaxStop time.Duration
	// MaxSwitchoverDelay bounds a role change end to end.
	MaxSwitchoverDelay time.Duration
}

// DefaultStopTimeouts are the values used when the Pod carries none.
func DefaultStopTimeouts() StopTimeouts {
	return StopTimeouts{
		SmartShutdown:      20 * time.Second,
		MaxStop:            60 * time.Second,
		MaxSwitchoverDelay: 30 * time.Second,
	}
}

// StopTimeoutsFrom resolves the deadlines out of the operator's configuration document.
//
// spec.highAvailability.switchoverTimeout says it bounds a planned role change *end to
// end*, so it is halved rather than taken as the first attempt's deadline: the plan is two
// clean attempts, and two attempts each given the whole figure would take twice what the
// field promises. Halving is also what keeps the retry from being an empty budget, which is
// what an escalation to immediate was doing the work of before. It cannot exceed the
// termination grace period either way, because the kubelet reaches for SIGKILL at that point
// whatever this says.
func StopTimeoutsFrom(config provision.AgentConfig) StopTimeouts {
	timeouts := DefaultStopTimeouts()
	if configured := config.SwitchoverTimeout.Duration; configured > 0 {
		timeouts.MaxSwitchoverDelay = min(configured, timeouts.MaxStop) / 2
	}
	return timeouts
}

// TranslateStop turns a shutdown cause into a PostgreSQL shutdown plan.
//
// A kubelet SIGTERM gets a smart shutdown first, because the tenant connections on this
// pod are the product and letting them finish is worth the wait; it escalates to fast so
// the escalation still completes inside the grace period rather than being SIGKILLed
// mid-checkpoint.
//
// A fence never waits for clients: the whole point is that this node must stop being the
// primary, and it escalates to immediate, accepting crash recovery on the next start as
// the cheaper outcome. Its writes are about to be discarded, so there is nothing a
// slower shutdown would preserve.
//
// A switchover is the one that must not escalate that far, and the reason is mechanical
// rather than aesthetic. Every switchover is followed by this member starting again from
// the data directory it is stopping with - as a standby that rewinds, or in place after a
// restart - and pg_rewind requires its target to have been shut down cleanly. An
// immediate stop leaves a data directory that has to be started and stopped again before
// it can be rewound at all, which turns the cheap path back into the expensive one at
// exactly the moment the design chose the cheap one. So a switchover checkpoints, stops
// fast, and if that does not finish it tries fast again with the rest of the budget: a
// stop that cannot be made clean is reported and retried, never converted into crash
// recovery. Waiting for clients is not the alternative either - a pooled backend never
// disconnects on its own, so a smart stop here would sit until its deadline and then
// escalate anyway. What holds the clients across the gap is the proxy's quiesce, not
// PostgreSQL's shutdown mode.
func TranslateStop(cause StopCause, role Role, timeouts StopTimeouts) StopPlan {
	checkpoint := role == RolePrimary
	switch cause {
	case CauseKubelet:
		return StopPlan{
			Mode:            pgtool.StopSmart,
			Timeout:         timeouts.SmartShutdown,
			EscalateTo:      pgtool.StopFast,
			EscalateTimeout: max(timeouts.MaxStop-timeouts.SmartShutdown, 0),
			Checkpoint:      checkpoint,
		}
	case CauseSwitchover:
		// Both attempts are bounded by the same figure, so the pair of them is the end-to-end
		// deadline the API field names rather than twice it.
		return StopPlan{
			Mode:            pgtool.StopFast,
			Timeout:         timeouts.MaxSwitchoverDelay,
			EscalateTo:      pgtool.StopFast,
			EscalateTimeout: min(timeouts.MaxSwitchoverDelay, max(timeouts.MaxStop-timeouts.MaxSwitchoverDelay, 0)),
			Checkpoint:      checkpoint,
		}
	case CauseFence:
	}
	return StopPlan{
		Mode:            pgtool.StopFast,
		Timeout:         timeouts.MaxSwitchoverDelay,
		EscalateTo:      pgtool.StopImmediate,
		EscalateTimeout: max(timeouts.MaxStop-timeouts.MaxSwitchoverDelay, 0),
		Checkpoint:      checkpoint,
	}
}

// Role is the member's replication role as this agent understands it.
type Role string

const (
	// RolePrimary is a member out of recovery.
	RolePrimary Role = "primary"
	// RoleReplica is a member in recovery.
	RoleReplica Role = "replica"
	// RoleUnknown is a member whose postmaster has not answered yet.
	RoleUnknown Role = "unknown"
)
