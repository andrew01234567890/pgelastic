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
)

// StopCause is why a shutdown was asked for. The two causes translate to different
// PostgreSQL shutdown modes because they are answering different questions: a kubelet
// SIGTERM is "finish your work", a switchover or a fence is "stop being the primary now".
type StopCause string

const (
	// CauseKubelet is a SIGTERM from the kubelet: a drain, an eviction, a rolling update.
	CauseKubelet StopCause = "Kubelet"
	// CauseSwitchover is a planned role change.
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

// TranslateStop turns a shutdown cause into a PostgreSQL shutdown plan.
//
// A kubelet SIGTERM gets a smart shutdown first, because the tenant connections on this
// pod are the product and letting them finish is worth the wait; it escalates to fast so
// the escalation still completes inside the grace period rather than being SIGKILLed
// mid-checkpoint. A switchover or a fence never waits for clients at all: the whole point
// is that this node must stop being the primary, and it escalates to immediate, accepting
// crash recovery on the next start as the cheaper outcome.
func TranslateStop(cause StopCause, role Role, timeouts StopTimeouts) StopPlan {
	checkpoint := role == RolePrimary
	if cause == CauseKubelet {
		return StopPlan{
			Mode:            pgtool.StopSmart,
			Timeout:         timeouts.SmartShutdown,
			EscalateTo:      pgtool.StopFast,
			EscalateTimeout: max(timeouts.MaxStop-timeouts.SmartShutdown, 0),
			Checkpoint:      checkpoint,
		}
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
