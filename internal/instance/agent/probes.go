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
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
)

// ProbeResult is one probe's answer plus the reason behind it. The reason is carried into
// the HTTP body because "why is this pod not ready" is a question asked under time
// pressure, and a bare status code answers it badly.
type ProbeResult struct {
	OK     bool
	Reason string
}

func probeOK(reason string) ProbeResult   { return ProbeResult{OK: true, Reason: reason} }
func probeFail(reason string) ProbeResult { return ProbeResult{OK: false, Reason: reason} }

// ProbeState is what the three probes read. It is written by the supervisor loop, never by
// the probes themselves.
type ProbeState struct {
	// CanCheck is true only between a postmaster spawn and its exit. Readiness is gated on
	// it because pg_isready against a socket nobody is listening on is indistinguishable
	// from pg_isready during a bootstrap that has not finished, and treating those two as
	// the same state is how a pod gets marked ready before it has a postmaster.
	CanCheck bool
	// Rejoin is the path a rejoin is currently taking, empty when none is running. While it
	// is set the startup probe is suspended entirely: a pg_rewind can run for far longer
	// than any startup deadline, and a re-clone for longer still, so letting the kubelet
	// restart the pod through one leaves a data directory that is neither the old one nor
	// the new one.
	Rejoin RejoinMethod
	// Role is the member's replication role.
	Role Role
	// ReplayLag is how far behind the primary this member is.
	ReplayLag time.Duration
	// LastPing is the most recent pg_isready result.
	LastPing pgtool.PingResult
	// Observation is the last reading the supervisor took from the postmaster, and
	// Observed says whether there has ever been one. The status endpoint serves it rather
	// than opening its own connection, so that a probe cannot consume a backend slot on an
	// instance that is already short of them.
	Observation MemberObservation
	Observed    bool
	// WALVolumeFull is measured from the filesystem rather than from PostgreSQL, so it is
	// still answerable when the postmaster is not.
	WALVolumeFull bool
	// DataUsedBytes and WALUsedBytes are the two volumes' usage from the same measurement.
	// Zero means the agent could not stat the path, which is distinguishable from an empty
	// volume only because an initialised one is never actually zero.
	DataUsedBytes int64
	WALUsedBytes  int64
}

// StartupProbe answers whether the postmaster has got far enough to be waited on.
//
// PQPING_REJECT counts as success. A server that is running but still rejecting
// connections - starting up, or in recovery before hot standby opens - is exactly the
// state the startup probe exists to wait through, and calling it a failure restarts a pod
// that was about to become healthy.
func StartupProbe(state ProbeState) ProbeResult {
	if state.Rejoin != "" {
		return probeOK("skipped while the member is " + string(state.Rejoin))
	}
	switch state.LastPing {
	case pgtool.PingOK:
		return probeOK("accepting connections")
	case pgtool.PingReject:
		return probeOK("running, still rejecting connections")
	case pgtool.PingNoResponse:
		return probeFail("no response from the postmaster")
	case pgtool.PingNoAttempt:
		return probeFail("no connection attempt was made")
	default:
		return probeFail("unrecognised pg_isready result")
	}
}

// ReadinessConfig bounds how far behind a replica may be and still serve reads.
type ReadinessConfig struct {
	// MaxReplayLag is the ceiling on a replica's replay lag. Zero disables the check.
	MaxReplayLag time.Duration
}

// ReadinessProbe answers whether this member should be behind a Service.
func ReadinessProbe(state ProbeState, config ReadinessConfig) ProbeResult {
	if !state.CanCheck {
		return probeFail("no postmaster is running")
	}
	if state.LastPing != pgtool.PingOK {
		return probeFail("the postmaster is not accepting connections")
	}
	if state.Role == RoleReplica && config.MaxReplayLag > 0 && state.ReplayLag > config.MaxReplayLag {
		return probeFail("replay lag exceeds the configured ceiling")
	}
	return probeOK("serving")
}

// IsolationView is what the liveness probe is allowed to look at: the control plane and
// the member's peers, and nothing else.
type IsolationView struct {
	// APIServerReachable reports whether the Kubernetes API server answered.
	APIServerReachable bool
	// PeersReachable counts peers that answered on their failsafe endpoint over a direct
	// connection, bypassing every Service and every DNS record.
	PeersReachable int
	// PeersTotal is how many peers were asked.
	PeersTotal int
}

// LivenessProbe is a network-isolation detector that never touches PostgreSQL.
//
// It returns OK on a replica unconditionally: a replica that is alone in the dark is
// harmless, and restarting it removes a failover candidate for no gain. It fails a primary
// only when the node can reach neither the API server nor any peer. Conflating "the
// operator is having a bad day" with "I am alone in the dark" is what turns routine
// control-plane maintenance into simultaneous self-immolation across the whole fleet - so
// an unreachable API server on its own is explicitly not enough.
func LivenessProbe(role Role, view IsolationView) ProbeResult {
	if role != RolePrimary {
		return probeOK("liveness does not fence a replica")
	}
	if view.APIServerReachable {
		return probeOK("the API server is reachable")
	}
	if view.PeersReachable > 0 {
		return probeOK("a peer is reachable over its failsafe endpoint")
	}
	if view.PeersTotal == 0 {
		return probeOK("no peers to ask, so isolation cannot be established")
	}
	return probeFail("isolated: neither the API server nor any peer is reachable")
}

// PeerChecker probes one peer's failsafe endpoint.
type PeerChecker interface {
	Reachable(ctx context.Context, endpoint string) bool
}

// SurveyPeers builds an IsolationView by asking every peer directly. Every check runs
// against a direct endpoint rather than a Service name, because a Service is exactly the
// thing that stops working during the partition this probe exists to detect.
func SurveyPeers(ctx context.Context, checker PeerChecker, endpoints []string, apiServer bool) IsolationView {
	view := IsolationView{APIServerReachable: apiServer, PeersTotal: len(endpoints)}
	for _, endpoint := range endpoints {
		if checker.Reachable(ctx, endpoint) {
			view.PeersReachable++
		}
	}
	return view
}
