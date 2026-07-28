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

package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Status server paths. They live beside the AgentConfig for the same reason: the operator
// that calls them and the agent that serves them must not be able to drift.
const (
	// StatusPathStartup, StatusPathReadiness and StatusPathLiveness are the three kubelet
	// probes, which mean three different things and are deliberately not one endpoint.
	StatusPathStartup   = "/startup"
	StatusPathReadiness = "/readiness"
	StatusPathLiveness  = "/liveness"
	// StatusPathPeer is the failsafe endpoint peers check each other on. It answers
	// unconditionally: its whole purpose is to prove this node's network still works.
	StatusPathPeer = "/peer"
	// StatusPathStatus is the member's full report, which the operator polls to build the
	// failover decision.
	StatusPathStatus = "/status"
)

// MemberReport is the body of the status endpoint: one member's own view of itself, as read
// out of its own postmaster.
//
// The operator polls it rather than reading only the member's status entry on the CR,
// because a dead agent stops updating the CR and leaves its last cheerful report behind
// forever. A poll that times out is evidence; a stale record is not. It is also the only
// vantage point from which pg_is_in_recovery() can be read without a network hop that the
// failure being diagnosed may already have removed.
type MemberReport struct {
	// Member is the Pod name, which is also the application_name
	// synchronous_standby_names refers to it by.
	Member string `json:"member"`
	// Role is primary, replica or unknown.
	Role string `json:"role"`
	// InRecovery is pg_is_in_recovery(). Two members reporting false at once is a dedicated
	// alarm, never a tiebreak.
	InRecovery bool `json:"inRecovery"`
	// Healthy reports that the postmaster answered this member's own query. A report that
	// arrives with Healthy false is a member that is up enough to answer HTTP and not up
	// enough to answer SQL, which is a different state from silence.
	Healthy bool `json:"healthy"`
	// Timeline is pg_control_checkpoint().timeline_id, the first term in candidate
	// selection.
	Timeline int32 `json:"timeline"`
	// LSN is the headline position; ReceivedLSN and ReplayLSN are the two candidate
	// selection orders on, received first.
	LSN         string `json:"lsn"`
	ReceivedLSN string `json:"receivedLSN"`
	ReplayLSN   string `json:"replayLSN"`
	// ReplayLagSeconds is how far behind the primary this member has replayed.
	ReplayLagSeconds float64 `json:"replayLagSeconds"`
	// WALReceiverActive is checked at two instants with two meanings: a candidate must have
	// had a receiver at detection time, and every member must have its receiver down before
	// promotion may proceed.
	WALReceiverActive bool `json:"walReceiverActive"`
	// WALVolumeFull refuses this member as a candidate outright.
	WALVolumeFull bool `json:"walVolumeFull"`
	// SynchronousStandbyNames is the clause this member's postmaster actually loaded, and
	// NumSync and VotingMembers are parsed out of it. On a standby the clause is inert, but
	// it is still reported: a standby whose loaded clause disagrees with the primary's is a
	// half-applied reload worth seeing.
	SynchronousStandbyNames string   `json:"synchronousStandbyNames"`
	NumSync                 int32    `json:"numSync"`
	VotingMembers           []string `json:"votingMembers"`
	// StreamingMembers is what pg_stat_replication reports as streaming quorum members.
	// Only a primary has any.
	StreamingMembers []string `json:"streamingMembers"`
	// PrimaryEpoch is the fence token bound into this member's postmaster, read back with
	// current_setting() so it cannot drift from the running server.
	PrimaryEpoch int64 `json:"primaryEpoch"`
}

// memberReportTimeout bounds one status poll.
//
// It is short on purpose. The operator polls every member on every reconcile, and a member
// that needs longer than this to describe itself is a member the failover decision should
// treat as unreachable: the whole reason the report is fetched rather than read off the CR
// is that a timeout is evidence and a stale record is not.
const memberReportTimeout = 2 * time.Second

// FetchMemberReport asks one member to describe itself, over its own status endpoint.
//
// The endpoint is addressed directly - a Pod IP, or a per-pod DNS record under the headless
// Service - never through a load-balanced Service, because a load-balanced Service is
// exactly what stops resolving correctly during the partition being diagnosed.
func FetchMemberReport(ctx context.Context, endpoint string) (MemberReport, error) {
	probeCtx, cancel := context.WithTimeout(ctx, memberReportTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet,
		"http://"+endpoint+StatusPathStatus, nil)
	if err != nil {
		return MemberReport{}, err
	}
	response, err := (&http.Client{Timeout: memberReportTimeout}).Do(request)
	if err != nil {
		return MemberReport{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return MemberReport{}, fmt.Errorf("%s answered %s", endpoint, response.Status)
	}

	var report MemberReport
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&report); err != nil {
		return MemberReport{}, err
	}
	return report, nil
}
