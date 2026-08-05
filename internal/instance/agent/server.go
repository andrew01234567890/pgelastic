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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// peerProbeTimeout bounds one direct peer check. It is short because the liveness probe it
// feeds has its own deadline, and a slow answer is a reachable peer.
const peerProbeTimeout = 2 * time.Second

// StatusServer carries the three probes and the failsafe endpoint peers check each other
// on. It is a separate port from 5432 for one reason: the liveness probe has to be
// answerable precisely when PostgreSQL is not.
type StatusServer struct {
	Supervisor *Supervisor
	Options    Options
	Readiness  ReadinessConfig
}

// Handler builds the mux.
func (s *StatusServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+provision.StatusPathStartup, func(writer http.ResponseWriter, request *http.Request) {
		respond(writer, StartupProbe(s.Supervisor.ProbeState()))
	})
	mux.HandleFunc("GET "+provision.StatusPathReadiness, func(writer http.ResponseWriter, request *http.Request) {
		respond(writer, ReadinessProbe(s.Supervisor.ProbeState(), s.Readiness))
	})
	mux.HandleFunc("GET "+provision.StatusPathLiveness, func(writer http.ResponseWriter, request *http.Request) {
		state := s.Supervisor.ProbeState()
		respond(writer, LivenessProbe(state.Role, s.isolationView(request.Context(), state.Role)))
	})
	// The failsafe endpoint answers unconditionally. Its whole purpose is to prove that
	// this node's network still works, so making it conditional on anything - including
	// PostgreSQL - would defeat it.
	mux.HandleFunc("GET "+provision.StatusPathPeer, func(writer http.ResponseWriter, request *http.Request) {
		respond(writer, probeOK("reachable"))
	})
	mux.HandleFunc("GET "+provision.StatusPathStatus, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(MemberReportOf(s.Options.Member, s.Supervisor.ProbeState()))
	})
	return mux
}

// MemberReportOf renders the operator's view of this member from the supervisor's last
// reading.
//
// Healthy is the probe state's CanCheck and Observed taken together, and not the HTTP
// status: an agent that answers while its postmaster does not is a distinct state from an
// agent that does not answer at all, and collapsing the two would make a member that is
// merely starting up look identical to one that has gone.
func MemberReportOf(member string, state ProbeState) provision.MemberReport {
	observation := state.Observation
	return provision.MemberReport{
		Member:                  member,
		Role:                    string(state.Role),
		InRecovery:              state.Role == RoleReplica,
		Healthy:                 state.CanCheck && state.Observed && state.LastPing == pgtool.PingOK,
		Timeline:                observation.Timeline,
		LSN:                     observation.LSN,
		ReceivedLSN:             observation.ReceivedLSN,
		ReplayLSN:               observation.ReplayLSN,
		ReplayLagSeconds:        state.ReplayLag.Seconds(),
		WALReceiverActive:       observation.WALReceiverActive,
		WALVolumeFull:           state.WALVolumeFull,
		DataUsedBytes:           state.DataUsedBytes,
		WALUsedBytes:            state.WALUsedBytes,
		ClientBackends:          state.ClientBackends,
		Rejoining:               string(state.Rejoin),
		SynchronousStandbyNames: observation.SyncStandbyNames,
		NumSync:                 observation.NumSync,
		VotingMembers:           observation.VotingMembers,
		StreamingMembers:        observation.SyncStandbys,
		PrimaryEpoch:            observation.PrimaryEpoch,
		Archive:                 archiveReport(observation.Archive),
		Databases:               state.Databases,
	}
}

// archiveReport carries the archive observation onto the wire, and reports nothing at all
// when this member has no repository.
//
// The distinction matters to the operator: an instance with no repository configured is not
// an instance whose archiving is broken, and a status that could not tell the two apart
// would either alarm on every instance that has not opted in or stay silent on every
// instance that has.
func archiveReport(observation ArchiveObservation) *provision.ArchiveReport {
	if observation.State == "" {
		return nil
	}
	report := &provision.ArchiveReport{
		State:              string(observation.State),
		LastArchivedWAL:    observation.LastArchivedWAL,
		FailedCount:        observation.FailedCount,
		LastFailureMessage: observation.LastFailureMessage,
		ReadyBacklog:       observation.ReadyBacklog,
	}
	if observation.LastArchivedAt != nil {
		report.LastArchivedAt = &metav1.Time{Time: *observation.LastArchivedAt}
	}
	if observation.LastFailureAt != nil {
		report.LastFailureAt = &metav1.Time{Time: *observation.LastFailureAt}
	}
	return report
}

// Serve runs the status server until the context is cancelled.
func (s *StatusServer) Serve(ctx context.Context) error {
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(int(s.Options.StatusPort)),
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func respond(writer http.ResponseWriter, result ProbeResult) {
	status := http.StatusOK
	if !result.OK {
		status = http.StatusServiceUnavailable
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(result.Reason + "\n"))
}

// isolationView asks the two questions the liveness probe is allowed to ask, and no
// others. It never touches PostgreSQL.
func (s *StatusServer) isolationView(ctx context.Context, role Role) IsolationView {
	if role != RolePrimary {
		return IsolationView{}
	}
	endpoints := s.peerEndpoints()
	return SurveyPeers(ctx, httpPeerChecker{}, endpoints, s.apiServerReachable(ctx))
}

// peerEndpoints addresses every other member directly, through the headless Service's
// per-pod DNS records rather than through a load-balanced Service. A load-balanced Service
// is exactly the thing that stops resolving correctly during the partition this is trying
// to detect.
func (s *StatusServer) peerEndpoints() []string {
	var endpoints []string
	for serial := int32(1); serial <= s.Options.Config.Replicas; serial++ {
		member := provision.MemberName(s.Options.Instance, serial)
		if member == s.Options.Member {
			continue
		}
		endpoints = append(endpoints, fmt.Sprintf("%s.%s.%s.svc:%d",
			member, s.Options.PeerService, s.Options.Namespace, s.Options.StatusPort))
	}
	return endpoints
}

func (s *StatusServer) apiServerReachable(ctx context.Context) bool {
	if s.Options.Client == nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
	defer cancel()
	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: s.Options.Namespace, Name: s.Options.Instance}
	return s.Options.Client.Get(probeCtx, key, instance) == nil
}

type httpPeerChecker struct{}

func (httpPeerChecker) Reachable(ctx context.Context, endpoint string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+endpoint+provision.StatusPathPeer, nil)
	if err != nil {
		return false
	}
	response, err := (&http.Client{Timeout: peerProbeTimeout}).Do(request)
	if err != nil {
		return false
	}
	defer func() { _ = response.Body.Close() }()
	return response.StatusCode == http.StatusOK
}

// DialPeer is used by the bootstrap path to wait for the primary's postmaster to accept
// TCP before pg_basebackup is attempted, so a clone failure means something went wrong
// rather than that the primary had not finished starting.
func DialPeer(ctx context.Context, host string, port int32) bool {
	dialer := net.Dialer{Timeout: peerProbeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
