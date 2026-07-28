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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
)

const postmasterPID = 7

func TestSelectReapableTakesOnlyOrphanedPostgresProcesses(t *testing.T) {
	processes := []Process{
		{PID: postmasterPID, PPid: 1, Executable: postmasterExecutable},
		{PID: 8, PPid: postmasterPID, Executable: postmasterExecutable},
		{PID: 9, PPid: 1, Executable: postmasterExecutable},
		{PID: 10, PPid: 1, Executable: "pg_basebackup"},
		{PID: 11, PPid: 1, Executable: "postgres-exporter"},
		{PID: 12, PPid: 9, Executable: postmasterExecutable},
		{PID: 13, PPid: 1, Executable: postmasterExecutable},
	}
	reapable := SelectReapable(processes, postmasterPID)
	if !slices.Equal(reapable, []int{9, 13}) {
		t.Errorf("reapable = %v, want the two orphaned postgres processes 9 and 13", reapable)
	}
}

func TestSelectReapableNeverTakesThePostmaster(t *testing.T) {
	processes := []Process{{PID: postmasterPID, PPid: 1, Executable: postmasterExecutable}}
	if reapable := SelectReapable(processes, postmasterPID); len(reapable) != 0 {
		t.Errorf("reapable = %v, want nothing: waiting on the postmaster steals its exit status", reapable)
	}
}

func TestSelectReapableReapsNothingWithoutAKnownPostmaster(t *testing.T) {
	processes := []Process{{PID: 9, PPid: 1, Executable: postmasterExecutable}}
	if reapable := SelectReapable(processes, 0); len(reapable) != 0 {
		t.Errorf("reapable = %v, want nothing while the postmaster pid is unknown", reapable)
	}
}

func TestSelectReapableRequiresAnExactExecutableMatch(t *testing.T) {
	processes := []Process{
		{PID: 9, PPid: 1, Executable: "postgresql"},
		{PID: 10, PPid: 1, Executable: "postgres_fdw"},
		{PID: 11, PPid: 1, Executable: "pg_rewind"},
	}
	if reapable := SelectReapable(processes, postmasterPID); len(reapable) != 0 {
		t.Errorf("reapable = %v, want nothing: only an exact \"postgres\" is reapable", reapable)
	}
}

func TestProcFSReadsParentAndExecutable(t *testing.T) {
	root := t.TempDir()
	write := func(pid, ppid int, name string) {
		procDir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(procDir, 0o755); err != nil {
			t.Fatal(err)
		}
		contents := "Name:\t" + name + "\nState:\tS (sleeping)\nPPid:\t" + strconv.Itoa(ppid) + "\n"
		if err := os.WriteFile(filepath.Join(procDir, "status"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(7, 1, "postgres")
	write(9, 1, "postgres")
	write(11, 7, "postgres")
	if err := os.MkdirAll(filepath.Join(root, "self"), 0o755); err != nil {
		t.Fatal(err)
	}

	processes, err := ProcFS{Root: root}.Processes()
	if err != nil {
		t.Fatalf("Processes() = %v", err)
	}
	if len(processes) != 3 {
		t.Fatalf("processes = %v, want three", processes)
	}
	if reapable := SelectReapable(processes, 7); !slices.Equal(reapable, []int{9}) {
		t.Errorf("reapable = %v, want only the orphan 9", reapable)
	}
}

func TestPostmasterPIDReadsTheFirstLine(t *testing.T) {
	dir := t.TempDir()
	contents := "4321\n/var/lib/postgresql/data/pgdata\n1769000000\n5432\n"
	if err := os.WriteFile(filepath.Join(dir, "postmaster.pid"), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if pid := PostmasterPID(dir); pid != 4321 {
		t.Errorf("PostmasterPID = %d, want 4321", pid)
	}
	if pid := PostmasterPID(t.TempDir()); pid != 0 {
		t.Errorf("PostmasterPID of a directory with no pid file = %d, want 0", pid)
	}
}

func TestTranslateStopKubeletIsSmartThenFast(t *testing.T) {
	timeouts := StopTimeouts{SmartShutdown: 20 * time.Second, MaxStop: 60 * time.Second}
	plan := TranslateStop(CauseKubelet, RolePrimary, timeouts)
	if plan.Mode != pgtool.StopSmart {
		t.Errorf("mode = %q, want smart", plan.Mode)
	}
	if plan.EscalateTo != pgtool.StopFast {
		t.Errorf("escalation = %q, want fast", plan.EscalateTo)
	}
	if plan.Timeout != 20*time.Second || plan.EscalateTimeout != 40*time.Second {
		t.Errorf("timeouts = %v/%v, want the escalation to fit inside the grace period",
			plan.Timeout, plan.EscalateTimeout)
	}
	if !plan.Checkpoint {
		t.Error("a primary must checkpoint first or a later pg_rewind finds the wrong divergence point")
	}
}

func TestTranslateStopSwitchoverIsFastThenImmediate(t *testing.T) {
	timeouts := StopTimeouts{SmartShutdown: 20 * time.Second, MaxStop: 60 * time.Second,
		MaxSwitchoverDelay: 30 * time.Second}
	for _, cause := range []StopCause{CauseSwitchover, CauseFence} {
		plan := TranslateStop(cause, RolePrimary, timeouts)
		if plan.Mode != pgtool.StopFast {
			t.Errorf("%s mode = %q, want fast", cause, plan.Mode)
		}
		if plan.EscalateTo != pgtool.StopImmediate {
			t.Errorf("%s escalation = %q, want immediate", cause, plan.EscalateTo)
		}
		if plan.Timeout != 30*time.Second {
			t.Errorf("%s timeout = %v, want the switchover delay", cause, plan.Timeout)
		}
	}
}

func TestTranslateStopSkipsTheCheckpointOnAReplica(t *testing.T) {
	plan := TranslateStop(CauseKubelet, RoleReplica, DefaultStopTimeouts())
	if plan.Checkpoint {
		t.Error("a replica has nothing to checkpoint")
	}
}

func TestStartupProbeTreatsRejectAsSuccess(t *testing.T) {
	if result := StartupProbe(ProbeState{LastPing: pgtool.PingReject}); !result.OK {
		t.Error("PQPING_REJECT is the state the startup probe exists to wait through")
	}
	if result := StartupProbe(ProbeState{LastPing: pgtool.PingOK}); !result.OK {
		t.Error("PQPING_OK must succeed")
	}
	if result := StartupProbe(ProbeState{LastPing: pgtool.PingNoResponse}); result.OK {
		t.Error("PQPING_NO_RESPONSE must fail")
	}
}

func TestStartupProbeIsSkippedDuringARewind(t *testing.T) {
	state := ProbeState{RewindInProgress: true, LastPing: pgtool.PingNoResponse}
	if result := StartupProbe(state); !result.OK {
		t.Error("the startup probe must not restart a pod in the middle of a pg_rewind")
	}
}

func TestReadinessProbeIsGatedOnTheCanCheckFlag(t *testing.T) {
	if result := ReadinessProbe(ProbeState{LastPing: pgtool.PingOK}, ReadinessConfig{}); result.OK {
		t.Error("readiness must fail while no postmaster is running, whatever a stale ping said")
	}
	state := ProbeState{CanCheck: true, LastPing: pgtool.PingOK, Role: RolePrimary}
	if result := ReadinessProbe(state, ReadinessConfig{}); !result.OK {
		t.Error("a primary accepting connections must be ready")
	}
}

func TestReadinessProbeFailsALaggingReplica(t *testing.T) {
	state := ProbeState{CanCheck: true, LastPing: pgtool.PingOK, Role: RoleReplica,
		ReplayLag: 30 * time.Second}
	config := ReadinessConfig{MaxReplayLag: 10 * time.Second}
	if result := ReadinessProbe(state, config); result.OK {
		t.Error("a replica past the lag ceiling must leave the read Service")
	}
	state.ReplayLag = time.Second
	if result := ReadinessProbe(state, config); !result.OK {
		t.Error("a replica inside the lag ceiling must stay in the read Service")
	}
}

func TestLivenessProbeNeverFencesAReplica(t *testing.T) {
	view := IsolationView{APIServerReachable: false, PeersTotal: 2}
	if result := LivenessProbe(RoleReplica, view); !result.OK {
		t.Error("an isolated replica is harmless and restarting it removes a failover candidate")
	}
}

func TestLivenessProbeSurvivesAnUnreachableAPIServer(t *testing.T) {
	view := IsolationView{APIServerReachable: false, PeersReachable: 1, PeersTotal: 2}
	if result := LivenessProbe(RolePrimary, view); !result.OK {
		t.Error("control-plane maintenance must not fence a primary whose peers can still see it")
	}
}

func TestLivenessProbeFencesAnIsolatedPrimary(t *testing.T) {
	view := IsolationView{APIServerReachable: false, PeersReachable: 0, PeersTotal: 2}
	if result := LivenessProbe(RolePrimary, view); result.OK {
		t.Error("a primary that can reach neither the API server nor any peer must fence itself")
	}
}

func TestLivenessProbeNeedsPeersToConcludeIsolation(t *testing.T) {
	view := IsolationView{APIServerReachable: false, PeersTotal: 0}
	if result := LivenessProbe(RolePrimary, view); !result.OK {
		t.Error("with nobody to ask, isolation is unproven and fencing would be a guess")
	}
}

type stubPeerChecker map[string]bool

func (s stubPeerChecker) Reachable(_ context.Context, endpoint string) bool { return s[endpoint] }

func TestSurveyPeersCountsDirectEndpoints(t *testing.T) {
	checker := stubPeerChecker{"10.0.0.2:8008": true, "10.0.0.3:8008": false}
	view := SurveyPeers(context.Background(), checker,
		[]string{"10.0.0.2:8008", "10.0.0.3:8008"}, false)
	if view.PeersTotal != 2 || view.PeersReachable != 1 {
		t.Errorf("view = %+v, want one of two peers reachable", view)
	}
}
