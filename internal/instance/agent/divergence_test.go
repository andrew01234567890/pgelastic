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
	"os"
	"path/filepath"
	"testing"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
)

// walDirForkedAtTimelineThree is a WAL directory holding the history file a standby fetches
// from a primary that has promoted twice: timeline 3 left timeline 2 at 0/70000A0.
func walDirForkedAtTimelineThree(t *testing.T) string {
	t.Helper()
	walDir := t.TempDir()
	contents := "1\t0/3000060\tno recovery target specified\n2\t0/70000A0\tno recovery target specified\n"
	if err := os.WriteFile(filepath.Join(walDir, "00000003.history"), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return walDir
}

func TestDetectDivergenceReadsTheForkPointOffDisk(t *testing.T) {
	walDir := walDirForkedAtTimelineThree(t)

	stranded := ha.TimelinePosition{Timeline: 2, LSN: ha.MustParseLSN("0/80000A0")}
	divergence, err := DetectDivergence(walDir, stranded, 3)
	if err != nil {
		t.Fatalf("DetectDivergence = %v", err)
	}
	if !divergence.Diverged || divergence.Reason != ha.DivergencePastForkPoint {
		t.Fatalf("divergence = %+v", divergence)
	}

	behind := ha.TimelinePosition{Timeline: 2, LSN: ha.MustParseLSN("0/6000000")}
	divergence, err = DetectDivergence(walDir, behind, 3)
	if err != nil {
		t.Fatalf("DetectDivergence = %v", err)
	}
	if divergence.Diverged {
		t.Fatalf("a standby short of the fork point can still stream: %s", divergence.Message)
	}
}

func TestDetectDivergenceIsSilentWithoutAHistoryFile(t *testing.T) {
	stranded := ha.TimelinePosition{Timeline: 2, LSN: ha.MustParseLSN("0/80000A0")}
	divergence, err := DetectDivergence(t.TempDir(), stranded, 3)
	if err != nil {
		t.Fatalf("DetectDivergence = %v", err)
	}
	if divergence.Diverged {
		t.Fatalf("nothing on disk says where the histories parted: %s", divergence.Message)
	}
}

func TestHeldPositionTakesTheFurthestOfTheThreePositions(t *testing.T) {
	observation := MemberObservation{
		Timeline:          2,
		ReceivedLSN:       "",
		ReplayLSN:         "0/7000000",
		MinRecoveryEndLSN: "0/80000A0",
	}
	position := observation.HeldPosition()
	if position.Timeline != 2 || position.LSN != ha.MustParseLSN("0/80000A0") {
		t.Fatalf("held position = %+v; the durable minimum recovery end is the only one a "+
			"member whose receiver never connected has", position)
	}
}

func TestStoppedPositionPrefersTheRecoveryTimelineOverTheCheckpointsOwn(t *testing.T) {
	position := StoppedPosition(pgtool.ControlData{
		TimelineID:             2,
		MinRecoveryEnd:         "0/80000A0",
		MinRecoveryEndTimeline: 3,
	})
	if position.Timeline != 3 {
		t.Errorf("timeline = %d, want 3: a standby restartpoints long after it follows a switch",
			position.Timeline)
	}
	if position.LSN != ha.MustParseLSN("0/80000A0") {
		t.Errorf("lsn = %s", position.LSN)
	}
}

func TestPrimaryTimelineIsZeroWhenTheInstanceHasNoRecordOfIt(t *testing.T) {
	instance := &pgelasticv1alpha1.PgInstance{
		Status: pgelasticv1alpha1.PgInstanceStatus{
			Instances: []pgelasticv1alpha1.InstanceMemberStatus{{Name: "pg-1", Timeline: 4}},
		},
	}
	if timeline := PrimaryTimeline(instance, "pg-1"); timeline != 4 {
		t.Errorf("PrimaryTimeline = %d, want 4", timeline)
	}
	if timeline := PrimaryTimeline(instance, "pg-2"); timeline != 0 {
		t.Errorf("PrimaryTimeline = %d, want 0", timeline)
	}
	if timeline := PrimaryTimeline(nil, "pg-1"); timeline != 0 {
		t.Errorf("PrimaryTimeline = %d, want 0", timeline)
	}
}
