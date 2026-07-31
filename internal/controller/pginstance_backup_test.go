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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// The member and the session every orphan-detection spec is written against.
const (
	testMember  = "pg-a-1"
	testSession = "the-session-that-claimed-it"
)

func pendingBackupNamed(name string, created time.Time) pgelasticv1alpha1.PgBackup {
	return pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
	}
}

// Backups have to run in a total order that every controller and every member computes the
// same way. Two created in the same instant is not hypothetical - a scheduled one and an
// on-demand one land together often enough - and picking arbitrarily would let two members
// each believe they were elected.
func TestTheOldestPendingBackupWinsWithTheNameAsTiebreak(t *testing.T) {
	early := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	backups := []pgelasticv1alpha1.PgBackup{
		pendingBackupNamed("zulu", early),
		pendingBackupNamed("alpha", early),
		pendingBackupNamed("later", early.Add(time.Hour)),
	}
	next := oldestPending(backups)
	if next == nil || next.Name != "alpha" {
		t.Fatalf("elected %v, want the alphabetically first of the two oldest", next)
	}
}

// A backup already being taken keeps the election. Starting a second alongside it would
// contend for the same repository and the same member's I/O, and pgBackRest would refuse
// the second anyway - after it had already been marked Running.
func TestARunningBackupKeepsTheElection(t *testing.T) {
	early := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	running := pendingBackupNamed("running", early.Add(time.Hour))
	running.Status.Phase = pgelasticv1alpha1.BackupPhaseRunning

	next := oldestPending([]pgelasticv1alpha1.PgBackup{
		pendingBackupNamed("older-and-waiting", early),
		running,
	})
	if next == nil || next.Name != "running" {
		t.Fatalf("elected %v, want the backup already being taken", next)
	}
}

func TestTerminalBackupsAreNeverElected(t *testing.T) {
	now := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	completed := pendingBackupNamed("completed", now)
	completed.Status.Phase = pgelasticv1alpha1.BackupPhaseCompleted
	failed := pendingBackupNamed("failed", now)
	failed.Status.Phase = pgelasticv1alpha1.BackupPhaseFailed

	if next := oldestPending([]pgelasticv1alpha1.PgBackup{completed, failed}); next != nil {
		t.Fatalf("elected %s, want nothing", next.Name)
	}
}

// The name is derived from the slot rather than generated, so two reconciles of the same
// minute produce one backup. A generated name would make the idempotency depend on the
// controller never reconciling twice, which is not a property controllers have.
func TestTheScheduledNameIsTheSlotItFills(t *testing.T) {
	slot := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	first := scheduledBackupName("pg-a", slot)
	second := scheduledBackupName("pg-a", slot)
	if first != second {
		t.Fatalf("the same slot produced %q and %q", first, second)
	}
	if next := scheduledBackupName("pg-a", slot.Add(24*time.Hour)); next == first {
		t.Fatalf("two slots produced one name %q", first)
	}
	if !strings.HasPrefix(first, "pg-a-") {
		t.Errorf("name = %q, want it to name its instance", first)
	}
}

// A slot named in local time would move under a controller whose node changed timezone, and
// two controllers in different zones would disagree about which slot they were filling.
func TestTheScheduledNameIsInUTC(t *testing.T) {
	zone := time.FixedZone("UTC+13", 13*3600)
	slot := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	if scheduledBackupName("pg-a", slot.In(zone)) != scheduledBackupName("pg-a", slot) {
		t.Fatal("the same instant in two zones produced two names")
	}
}

// A five-field cron is what everybody who has written a CronJob writes. CNPG's six-field
// variant silently shifts every field by one rather than failing, and their own docs carry
// a warning about it.
func TestTheScheduleIsFiveFieldCron(t *testing.T) {
	if _, err := backupScheduleParser.Parse("0 2 * * *"); err != nil {
		t.Fatalf("the documented default does not parse: %v", err)
	}
	if _, err := backupScheduleParser.Parse("0 0 2 * * *"); err == nil {
		t.Fatal("a six-field expression parsed, so a seconds field would shift every other")
	}
}

// "@every 6h" is a delay from an arbitrary anchor, not a grid of fixed times, so it has no
// most-recent-firing for dueSlot to find. Accepted, it was silent for delays longer than the
// grace window and minted a differently-named backup every minute for delays shorter than
// it - a real full backup each time. Refused at the parse instead, which is a logged error
// naming the schedule.
func TestADelayIsNotASchedule(t *testing.T) {
	for _, expression := range []string{"@every 6h", "@every 30m", "@hourly", "@daily"} {
		if _, err := backupScheduleParser.Parse(expression); err == nil {
			t.Errorf("%q parsed; the API documents a five-field cron expression and dueSlot "+
				"can only answer for one", expression)
		}
	}
}

// An instance whose archiving has stopped must not take a base backup. It would need every
// WAL segment from its own start position to reach consistency, and those are precisely the
// ones not arriving - so the result is an object in a bucket that no restore can use.
func TestArchivingMustWorkBeforeABackupIsElected(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		health *pgelasticv1alpha1.ArchiveHealthStatus
		want   bool
	}{
		{"nothing reported", nil, false},
		{"reported degraded", &pgelasticv1alpha1.ArchiveHealthStatus{Healthy: false}, false},
		{"reported working", &pgelasticv1alpha1.ArchiveHealthStatus{Healthy: true}, true},
	} {
		instance := &pgelasticv1alpha1.PgInstance{
			Status: pgelasticv1alpha1.PgInstanceStatus{ArchiveHealth: testCase.health},
		}
		if got := archivingWorks(instance); got != testCase.want {
			t.Errorf("%s: archivingWorks = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// A backup left Running by an agent that died never completes, never fails, and holds the
// election - so no later backup is ever taken either. The session token is what tells a
// backup still being taken from one nobody is taking.
func TestABackupIsOrphanedWhenItsAgentRestarted(t *testing.T) {
	reconciler := &PgBackupReconciler{}
	status := &pgelasticv1alpha1.PgBackupStatus{
		Phase:        pgelasticv1alpha1.BackupPhaseRunning,
		Member:       testMember,
		AgentSession: testSession,
	}
	instance := &pgelasticv1alpha1.PgInstance{
		Status: pgelasticv1alpha1.PgInstanceStatus{
			Instances: []pgelasticv1alpha1.InstanceMemberStatus{
				{Name: testMember, AgentSession: "a-different-session"},
			},
		},
	}
	reconciler.orphanIfNobodyIsTakingIt(status, instance)
	if status.Phase != pgelasticv1alpha1.BackupPhaseFailed {
		t.Fatalf("phase = %q, want the backup failed", status.Phase)
	}
	if !strings.Contains(status.Error, testMember) {
		t.Errorf("error = %q, want it to name the member", status.Error)
	}
}

// Silence is not evidence. An agent that is not reporting is very often a working agent
// behind a broken network, and failing its backup on that basis would destroy work that was
// about to succeed.
func TestABackupIsNotOrphanedOnSilence(t *testing.T) {
	reconciler := &PgBackupReconciler{}
	for _, testCase := range []struct {
		name     string
		instance *pgelasticv1alpha1.PgInstance
	}{
		{"the instance is gone", nil},
		{"the member reports nothing", &pgelasticv1alpha1.PgInstance{
			Status: pgelasticv1alpha1.PgInstanceStatus{
				Instances: []pgelasticv1alpha1.InstanceMemberStatus{{Name: testMember}},
			},
		}},
		{"the member is absent from the list", &pgelasticv1alpha1.PgInstance{}},
		{"the session is the one that claimed it", &pgelasticv1alpha1.PgInstance{
			Status: pgelasticv1alpha1.PgInstanceStatus{
				Instances: []pgelasticv1alpha1.InstanceMemberStatus{
					{Name: testMember, AgentSession: testSession},
				},
			},
		}},
	} {
		status := &pgelasticv1alpha1.PgBackupStatus{
			Phase:        pgelasticv1alpha1.BackupPhaseRunning,
			Member:       testMember,
			AgentSession: testSession,
		}
		reconciler.orphanIfNobodyIsTakingIt(status, testCase.instance)
		if status.Phase != pgelasticv1alpha1.BackupPhaseRunning {
			t.Errorf("%s: phase = %q, want it left alone", testCase.name, status.Phase)
		}
	}
}

// The controller must never recompute a phase the member wrote. Every stuck-forever bug
// CloudNativePG fixed across two releases was a controller overwriting a terminal status
// that an agent had just written.
func TestThePhaseAMemberWroteIsCarriedForward(t *testing.T) {
	for _, phase := range []pgelasticv1alpha1.BackupPhase{
		pgelasticv1alpha1.BackupPhaseRunning,
		pgelasticv1alpha1.BackupPhaseCompleted,
		pgelasticv1alpha1.BackupPhaseFailed,
	} {
		status := &pgelasticv1alpha1.PgBackupStatus{Phase: phase}
		if got := backupPhase(status); got != phase {
			t.Errorf("phase %q became %q", phase, got)
		}
	}
	if got := backupPhase(&pgelasticv1alpha1.PgBackupStatus{}); got != pgelasticv1alpha1.BackupPhasePending {
		t.Errorf("an unclaimed backup reads as %q, want Pending", got)
	}
}

// Pending covers two situations that call for entirely different actions: nobody has picked
// the backup up yet, which resolves itself, and archiving is not working, which does not.
func TestAPendingBackupSaysWhichKindOfPendingItIs(t *testing.T) {
	waiting := &pgelasticv1alpha1.PgInstance{
		Spec: pgelasticv1alpha1.PgInstanceSpec{Backup: &pgelasticv1alpha1.InstanceBackup{}},
		Status: pgelasticv1alpha1.PgInstanceStatus{
			ArchiveHealth: &pgelasticv1alpha1.ArchiveHealthStatus{Healthy: true},
		},
	}
	if got := progressReason(&pgelasticv1alpha1.PgBackupStatus{}, waiting); got != pgelasticv1alpha1.ReasonPending {
		t.Errorf("reason = %q, want Pending", got)
	}

	degraded := waiting.DeepCopy()
	degraded.Status.ArchiveHealth.Healthy = false
	if got := progressReason(&pgelasticv1alpha1.PgBackupStatus{}, degraded); got != pgelasticv1alpha1.ReasonArchiveDegraded {
		t.Errorf("reason = %q, want ArchiveDegraded", got)
	}
	message := progressMessage(&pgelasticv1alpha1.PgBackupStatus{}, degraded)
	if !strings.Contains(message, "consistency") {
		t.Errorf("message = %q, want it to say why a backup now would be unusable", message)
	}
}

// The summary the gates read has to come from a completed backup and carry the source's
// max_connections, because a restore into an instance configured lower than that FATALs at
// start-up with a message naming the parameter and not the cause.
func TestTheLastBackupSummaryIsTheNewestCompletedOne(t *testing.T) {
	stopped := func(offset time.Duration) *metav1.Time {
		moment := metav1.NewTime(time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC).Add(offset))
		return &moment
	}
	older := pendingBackupNamed("older", time.Time{})
	older.Status = pgelasticv1alpha1.PgBackupStatus{
		Phase: pgelasticv1alpha1.BackupPhaseCompleted, StoppedAt: stopped(0), SizeBytes: 1,
	}
	newer := pendingBackupNamed("newer", time.Time{})
	newer.Status = pgelasticv1alpha1.PgBackupStatus{
		Phase:                    pgelasticv1alpha1.BackupPhaseCompleted,
		StoppedAt:                stopped(time.Hour),
		SizeBytes:                2,
		Type:                     pgelasticv1alpha1.BackupTypeDifferential,
		SourceEnforcedParameters: map[string]int32{"max_connections": 422},
	}
	running := pendingBackupNamed("running", time.Time{})
	running.Status.Phase = pgelasticv1alpha1.BackupPhaseRunning

	summary := lastBackupSummary([]pgelasticv1alpha1.PgBackup{older, running, newer})
	if summary == nil {
		t.Fatal("no summary was produced from two completed backups")
	}
	if summary.SizeBytes != 2 || summary.Type != pgelasticv1alpha1.BackupTypeDifferential {
		t.Errorf("summary = %+v, want the newer backup", summary)
	}
	if summary.SourceMaxConnections != 422 {
		t.Errorf("sourceMaxConnections = %d, want the source's", summary.SourceMaxConnections)
	}
	// Nothing has verified it, and completing is not verification. A backup reported as
	// verified because it finished is an assurance worse than none.
	if summary.Verified {
		t.Error("a backup was reported verified without anything having verified it")
	}
}

func TestNoSummaryWithoutACompletedBackup(t *testing.T) {
	running := pendingBackupNamed("running", time.Time{})
	running.Status.Phase = pgelasticv1alpha1.BackupPhaseRunning
	if summary := lastBackupSummary([]pgelasticv1alpha1.PgBackup{running}); summary != nil {
		t.Fatalf("summary = %+v, want nothing", summary)
	}
}

// Every minute of a day, counted, because the interesting answer is a total rather than a
// verdict at one instant.
//
// The slot used to be derived by reflecting the next firing backwards, which lands on the
// right slot only once a third of the period has passed - by which time the grace window has
// already rejected it for being stale. For the documented default the two windows never
// intersect, so a nightly backup was minted on none of the day's 1440 minutes. A spot check
// at 02:00 would have reported that as a pass; only the count catches it.
func TestEachScheduleIsDueForExactlyItsGraceWindow(t *testing.T) {
	midnight := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		expression string
		want       int
	}{
		{"0 2 * * *", 60},
		{"0 */6 * * *", 240},
		// Firing more often than the window is wide does not mint more: one slot is due at
		// any minute, and it is the newest.
		{"0 * * * *", 1440},
		{"*/5 * * * *", 1440},
	} {
		schedule, err := backupScheduleParser.Parse(testCase.expression)
		if err != nil {
			t.Fatalf("%s does not parse: %v", testCase.expression, err)
		}
		due := 0
		for minute := range 1440 {
			if _, ok := dueSlot(schedule, midnight.Add(time.Duration(minute)*time.Minute)); ok {
				due++
			}
		}
		if due != testCase.want {
			t.Errorf("%s is due on %d of 1440 minutes, want %d",
				testCase.expression, due, testCase.want)
		}
	}
}

// The newest firing inside the window, not the oldest. A schedule that fires more often than
// the grace window is wide has several to choose from, and naming the backup after a slot
// that has already been filled and passed would mint one nobody asked for.
func TestTheDueSlotIsTheNewestFiringInsideTheWindow(t *testing.T) {
	schedule, err := backupScheduleParser.Parse("*/5 * * * *")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	slot, due := dueSlot(schedule, time.Date(2026, 7, 31, 2, 32, 0, 0, time.UTC))
	if !due {
		t.Fatal("nothing was due, and a schedule firing every five minutes always has a slot")
	}
	if want := time.Date(2026, 7, 31, 2, 30, 0, 0, time.UTC); !slot.Equal(want) {
		t.Errorf("slot = %s, want the newest firing at or before now (%s)", slot, want)
	}
}

// A controller that was down for a day should not mint yesterday's backup the moment it
// comes back, and then today's an hour later.
func TestASlotOlderThanTheGraceWindowIsNotDue(t *testing.T) {
	schedule, err := backupScheduleParser.Parse("0 2 * * *")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, due := dueSlot(schedule, time.Date(2026, 7, 31, 3, 1, 0, 0, time.UTC)); due {
		t.Error("the 02:00 slot was still due at 03:01, an hour and a minute late")
	}
}

// "0 0 30 2 *" parses and never fires, because February has no thirtieth. robfig answers a
// zero time for it, which the walk has to recognise as an end rather than as a slot to
// advance from.
func TestAScheduleThatNeverFiresIsNeverDue(t *testing.T) {
	schedule, err := backupScheduleParser.Parse("0 0 30 2 *")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	if _, due := dueSlot(schedule, time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)); due {
		t.Error("a schedule that can never fire reported a due slot")
	}
}
