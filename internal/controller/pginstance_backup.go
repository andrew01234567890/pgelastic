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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/index"
)

// backupScheduleParser is the standard five-field cron, with no seconds field.
//
// CNPG's six-field variant is a documented usability wart in their own docs: every operator
// who has written a CronJob writes five fields, and the sixth silently shifts everything by
// one position rather than failing.
//
// Descriptors are deliberately not enabled, matching internal/autoscale. "@every 6h" is not
// a cron expression at all: it is a delay measured from an arbitrary anchor rather than a
// grid of fixed times, so it has no "most recent firing at or before now" for dueSlot to
// find. Accepting it produced silence for delays longer than the grace window and a backup
// every minute for delays shorter than it. Refusing it at the parse is a logged error naming
// the schedule, which is the failure an operator can act on.
var backupScheduleParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// backupState is what one pass over an instance's backups concluded.
type backupState struct {
	// pending is the backup a member should be taking, if any.
	pending *pgelasticv1alpha1.PendingBackup
	// requeue is when the next scheduled backup falls due, so the reconcile heartbeat does
	// not have to be fast enough to notice a cron minute by luck.
	requeue time.Duration
	// last summarises the newest completed backup, which is what the gates read rather than
	// listing backups themselves.
	last *pgelasticv1alpha1.BackupSummary
}

// reconcileBackups mints the scheduled backup when one is due and elects a member to take
// whichever backup is next in line.
//
// The operator's whole authority here is status.pendingBackup. It never runs anything
// inside a Pod: the member's own agent reads this object every observe tick anyway, claims
// the backup with an optimistic update, and reports what happened. That is the same split
// targetPrimary uses, and it is what keeps the operator's ServiceAccount free of
// pods/exec - which CNPG needs for exactly this and documents as a mistake.
func (r *PgInstanceReconciler) reconcileBackups(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
) backupState {
	log := logf.FromContext(ctx)
	if instance.Spec.Backup == nil {
		return backupState{}
	}
	// An instance recovering from a repository has that repository configured so it can
	// read from it, and must not be scheduled to write to it. It carries its source's
	// system identifier - a physical restore copies the control file verbatim - so it
	// addresses the source's stanza while running on a forked timeline, and a backup taken
	// from here would land in somebody else's archive.
	if instance.Spec.Restore != nil {
		return backupState{}
	}

	backups, err := r.backupsOf(ctx, instance)
	if err != nil {
		log.Error(err, "could not read this instance's backups")
		return backupState{}
	}

	state := backupState{
		requeue: r.mintScheduledBackup(ctx, instance, backups),
		last:    lastBackupSummary(backups),
	}

	// An election is only made once archiving is known to work. A base backup whose WAL
	// cannot reach the repository is not restorable: it needs every segment from its own
	// start LSN to reach consistency, and those are precisely the ones that would be lost.
	if !archivingWorks(instance) {
		return state
	}

	next := oldestPending(backups)
	if next == nil {
		return state
	}
	member := r.electBackupMember(instance, next, decision)
	if member == "" {
		return state
	}
	state.pending = &pgelasticv1alpha1.PendingBackup{
		Name:        next.Name,
		Member:      member,
		RequestedAt: metav1.Now(),
	}
	// The election is republished with a fresh timestamp on every pass, which would defeat
	// the point of recording one. Carry the existing timestamp forward while the election
	// stands, so "how long has this member been asked and not answered" is answerable.
	if existing := instance.Status.PendingBackup; existing != nil &&
		existing.Name == next.Name && existing.Member == member {
		state.pending.RequestedAt = existing.RequestedAt
	}
	return state
}

// backupsOf lists this instance's backups through the index.
func (r *PgInstanceReconciler) backupsOf(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) ([]pgelasticv1alpha1.PgBackup, error) {
	list := &pgelasticv1alpha1.PgBackupList{}
	if err := r.List(ctx, list,
		client.InNamespace(instance.Namespace),
		client.MatchingFields{index.BackupByInstance: instance.Name},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// mintScheduledBackup creates the scheduled backup when one is due, and returns how long
// until the next one is.
//
// The name is derived from the schedule slot rather than generated, so two reconciles of
// the same minute produce one backup: the second create is an AlreadyExists that is
// deliberately not an error. A generated name would make the idempotency depend on the
// controller never reconciling twice, which is not a property controllers have.
func (r *PgInstanceReconciler) mintScheduledBackup(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	existing []pgelasticv1alpha1.PgBackup,
) time.Duration {
	log := logf.FromContext(ctx)
	expression := "0 2 * * *"
	if schedule := instance.Spec.Backup.Schedule; schedule != nil && *schedule != "" {
		expression = *schedule
	}
	schedule, err := backupScheduleParser.Parse(expression)
	if err != nil {
		log.Error(err, "the backup schedule is not a five-field cron expression",
			"schedule", expression)
		return 0
	}

	now := r.now()
	// A schedule that parses and never fires again - "0 0 30 2 *" is one - answers a zero
	// time here, and subtracting from it would hand back a hugely negative requeue.
	var untilNext time.Duration
	if next := schedule.Next(now); !next.IsZero() {
		untilNext = next.Sub(now)
	}

	slot, due := dueSlot(schedule, now)
	if !due {
		return untilNext
	}

	name := scheduledBackupName(instance.Name, slot)
	if slices.ContainsFunc(existing, func(backup pgelasticv1alpha1.PgBackup) bool {
		return backup.Name == name
	}) {
		return untilNext
	}

	backup := &pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace},
		Spec: pgelasticv1alpha1.PgBackupSpec{
			InstanceRef: corev1.LocalObjectReference{Name: instance.Name},
			Type:        pgelasticv1alpha1.BackupTypeFull,
			Target:      backupTargetOf(instance),
		},
	}
	if err := r.Create(ctx, backup); err != nil && !apierrors.IsAlreadyExists(err) {
		log.Error(err, "could not create the scheduled backup", "backup", name)
	}
	return untilNext
}

// dueSlot returns the most recent firing in (now-grace, now], if there is one.
//
// A controller that was down over the scheduled minute should still take the backup when it
// comes back; one that was down for a day should not take yesterday's the moment it starts
// and then today's an hour later. The grace window is therefore the whole of the lookback,
// and the walk forward is what picks the newest slot inside it when a schedule fires more
// often than the window is wide.
//
// It is written as a walk rather than as arithmetic on the interval because a cron schedule
// has no single interval: "0 2 * * *" and "*/5 9-17 * * 1-5" both answer Next, and only one
// of them can be reflected around it.
//
// The zero checks are not defensive tidiness: "0 0 30 2 *" parses and never fires, and Next
// answers a zero time for it. Without them the walk would never advance and would spin.
func dueSlot(schedule cron.Schedule, now time.Time) (time.Time, bool) {
	slot := schedule.Next(now.Add(-backupScheduleGrace))
	if slot.IsZero() || slot.After(now) {
		return time.Time{}, false
	}
	for {
		following := schedule.Next(slot)
		if following.IsZero() || following.After(now) {
			return slot, true
		}
		slot = following
	}
}

// backupScheduleGrace bounds how late a scheduled backup may still be taken.
//
// An operator that was down over the scheduled minute should still take the backup when it
// comes back; one that was down for a day should not take yesterday's the moment it starts
// and then today's an hour later. An hour is comfortably longer than any restart and
// comfortably shorter than the shortest schedule anybody writes.
const backupScheduleGrace = time.Hour

// scheduledBackupName names a backup after the slot it fills, in UTC, so the same slot
// cannot be filled twice.
func scheduledBackupName(instance string, slot time.Time) string {
	return fmt.Sprintf("%s-%s", instance, slot.UTC().Format("20060102t1504"))
}

// oldestPending picks the backup to run next: creation time, with the name as a tiebreak.
//
// A total order is what lets several controllers and several members converge on the same
// answer without a lock. Two backups created in the same instant is not hypothetical - a
// scheduled one and an on-demand one land together often enough - and picking arbitrarily
// would let two members each believe they were elected.
func oldestPending(backups []pgelasticv1alpha1.PgBackup) *pgelasticv1alpha1.PgBackup {
	// A backup already being taken keeps the election: taking a second one alongside it
	// would contend for the same repository and the same member's I/O.
	for i := range backups {
		if backups[i].Status.Phase == pgelasticv1alpha1.BackupPhaseRunning {
			return &backups[i]
		}
	}

	var oldest *pgelasticv1alpha1.PgBackup
	for i := range backups {
		candidate := &backups[i]
		if candidate.Status.Phase != "" &&
			candidate.Status.Phase != pgelasticv1alpha1.BackupPhasePending {
			continue
		}
		if oldest == nil || earlierBackup(candidate, oldest) {
			oldest = candidate
		}
	}
	return oldest
}

func earlierBackup(candidate, incumbent *pgelasticv1alpha1.PgBackup) bool {
	if !candidate.CreationTimestamp.Equal(&incumbent.CreationTimestamp) {
		return candidate.CreationTimestamp.Before(&incumbent.CreationTimestamp)
	}
	return strings.Compare(candidate.Name, incumbent.Name) < 0
}

// electBackupMember picks the member that will take the backup.
//
// It is always the primary, whatever the target asks for. pgBackRest runs the backup control
// functions against the cluster it is configured with and refuses one that is in recovery:
// backing up from a standby needs the primary configured as a second host, which needs
// pgBackRest's TLS server running in every Pod. The preference is not silently discarded -
// the PgBackup's condition says the standby was preferred and why it was not used - because
// an operator who asked for one thing and got another without being told is the failure
// mode this whole design keeps trying to avoid.
func (r *PgInstanceReconciler) electBackupMember(
	instance *pgelasticv1alpha1.PgInstance,
	_ *pgelasticv1alpha1.PgBackup,
	decision ha.Decision,
) string {
	primary := decision.ServingPrimary
	if primary == "" {
		primary = instance.Status.CurrentPrimary
	}
	return primary
}

// archivingWorks reports whether this instance's own status says WAL is reaching the
// repository.
func archivingWorks(instance *pgelasticv1alpha1.PgInstance) bool {
	health := instance.Status.ArchiveHealth
	return health != nil && health.Healthy
}

func backupTargetOf(instance *pgelasticv1alpha1.PgInstance) pgelasticv1alpha1.BackupTarget {
	if backup := instance.Spec.Backup; backup != nil &&
		backup.BackupStandby != nil && !*backup.BackupStandby {
		return pgelasticv1alpha1.BackupTargetPrimary
	}
	return pgelasticv1alpha1.BackupTargetPreferStandby
}

// lastBackupSummary projects the most recent completed backup onto the instance, which is
// what the admission and migration gates read rather than listing backups themselves.
func lastBackupSummary(
	backups []pgelasticv1alpha1.PgBackup,
) *pgelasticv1alpha1.BackupSummary {
	var latest *pgelasticv1alpha1.PgBackup
	for i := range backups {
		candidate := &backups[i]
		if candidate.Status.Phase != pgelasticv1alpha1.BackupPhaseCompleted ||
			candidate.Status.StoppedAt == nil {
			continue
		}
		if latest == nil || candidate.Status.StoppedAt.After(latest.Status.StoppedAt.Time) {
			latest = candidate
		}
	}
	if latest == nil {
		return nil
	}
	return &pgelasticv1alpha1.BackupSummary{
		At:        latest.Status.StoppedAt,
		Type:      latest.Status.Type,
		SizeBytes: latest.Status.SizeBytes,
		// Verified stays false until something has actually verified it. pgbackrest verify
		// is what would set it, and claiming a backup is verified because it completed is
		// the sort of assurance that is worse than none.
		SourceMaxConnections: latest.Status.SourceEnforcedParameters["max_connections"],
	}
}
