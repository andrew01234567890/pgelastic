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
	"crypto/rand"
	"encoding/hex"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgbackrest"
)

// NewSession mints this instance manager process's identity.
//
// It changes on every start, including a container restart in place, and that is the whole
// point. A backup runs as a goroutine inside a process that can die; without a token that
// dies with it, a restarted agent leaves its predecessor's backup marked Running forever,
// indistinguishable from one still in progress. The operator compares this against the
// session recorded on the backup and fails the ones nobody is taking any more.
func NewSession() string {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		// crypto/rand does not fail on any platform this runs on, and a session that
		// collided would at worst let an orphaned backup look live until the next restart.
		return "unknown"
	}
	return hex.EncodeToString(token)
}

// Session is the identity this agent process claims backups under.
func (s *Supervisor) Session() string { return s.session }

// reconcileBackup acts on the operator's election, and only on this member's own.
//
// The backup runs in a goroutine on the supervisor's own context rather than the observe
// tick's, because it outlives that tick by minutes to hours and cancelling it halfway
// leaves a partial backup in the repository that pgBackRest would have to resume or
// discard. The one-at-a-time flag is what stops every subsequent tick starting another.
//
// Only the primary takes backups. pgBackRest runs the backup control functions against the
// cluster it is configured with, and refuses one that is in recovery: a standby backup
// needs the primary configured as a second host, which needs pgBackRest's TLS server in
// every Pod. spec.backup.backupStandby is therefore a preference that cannot be honoured
// yet, and the operator says so on the backup rather than silently doing something else.
func (s *Supervisor) reconcileBackup(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	observation MemberObservation,
) {
	if s.options.Client == nil || instance == nil || observation.Role != RolePrimary {
		return
	}
	pending := instance.Status.PendingBackup
	if pending == nil || pending.Member != s.options.Member {
		return
	}

	s.mutex.Lock()
	if s.backingUp {
		s.mutex.Unlock()
		return
	}
	s.backingUp = true
	s.mutex.Unlock()

	name := pending.Name
	go func() {
		defer func() {
			s.mutex.Lock()
			s.backingUp = false
			s.mutex.Unlock()
		}()
		if err := TakeBackup(ctx, s.options, s.session, name); err != nil {
			logf.FromContext(ctx).Error(err, "the backup could not be taken", "backup", name)
			return
		}
		// Expiry runs on the back of a successful backup, which is the only moment the
		// recovery window can safely move forward: expiring while nothing new has arrived
		// would eat the window rather than maintain it.
		if err := ExpireBackups(ctx, s.options); err != nil {
			logf.FromContext(ctx).Error(err, "the retention policy could not be applied")
		}
	}()
}

// TakeBackup claims the backup this member was elected for and runs it to completion.
//
// Claiming is an optimistic update rather than a patch: two members that both believe they
// were elected - which is what a failover in the middle of an election looks like - race on
// the resource version, and exactly one of them wins. The loser returns without doing
// anything, rather than taking a second backup nobody asked for.
func TakeBackup(ctx context.Context, options Options, session, name string) error {
	log := logf.FromContext(ctx).WithValues("backup", name)

	backup := &pgelasticv1alpha1.PgBackup{}
	key := types.NamespacedName{Namespace: options.Namespace, Name: name}
	if err := options.Client.Get(ctx, key, backup); err != nil {
		return client.IgnoreNotFound(err)
	}
	if backup.Status.Phase != "" && backup.Status.Phase != pgelasticv1alpha1.BackupPhasePending {
		return nil
	}

	invocation, configured, err := ensureRepository(ctx, options)
	if err != nil {
		return finishBackup(ctx, options, backup, err)
	}
	if !configured {
		return finishBackup(ctx, options, backup,
			fmt.Errorf("this instance has no repository to back up into"))
	}

	backup.Status.Phase = pgelasticv1alpha1.BackupPhaseRunning
	backup.Status.Member = options.Member
	backup.Status.AgentSession = session
	backup.Status.Stanza = invocation.Stanza
	if err := options.Client.Status().Update(ctx, backup); err != nil {
		// A conflict means somebody else claimed it first, which is the outcome this update
		// exists to produce rather than an error to report.
		if apierrors.IsConflict(err) {
			log.Info("another member claimed this backup first")
			return nil
		}
		return err
	}

	log.Info("taking a backup", "type", backup.Spec.Type, "stanza", invocation.Stanza)
	kind := backupType(backup.Spec.Type)
	_, runErr := (pgbackrest.Runner{}).Run(ctx, invocation.Backup(kind, name))
	return finishBackup(ctx, options, backup, runErr)
}

// finishBackup writes the terminal status, reading what the backup turned out to be back
// out of the repository rather than from what the command reported.
//
// The repository is the source of truth about what is in it. A status assembled from the
// command's own account can disagree with what actually landed, and that disagreement is
// discovered at restore time.
func finishBackup(
	ctx context.Context,
	options Options,
	backup *pgelasticv1alpha1.PgBackup,
	cause error,
) error {
	log := logf.FromContext(ctx).WithValues("backup", backup.Name)

	if cause != nil {
		log.Error(cause, "the backup failed")
		return writeTerminalStatus(ctx, options, backup.Name,
			func(status *pgelasticv1alpha1.PgBackupStatus) {
				status.Phase = pgelasticv1alpha1.BackupPhaseFailed
				status.Error = truncate(cause.Error(), maxFailureMessage)
			})
	}

	taken, err := readBackBackup(ctx, options, backup.Name)
	if err != nil {
		// The backup itself succeeded and the catalogue could not be read. Reporting
		// success without the LSN range would publish a backup nothing could plan a restore
		// from, which is worse than reporting the failure to read it.
		return writeTerminalStatus(ctx, options, backup.Name,
			func(status *pgelasticv1alpha1.PgBackupStatus) {
				status.Phase = pgelasticv1alpha1.BackupPhaseFailed
				status.Error = truncate(err.Error(), maxFailureMessage)
			})
	}

	// The five settings PostgreSQL refuses to begin recovery below are the source's, and a
	// restore target's own configuration is not evidence about them. Recorded here so a
	// restore can raise its floor before it starts rather than FATAL after it has pulled
	// the whole base backup down.
	controlData, controlErr := toolchain(options).ControlData(ctx, options.DataDir)

	log.Info("the backup completed", "backupID", taken.Label, "sizeBytes", taken.SizeBytes)
	return writeTerminalStatus(ctx, options, backup.Name,
		func(status *pgelasticv1alpha1.PgBackupStatus) {
			status.Phase = pgelasticv1alpha1.BackupPhaseCompleted
			status.Error = ""
			status.BackupID = taken.Label
			status.Type = instanceBackupType(taken.Type)
			status.StartedAt = &metav1.Time{Time: taken.Started}
			status.StoppedAt = &metav1.Time{Time: taken.Stopped}
			status.BeginLSN = taken.BeginLSN
			status.EndLSN = taken.EndLSN
			status.BeginWAL = taken.BeginWAL
			status.EndWAL = taken.EndWAL
			status.SizeBytes = taken.SizeBytes
			status.SystemIdentifier = taken.SystemIdentifier
			status.Repository = repositoryOf(options)
			if controlErr == nil {
				status.SourceEnforcedParameters = controlData.EnforcedSettings
				status.Timeline = controlData.TimelineID
			}
		})
}

// writeTerminalStatus records how a backup ended, against whatever the object looks like now.
//
// It cannot write through the copy the claim was made on. Claiming sets the phase to Running,
// the PgBackup controller sees that and rewrites the object's conditions to say so, and by
// then the agent's copy is stale - while the backup it describes still has minutes to hours
// to run. Writing through it lost every terminal status to a conflict, so a backup that had
// completed stayed Running for ever, and because a running backup holds the election, no
// later backup was taken either.
func writeTerminalStatus(
	ctx context.Context,
	options Options,
	name string,
	apply func(*pgelasticv1alpha1.PgBackupStatus),
) error {
	key := types.NamespacedName{Namespace: options.Namespace, Name: name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &pgelasticv1alpha1.PgBackup{}
		if err := options.Client.Get(ctx, key, fresh); err != nil {
			// A backup deleted while it ran has nowhere to record what happened, and that is
			// somebody's decision rather than a failure of the backup.
			return client.IgnoreNotFound(err)
		}
		apply(&fresh.Status)
		return options.Client.Status().Update(ctx, fresh)
	})
}

// readBackBackup finds this backup in the repository catalogue by the annotation it was
// tagged with, rather than by taking the newest entry.
//
// Newest-wins would be wrong exactly when it matters: a scheduled backup and an on-demand
// one that overlapped would each claim the other's result, and the LSN range recorded
// against a backup is what a restore trusts.
func readBackBackup(ctx context.Context, options Options, name string) (pgbackrest.Backup, error) {
	invocation, configured, err := ensureRepository(ctx, options)
	if err != nil {
		return pgbackrest.Backup{}, err
	}
	if !configured {
		return pgbackrest.Backup{}, fmt.Errorf("this instance has no repository")
	}

	document, err := (pgbackrest.Runner{}).Run(ctx, invocation.Info())
	if err != nil {
		return pgbackrest.Backup{}, err
	}
	backups, err := pgbackrest.ParseInfo(document, invocation.Stanza)
	if err != nil {
		return pgbackrest.Backup{}, err
	}
	for _, candidate := range backups {
		if candidate.Annotation[pgbackrest.BackupAnnotation] == name {
			return candidate, nil
		}
	}
	return pgbackrest.Backup{}, fmt.Errorf(
		"pgBackRest reported success and the repository holds no backup annotated %q", name)
}

// ExpireBackups applies the stanza's retention policy.
//
// It runs after a backup rather than on a schedule of its own, which is pgBackRest's own
// arrangement and has one consequence worth stating: an instance that stops backing up also
// stops expiring, and keeps everything forever. That is the right trade - expiring while
// nothing new is arriving would eat the recovery window rather than defend it - but the
// storage it costs is a real bill and the archive-degraded condition is what surfaces it.
func ExpireBackups(ctx context.Context, options Options) error {
	invocation, configured, err := ensureRepository(ctx, options)
	if err != nil || !configured {
		return err
	}
	_, err = (pgbackrest.Runner{}).Run(ctx, invocation.Expire())
	return err
}

func repositoryOf(options Options) *pgelasticv1alpha1.ObjectStore {
	repository := options.Config.Backup
	if repository == nil {
		return nil
	}
	return &pgelasticv1alpha1.ObjectStore{
		Path:        repository.Path,
		EndpointURL: repository.EndpointURL,
		Region:      repository.Region,
	}
}

func backupType(kind pgelasticv1alpha1.BackupType) pgbackrest.BackupType {
	switch kind {
	case pgelasticv1alpha1.BackupTypeDifferential:
		return pgbackrest.BackupDifferential
	case pgelasticv1alpha1.BackupTypeIncremental:
		return pgbackrest.BackupIncremental
	default:
		return pgbackrest.BackupFull
	}
}

// instanceBackupType translates back, because pgBackRest promotes a differential or an
// incremental to a full when there is no full to descend from, and the status has to say
// what was taken rather than what was asked for.
func instanceBackupType(kind pgbackrest.BackupType) pgelasticv1alpha1.BackupType {
	switch kind {
	case pgbackrest.BackupDifferential:
		return pgelasticv1alpha1.BackupTypeDifferential
	case pgbackrest.BackupIncremental:
		return pgelasticv1alpha1.BackupTypeIncremental
	default:
		return pgelasticv1alpha1.BackupTypeFull
	}
}
