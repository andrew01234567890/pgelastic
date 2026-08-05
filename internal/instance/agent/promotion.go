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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// promoteTimeout bounds pg_ctl promote itself, and separately bounds the wait for
// standby.signal to disappear. PostgreSQL removes that file itself; its continued existence
// past the deadline means the promotion did not happen, and is a hard failure rather than
// something to tidy away.
const promoteTimeout = 60 * time.Second

// PromotionResult is what a completed promotion established. It is returned rather than
// only logged because the caller has to keep rendering the new epoch into this member's
// configuration on every later reload.
type PromotionResult struct {
	// Epoch is the fence token, derived from the Lease's transition counter.
	Epoch int64
	// SynchronousStandbyNames is the clause written for the new topology before any write
	// was accepted.
	SynchronousStandbyNames string
	// Timeline is the timeline the promotion produced. Every epoch bump must accompany a
	// timeline bump, and recording both is what lets that be checked afterwards.
	Timeline int32
	// PromoteLSN is where the new primary started writing. Paired with the old primary's
	// last known position it is the audit record for "was anything lost".
	PromoteLSN string
}

// ErrQuorumDenied is returned when the re-verification at promotion time fails. It is a
// refusal, not a fault: a stalled instance is recoverable and a lost acknowledged commit is
// not.
var ErrQuorumDenied = errors.New("the quorum gate denied the promotion")

// Promote runs the whole promotion sequence, in the order the design fixes it in.
//
// Every step is a precondition for the one after it. Acquiring the Lease first is what
// makes the sequence mutually exclusive; re-verifying the quorum evidence after acquiring
// it is what stops a decision made seconds ago from being acted on after the world changed;
// the CHECKPOINT is what lets the old primary rewind to the right divergence point; the
// synchronous_standby_names rewrite has to land before any write is accepted, because
// promoting with the old clause blocks every commit and accepting writes before the new one
// loads opens a window of local-only durability nobody can detect afterwards.
func Promote(ctx context.Context, options Options) (PromotionResult, error) {
	log := logf.FromContext(ctx)
	tools := toolchain(options)
	result := PromotionResult{}

	lease := LeaseManagerFor(options)
	acquireCtx, cancel := context.WithTimeout(ctx, lease.Config.AcquireTimeout())
	defer cancel()
	held, err := lease.Acquire(acquireCtx)
	if err != nil {
		return result, err
	}
	result.Epoch = ha.Epoch(ha.InitialPrimaryEpoch, LeaderTransitions(held))
	// Published before the promotion proceeds rather than after it returns: everything below
	// this line can fail or be slow, and the fence token must not be observable as the old
	// one for any of that time.
	if options.OnEpoch != nil {
		options.OnEpoch(result.Epoch)
	}
	log.Info("acquired the promotion lease", "member", options.Member, "epoch", result.Epoch)

	instance, err := reReadInstance(ctx, options)
	if err != nil {
		return result, err
	}
	if err := verifyQuorum(ctx, options, instance); err != nil {
		return result, err
	}

	conn, err := Connect(ctx, options.SocketDir, provision.PostgresPort)
	if err != nil {
		return result, fmt.Errorf("connecting to the postmaster being promoted: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Every step from here is idempotent, and the promotion is entered again from the top
	// if any of them fails. A sequence that could only be run once would leave a member
	// that had already left recovery with no way back to a complete promotion - out of
	// recovery, holding the lease, and with neither a quorum clause nor an epoch.
	inRecovery, err := readInRecovery(ctx, conn)
	if err != nil {
		return result, err
	}
	if inRecovery {
		if err := promoteLocal(ctx, options, tools); err != nil {
			return result, err
		}
		if err := awaitOutOfRecovery(ctx, conn); err != nil {
			return result, err
		}
	}

	// The checkpoint is not optional and its failure fails the promotion. Without it the
	// old primary's pg_rewind computes the wrong divergence point, and a rewind against the
	// wrong point does not fail - it produces a data directory that is quietly wrong.
	if err := Checkpoint(ctx, conn); err != nil {
		return result, fmt.Errorf("the post-promotion checkpoint failed, so the promotion failed: %w", err)
	}

	standbys := promotionStandbys(options, instance)
	if err := ensureReplicationSlots(ctx, conn, standbys); err != nil {
		return result, err
	}
	result.SynchronousStandbyNames = SynchronousStandbyNames(options.Config.Quorum, standbys)
	if err := writeSyncStandbyNames(ctx, options, tools, result.SynchronousStandbyNames); err != nil {
		return result, err
	}

	if err := publishEpoch(ctx, options, tools, result.Epoch); err != nil {
		return result, err
	}

	terminated, err := TerminateStaleBackends(ctx, conn)
	if err != nil {
		return result, err
	}
	log.Info("severed the connections inherited from the previous epoch", "connections", terminated)

	observation, err := Observe(ctx, conn)
	if err != nil {
		return result, err
	}
	result.Timeline, result.PromoteLSN = observation.Timeline, observation.LSN

	reporter := Reporter{
		Client:    options.Client,
		Namespace: options.Namespace,
		Instance:  options.Instance,
		Member:    options.Member,
	}
	if err := reporter.PublishPrimaryState(ctx, PrimaryState{
		ClaimRole:   true,
		Epoch:       result.Epoch,
		Observation: observation,
	}); err != nil {
		return result, err
	}
	log.Info("promotion complete", "member", options.Member, "epoch", result.Epoch,
		"timeline", result.Timeline, "promoteLSN", result.PromoteLSN,
		"synchronousStandbyNames", result.SynchronousStandbyNames)
	return result, nil
}

// LeaseManagerFor builds this member's view of the promotion Lease, which is named after
// the PgInstance.
func LeaseManagerFor(options Options) *LeaseManager {
	return &LeaseManager{
		Client:    options.Client,
		Namespace: options.Namespace,
		Name:      options.Instance,
		Holder:    options.Member,
		Config:    LeaseConfigOf(options.Config),
	}
}

// LeaseConfigOf reads the four durations out of the operator's configuration document,
// falling back to the validated defaults for any the operator did not set.
func LeaseConfigOf(config provision.AgentConfig) ha.LeaseConfig {
	resolved := ha.DefaultLeaseConfig()
	if config.Lease.LeaseDuration.Duration > 0 {
		resolved.LeaseDuration = config.Lease.LeaseDuration.Duration
	}
	if config.Lease.RenewDeadline.Duration > 0 {
		resolved.RenewDeadline = config.Lease.RenewDeadline.Duration
	}
	if config.Lease.RetryPeriod.Duration > 0 {
		resolved.RetryPeriod = config.Lease.RetryPeriod.Duration
	}
	if config.Lease.ReleasedLeaseDuration.Duration > 0 {
		resolved.ReleasedLeaseDuration = config.Lease.ReleasedLeaseDuration.Duration
	}
	return resolved
}

func reReadInstance(ctx context.Context, options Options) (*pgelasticv1alpha1.PgInstance, error) {
	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: options.Namespace, Name: options.Instance}
	if err := options.Client.Get(ctx, key, instance); err != nil {
		return nil, err
	}
	if instance.Status.TargetPrimary != options.Member {
		return nil, fmt.Errorf(
			"targetPrimary is %q, not %s: the decision that triggered this promotion has been withdrawn",
			instance.Status.TargetPrimary, options.Member)
	}
	return instance, nil
}

// verifyQuorum re-runs the R + W > N gate against evidence read again from the API server
// and against peers asked again over their own endpoints.
//
// Re-verifying rather than trusting the value that triggered the reconcile is the whole
// point: acquiring the Lease can take up to leaseDuration, and a standby that was reachable
// when the operator decided may not be reachable by the time the decision is acted on.
func verifyQuorum(ctx context.Context, options Options, instance *pgelasticv1alpha1.PgInstance) error {
	if highAvailability := instance.Spec.HighAvailability; highAvailability != nil &&
		highAvailability.FailoverQuorum != nil && !*highAvailability.FailoverQuorum {
		return nil
	}
	evidence := ha.EvidenceFrom(instance.Status.QuorumEvidence)
	reference := time.Now()
	if failing := instance.Status.CurrentPrimaryFailingSince; failing != nil {
		reference = failing.Time
	}
	verdict := ha.EvaluateQuorum(evidence, surveyReachable(ctx, options, evidence.VotingMembers), reference)
	if !verdict.Satisfied {
		return fmt.Errorf("%w: %s (%s)", ErrQuorumDenied, verdict.Message, verdict.Reason)
	}
	return nil
}

// surveyReachable asks each voter directly, over the headless Service's per-pod DNS record
// rather than through any load-balanced Service - which is exactly the thing that stops
// resolving correctly during the partition being diagnosed.
func surveyReachable(ctx context.Context, options Options, voters []string) []string {
	var reachable []string
	for _, voter := range voters {
		if voter == options.Member {
			reachable = append(reachable, voter)
			continue
		}
		endpoint := StatusEndpoint(voter, options)
		report, err := provision.FetchMemberReport(ctx, endpoint)
		if err != nil || !report.Healthy {
			continue
		}
		reachable = append(reachable, voter)
	}
	return reachable
}

// promoteLocal is pg_ctl promote plus the standby.signal check, and nothing else. It is
// unexported because a promotion that can be triggered on its own is a promotion that can
// happen without the quorum gate.
func promoteLocal(ctx context.Context, options Options, tools pgtool.Toolchain) error {
	log := logf.FromContext(ctx)
	if err := tools.Promote(ctx, promoteTimeout); err != nil {
		return err
	}

	deadline := time.Now().Add(promoteTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(options.DataDir, StandbySignal)); os.IsNotExist(err) {
			log.Info("standby.signal is gone", "member", options.Member)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("standby.signal still exists %s after promoting %s", promoteTimeout, options.Member)
}

func readInRecovery(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var inRecovery bool
	err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery)
	return inRecovery, err
}

func awaitOutOfRecovery(ctx context.Context, conn *pgx.Conn) error {
	deadline := time.Now().Add(promoteTimeout)
	for time.Now().Before(deadline) {
		var inRecovery bool
		if err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err == nil && !inRecovery {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("pg_is_in_recovery() was still true %s after promoting", promoteTimeout)
}

// promotionStandbys is the new topology: every member of the instance except this one.
//
// The old primary is included even though it is currently down. Under dataDurability
// Required that means commits stall until one of the two reconnects, which is the declared
// contract and is surfaced as an alertable condition rather than left as a hang. Naming
// only the survivor would buy nothing - it stalls until that one connects either way - and
// would then need a second reload to re-add the old primary, which is the shrink-then-grow
// Patroni's ordering rule exists to avoid.
func promotionStandbys(options Options, instance *pgelasticv1alpha1.PgInstance) []string {
	replicas := options.Config.Replicas
	if replicas <= 0 {
		replicas = 3
	}
	standbys := make([]string, 0, replicas-1)
	for serial := int32(1); serial <= replicas; serial++ {
		member := provision.MemberName(instance.Name, serial)
		if member != options.Member {
			standbys = append(standbys, member)
		}
	}
	slices.Sort(standbys)
	return standbys
}

// ensureReplicationSlots creates the physical slots the other members stream from.
//
// Physical slots are not replicated, so a newly promoted primary has none: without this
// every standby's WAL receiver loops on "replication slot does not exist" and the instance
// never regains its quorum.
func ensureReplicationSlots(ctx context.Context, conn *pgx.Conn, standbys []string) error {
	for _, standby := range standbys {
		slot := provision.ReplicationSlotName(standby)
		const statement = `
			SELECT pg_create_physical_replication_slot($1, true)
			WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`
		if _, err := conn.Exec(ctx, statement, slot); err != nil {
			return fmt.Errorf("creating the replication slot %s: %w", slot, err)
		}
	}
	return nil
}

// writeSyncStandbyNames lands the new clause and reloads, before any write is accepted.
func writeSyncStandbyNames(ctx context.Context, options Options, tools pgtool.Toolchain, clause string) error {
	replication := CurrentReplicationConfig(options.DataDir)
	// A promoted member is no longer in recovery, so every trace of how it used to stream
	// goes with the role. Leaving primary_conninfo behind would have it try to follow a
	// primary that no longer exists the next time it starts.
	replication.Standby = false
	replication.PrimaryConnInfo = ""
	replication.PrimarySlotName = ""
	// The old primary's synchronized_standby_slots names slots that do not exist here, and
	// naming a slot that does not exist blocks logical decoding outright. The observe loop
	// puts it back the moment a standby starts streaming to this member.
	replication.SynchronizedStandbySlots = ""
	replication.SynchronousStandbyNames = clause
	if _, err := WriteConfig(options.Config, options.Member, replication, options.DataDir, nil); err != nil {
		return err
	}
	return tools.Reload(ctx)
}

// publishEpoch binds the new fence token into the postmaster.
//
// It is a separate reload from the clause above, and deliberately the later of the two. The
// epoch is the signal the proxy acts on to start sending writes here; publishing it before
// synchronous_standby_names has loaded would open exactly the undetectable window of
// local-only durability the ordering exists to close.
func publishEpoch(ctx context.Context, options Options, tools pgtool.Toolchain, epoch int64) error {
	config := options.Config
	config.Postgres.PrimaryEpoch = epoch
	if _, err := WriteConfig(config, options.Member,
		CurrentReplicationConfig(options.DataDir), options.DataDir, nil); err != nil {
		return err
	}
	return tools.Reload(ctx)
}

// TerminateStaleBackends severs every client and replication connection inherited from the
// previous epoch.
//
// Kubernetes never tears down established TCP connections, so a client that was talking to
// this member while it was a standby - or a walsender still streaming to a peer that has
// not noticed the failover - carries on as if nothing happened. This is the local half of
// the fence; the proxy's epoch check is the half that works under partition.
func TerminateStaleBackends(ctx context.Context, conn *pgx.Conn) (int, error) {
	const statement = `
		SELECT count(*) FROM (
			SELECT pg_terminate_backend(pid)
			  FROM pg_stat_activity
			 WHERE pid <> pg_backend_pid()
			   AND backend_type IN ('client backend', 'walsender')
		) AS terminated`
	var terminated int
	err := conn.QueryRow(ctx, statement).Scan(&terminated)
	return terminated, err
}

// CurrentReplicationConfig reads back the override.conf this member is already running
// with, so that rewriting the file for one reason does not silently discard the rest of it.
// The file is replaced wholesale by design; that only works if the whole of it is known.
func CurrentReplicationConfig(dataDir string) pgconf.ReplicationConfig {
	contents, err := os.ReadFile(filepath.Join(dataDir, pgconf.OverrideConfFile))
	if err != nil {
		return pgconf.ReplicationConfig{}
	}
	values := pgconf.ParseSettings(string(contents))
	return pgconf.ReplicationConfig{
		PrimaryConnInfo:          values["primary_conninfo"],
		PrimarySlotName:          values["primary_slot_name"],
		SynchronousStandbyNames:  values["synchronous_standby_names"],
		SynchronizedStandbySlots: values["synchronized_standby_slots"],
		RestoreCommand:           values["restore_command"],
	}
}
