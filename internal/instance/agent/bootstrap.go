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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// bootstrapSocketDir is a private directory nothing outside this pod can reach. The
// temporary postmaster that runs the bootstrap SQL listens on it and on no TCP address at
// all, so no client can ever see a half-configured instance.
const bootstrapSocketDir = "/tmp/pgelastic-bootstrap"

// dataDirMode is the mode PostgreSQL insists PGDATA has.
const dataDirMode os.FileMode = 0o700

// Bootstrap brings a member's data directory into existence, exactly once.
//
// A directory that already holds a control file is adopted rather than rebuilt: this runs
// in an init container, so it runs again on every Pod restart, and rebuilding would mean
// a kubelet restart silently destroyed a member's data.
func Bootstrap(ctx context.Context, options Options) error {
	log := logf.FromContext(ctx)
	tools := toolchain(options)

	if pgtool.DataDirectoryInitialised(options.DataDir) {
		if _, err := tools.ControlData(ctx, options.DataDir); err == nil {
			log.Info("adopting an existing data directory", "dataDir", options.DataDir)
			return PrepareToFollow(ctx, options, tools)
		}
	}

	primary, err := designatedPrimary(ctx, options)
	if err != nil {
		return err
	}
	if primary == options.Member {
		return initialise(ctx, options, tools)
	}
	return join(ctx, options, tools, primary)
}

// PrepareToFollow makes an existing data directory consistent with whoever the instance's
// primary is now, before any postmaster is started.
//
// It runs from the init container and again from the agent's own start-up, because those
// are two different events: a Pod recreated after a node loss re-runs its init containers,
// while a container that merely restarted in place does not. A member that came back as a
// primary after somebody else was promoted, and started its postmaster anyway, is the split
// brain the whole design exists to prevent.
func PrepareToFollow(ctx context.Context, options Options, tools pgtool.Toolchain) error {
	log := logf.FromContext(ctx)
	if options.Client == nil || !pgtool.DataDirectoryInitialised(options.DataDir) {
		return nil
	}
	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: options.Namespace, Name: options.Instance}
	if err := options.Client.Get(ctx, key, instance); err != nil {
		// Not knowing who the primary is, is not evidence that it is somebody else. The
		// startup probe keeps this member out of every Service until it can answer, and the
		// agent's own loop fences it the moment the API server comes back and disagrees.
		log.Info("could not read the instance; leaving the data directory alone", "error", err.Error())
		return nil
	}

	_, inRecovery := os.Stat(filepath.Join(options.DataDir, StandbySignal))
	holder, err := LeaseManagerFor(options).Snapshot(ctx)
	if err != nil {
		log.Info("could not read the promotion lease", "error", err.Error())
	}

	action := ha.StartupDecision(options.Member, inRecovery == nil,
		instance.Status.CurrentPrimary, instance.Status.TargetPrimary, holder.Holder)
	if action.Follow == "" {
		return nil
	}
	if !action.Rejoin {
		action = escalateDivergedStandby(ctx, options, tools, instance, action)
	}

	host := PeerHost(action.Follow, options.PeerService, options.Namespace)
	if !action.Rejoin {
		return followPrimary(options, host)
	}
	log.Info("this member cannot follow the primary as it stands; rejoining",
		"member", options.Member, "primary", action.Follow, "reason", action.Reason)
	return Rejoin(ctx, options, tools, action.Follow, nil)
}

// escalateDivergedStandby turns "repoint at the new primary" into "rewind or re-clone" for
// a standby whose own history has diverged.
//
// Being in recovery used to be treated as proof that a member's history was intact, on the
// grounds that it had only ever received WAL. That is false: a standby that received WAL
// past the point the primary's history forked at holds records nothing else has, and
// repointing it produces a member that asks to stream, is refused, and asks again forever.
// The evidence is read from the stopped data directory and the newest timeline history file
// the member fetched from the primary, so no postmaster has to be started to find out.
func escalateDivergedStandby(
	ctx context.Context,
	options Options,
	tools pgtool.Toolchain,
	instance *pgelasticv1alpha1.PgInstance,
	action ha.StartupAction,
) ha.StartupAction {
	log := logf.FromContext(ctx)
	data, err := tools.ControlData(ctx, options.DataDir)
	if err != nil {
		log.Info("could not read the control file; leaving the data directory alone", "error", err.Error())
		return action
	}
	divergence, err := DetectDivergence(options.WALDir, StoppedPosition(data),
		PrimaryTimeline(instance, action.Follow))
	if err != nil {
		log.Info("could not read the timeline history", "error", err.Error())
		return action
	}
	if !divergence.Diverged {
		return action
	}
	log.Info("this member's history has diverged from the primary's",
		"member", options.Member, "primary", action.Follow,
		"reason", divergence.Reason, "detail", divergence.Message)
	action.Rejoin = true
	action.Reason = divergence.Reason
	return action
}

// designatedPrimary decides what this member is bootstrapping as.
//
// The answer is read from the CR rather than passed in as an argument, because a Pod
// created for serial 1 is not necessarily the primary: after a failover, serial 1 is a
// standby that has to clone from whoever was promoted. Only status.currentPrimary knows.
func designatedPrimary(ctx context.Context, options Options) (string, error) {
	if options.Client == nil {
		return provision.MemberName(options.Instance, 1), nil
	}
	instance := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: options.Namespace, Name: options.Instance}
	if err := options.Client.Get(ctx, key, instance); err != nil {
		return "", err
	}
	if instance.Status.CurrentPrimary != "" {
		return instance.Status.CurrentPrimary, nil
	}
	first := provision.MemberName(options.Instance, 1)
	if options.Member != first {
		return "", fmt.Errorf(
			"no primary has been elected yet; %s cannot clone until one exists", options.Member)
	}
	return first, nil
}

// initialise runs initdb with every flag pinned, then the bootstrap SQL against a
// temporary postmaster that is not reachable over TCP at all.
func initialise(ctx context.Context, options Options, tools pgtool.Toolchain) error {
	log := logf.FromContext(ctx)
	if err := quarantine(ctx, options, tools); err != nil {
		return err
	}
	if err := os.MkdirAll(options.DataDir, dataDirMode); err != nil {
		return err
	}
	if err := os.Chmod(options.DataDir, dataDirMode); err != nil {
		return err
	}
	log.Info("running initdb", "dataDir", options.DataDir, "walDir", options.WALDir)
	if err := tools.InitDB(ctx, pgtool.DefaultInitDBOptions()); err != nil {
		return err
	}
	if err := EnsureIncludes(options.DataDir); err != nil {
		return err
	}
	if _, err := WriteConfig(options.Config, options.Member,
		pgconf.ReplicationConfig{}, options.DataDir, nil); err != nil {
		return err
	}
	return runBootstrapSQL(ctx, options)
}

// runBootstrapSQL starts a private postmaster, creates the two non-superuser roles, and
// stops it again.
func runBootstrapSQL(ctx context.Context, options Options) error {
	log := logf.FromContext(ctx)
	if err := os.MkdirAll(bootstrapSocketDir, 0o700); err != nil {
		return err
	}

	// Every override here is deliberate. listen_addresses is empty so nothing can reach
	// this postmaster over the network; archive_mode is off so a half-built instance
	// cannot push a segment into an archive; the logging collector is off because the FIFO
	// it writes into belongs to the long-running agent, not to this one.
	command := exec.Command(filepath.Join(options.BinDir, postmasterExecutable),
		"-D", options.DataDir,
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories="+bootstrapSocketDir,
		"-c", "archive_mode=off",
		"-c", "logging_collector=off",
		"-c", "log_destination=stderr",
		"-c", "synchronous_standby_names=",
	)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return err
	}
	defer func() {
		_ = command.Process.Signal(os.Interrupt)
		_ = command.Wait()
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	conn, err := WaitForPostmaster(waitCtx, bootstrapSocketDir, provision.PostgresPort)
	if err != nil {
		return fmt.Errorf("the bootstrap postmaster never accepted a connection: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	log.Info("creating the replication and ops roles")
	return BootstrapRoles(ctx, conn, options.ReplicationPassword, options.OpsPassword, options.RewindPassword)
}

// join clones this member from the current primary.
func join(ctx context.Context, options Options, tools pgtool.Toolchain, primary string) error {
	log := logf.FromContext(ctx)
	if err := quarantine(ctx, options, tools); err != nil {
		return err
	}

	host := PeerHost(primary, options.PeerService, options.Namespace)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for !DialPeer(waitCtx, host, provision.PostgresPort) {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("the primary %s never accepted a connection", host)
		case <-time.After(time.Second):
		}
	}

	slot := provision.ReplicationSlotName(options.Member)
	log.Info("cloning from the primary", "primary", primary, "slot", slot)
	tools.Stdout = os.Stdout
	if err := cloneWithSlot(ctx, options, tools, host, slot); err != nil {
		return err
	}

	replication := pgconf.ReplicationConfig{
		Standby:         true,
		PrimaryConnInfo: PrimaryConnInfo(host, options.Member, options.ReplicationPassword),
		PrimarySlotName: slot,
	}
	if err := EnsureIncludes(options.DataDir); err != nil {
		return err
	}
	if _, err := WriteConfig(options.Config, options.Member,
		replication, options.DataDir, nil); err != nil {
		return err
	}
	return SetStandbySignal(options.DataDir, true)
}

// cloneWithSlot takes the base backup, creating the replication slot as part of it when the
// primary does not already have one for this member.
//
// Both cases are real. A member joining a brand new instance needs the slot created; a
// member re-cloning after a failover finds one waiting, because the promotion creates a slot
// for every other member before it accepts any write. pg_basebackup refuses to create a slot
// that already exists, so trying and falling back is what covers both without asking the
// joining member to hold a credential that can create slots on somebody else's server.
func cloneWithSlot(
	ctx context.Context,
	options Options,
	tools pgtool.Toolchain,
	host, slot string,
) error {
	clone := func(createSlot bool) error {
		return runWithPassword(ctx, options.ReplicationPassword, func(ctx context.Context) error {
			return tools.BaseBackup(ctx, pgtool.BaseBackupOptions{
				Host:       host,
				Port:       provision.PostgresPort,
				User:       provision.ReplicationRole,
				SlotName:   slot,
				CreateSlot: createSlot,
			})
		})
	}
	err := clone(true)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		logf.FromContext(ctx).Info("the primary already holds this member's slot", "slot", slot)
		return clone(false)
	}
	return err
}

// PeerHost is a member's stable per-pod DNS name under the headless Service.
func PeerHost(member, peerService, namespace string) string {
	return fmt.Sprintf("%s.%s.%s.svc", member, peerService, namespace)
}

// PrimaryConnInfo builds a standby's connection string.
//
// dbname is present and not optional: slot synchronisation errors out without it, and the
// failure surfaces only once a failover needs the synchronised slot that was never kept up
// to date. application_name is the member name because that is what
// synchronous_standby_names names.
func PrimaryConnInfo(host, member, password string) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s application_name=%s dbname=postgres",
		host, provision.PostgresPort, provision.ReplicationRole, password, member)
}

// quarantine moves a pre-existing data directory aside instead of deleting it.
//
// Renaming rather than deleting is the entire point. A directory that parses as PostgreSQL
// data is somebody's database until proven otherwise; the cost of keeping it is disk, and
// the cost of being wrong about deleting it is unbounded. A directory that does not parse
// is moved aside too, because "I could not read it" is not evidence that it was worthless.
func quarantine(ctx context.Context, options Options, tools pgtool.Toolchain) error {
	log := logf.FromContext(ctx)
	suffix := pgtool.QuarantineSuffix(time.Now())

	for _, directory := range []string{options.DataDir, options.WALDir} {
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			continue
		}
		if directory == options.DataDir {
			if data, err := tools.ControlData(ctx, directory); err == nil {
				log.Info("a parseable data directory is already here; moving it aside",
					"systemIdentifier", data.SystemIdentifier, "state", data.ClusterState)
			}
		}
		if err := os.Rename(directory, directory+suffix); err != nil {
			return fmt.Errorf("moving %s aside: %w", directory, err)
		}
		log.Info("quarantined a pre-existing directory", "from", directory, "to", directory+suffix)
	}
	return nil
}

// runWithPassword scopes PGPASSWORD to one operation. Passing it on the command line would
// put the replication password into every process listing in the pod.
func runWithPassword(ctx context.Context, password string, operation func(context.Context) error) error {
	previous, had := os.LookupEnv("PGPASSWORD")
	if err := os.Setenv("PGPASSWORD", password); err != nil {
		return err
	}
	defer func() {
		if had {
			_ = os.Setenv("PGPASSWORD", previous)
			return
		}
		_ = os.Unsetenv("PGPASSWORD")
	}()
	return operation(ctx)
}

func toolchain(options Options) pgtool.Toolchain {
	return pgtool.Toolchain{
		BinDir:  options.BinDir,
		DataDir: options.DataDir,
		WALDir:  options.WALDir,
		Stderr:  os.Stderr,
	}
}
