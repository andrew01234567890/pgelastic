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
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// observeInterval is how often the agent re-reads its own postmaster and republishes its
// member status.
const observeInterval = 2 * time.Second

// Command is an instruction delivered to the supervisor's inner loop from outside the
// signal path.
type Command int

const (
	// CommandRestart restarts the postmaster in place, without a Pod restart. That is the
	// whole reason the postmaster is a child process rather than something the agent exec'd
	// into: a Pod restart would also restart the agent, and with it every probe the control
	// plane is using to decide what to do next.
	CommandRestart Command = iota
	// CommandFence stops the postmaster because this member must stop being the primary:
	// it has provably lost the promotion Lease, or the operator has named somebody else. It
	// does not restart in place - the member has to rejoin, and rejoining begins with the
	// bootstrap path deciding between a rewind and a re-clone.
	CommandFence
)

// Options is everything the agent was told by the Pod it is running in.
type Options struct {
	Config      provision.AgentConfig
	Member      string
	Serial      int32
	Namespace   string
	Instance    string
	DataDir     string
	WALDir      string
	SocketDir   string
	LogDir      string
	BinDir      string
	StatusPort  int32
	PeerService string
	// ReplicationPassword authenticates this member's WAL receiver against the primary.
	ReplicationPassword string
	// OpsPassword authenticates the control plane's non-superuser role.
	OpsPassword string
	// RewindPassword authenticates pg_rewind against the member it is rewinding from.
	RewindPassword string
	// Client is the agent's API server connection, used to report member status.
	Client client.Client
	// Timeouts bound the shutdown ladder.
	Timeouts StopTimeouts
}

// Supervisor is PID 1. The postmaster is an ordinary child process: it is never exec'd
// into, because a process that has exec'd cannot supervise anything.
type Supervisor struct {
	options  Options
	tools    pgtool.Toolchain
	commands chan Command

	mutex sync.RWMutex
	state ProbeState
	// lease is this member's hold on the promotion Lease, created on first use because a
	// member that never becomes a primary never needs one.
	lease *LeaseManager
	// leaseUnverifiedSince records when renewals started failing to reach the API server.
	// It is reported rather than acted on: failing to verify the lease is not losing it.
	leaseUnverifiedSince time.Time
	// promoting admits one promotion at a time, because a promotion outlives the observe
	// tick that started it, and promotionRefusedAt paces the retries after a refusal.
	promoting          bool
	promotionRefusedAt time.Time
	// promotedEpoch is the fence token this member was promoted at, which every later
	// configuration rewrite has to keep publishing.
	promotedEpoch int64
	// postmasterPID is the child the reaper must never wait on.
	postmasterPID int
	// restartedFor records the configuration hashes an in-place restart has already been
	// spent on, so a parameter that reports pending_restart forever costs one restart
	// rather than an endless loop of them.
	restartedFor map[string]bool
}

// NewSupervisor builds the agent.
func NewSupervisor(options Options) *Supervisor {
	return &Supervisor{
		options:  options,
		commands: make(chan Command, 1),
		tools: pgtool.Toolchain{
			BinDir:  options.BinDir,
			DataDir: options.DataDir,
			WALDir:  options.WALDir,
			Stderr:  os.Stderr,
		},
		state:        ProbeState{Role: RoleUnknown},
		restartedFor: map[string]bool{},
	}
}

// ProbeState returns a snapshot for the status server.
func (s *Supervisor) ProbeState() ProbeState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

func (s *Supervisor) update(mutate func(*ProbeState)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	mutate(&s.state)
}

// Run is the outer loop: one iteration is one postmaster lifetime.
func (s *Supervisor) Run(ctx context.Context) error {
	log := logf.FromContext(ctx)

	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGCHLD)
	defer signal.Stop(signals)

	if err := s.prepareLogFIFO(); err != nil {
		return err
	}
	if err := EnsureIncludes(s.options.DataDir); err != nil {
		return err
	}
	// A container that restarted in place does not re-run its init containers, so the check
	// that keeps a demoted primary from starting as a primary again has to happen here too.
	if err := PrepareToFollow(ctx, s.options, s.tools); err != nil {
		return err
	}

	for {
		if err := s.writeConfig(ctx, nil); err != nil {
			return err
		}
		restart, err := s.runPostmaster(ctx, signals)
		if err != nil {
			return err
		}
		if !restart || ctx.Err() != nil {
			// restart_after_crash is off precisely so that a crashed postmaster becomes a
			// visible Kubernetes event rather than a silent self-heal. The agent honours
			// that: it does not respawn on its own, it exits and lets the kubelet record
			// the restart.
			return nil
		}
		log.Info("restarting the postmaster in place")
	}
}

// runPostmaster spawns one postmaster and services the inner select until it exits.
func (s *Supervisor) runPostmaster(ctx context.Context, signals chan os.Signal) (bool, error) {
	log := logf.FromContext(ctx)

	command := exec.Command(s.postgresBinary(), "-D", s.options.DataDir)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return false, fmt.Errorf("starting the postmaster: %w", err)
	}
	s.mutex.Lock()
	s.postmasterPID = command.Process.Pid
	s.state.CanCheck = true
	s.mutex.Unlock()
	log.Info("postmaster started", "pid", command.Process.Pid)

	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()

	defer s.update(func(state *ProbeState) {
		state.CanCheck = false
		state.Role = RoleUnknown
	})

	ticker := time.NewTicker(observeInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-exited:
			log.Info("postmaster exited", "error", err)
			return false, nil
		case <-ctx.Done():
			s.stop(context.WithoutCancel(ctx), CauseKubelet)
			<-exited
			return false, nil
		case received := <-signals:
			if received == syscall.SIGCHLD {
				s.reap(ctx)
				continue
			}
			s.stop(context.WithoutCancel(ctx), CauseKubelet)
			<-exited
			return false, nil
		case command := <-s.commands:
			if command == CommandFence {
				s.stop(context.WithoutCancel(ctx), CauseFence)
				<-exited
				return false, nil
			}
			s.stop(ctx, CauseSwitchover)
			<-exited
			return true, nil
		case <-ticker.C:
			s.observe(ctx)
		}
	}
}

func (s *Supervisor) postgresBinary() string {
	return filepath.Join(s.options.BinDir, postmasterExecutable)
}

// reap waits on the orphans this agent owns, and only those.
func (s *Supervisor) reap(ctx context.Context) {
	log := logf.FromContext(ctx)
	processes, err := ProcFS{}.Processes()
	if err != nil {
		return
	}
	s.mutex.RLock()
	postmaster := s.postmasterPID
	s.mutex.RUnlock()

	for _, pid := range SelectReapable(processes, postmaster) {
		var status syscall.WaitStatus
		reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if err == nil && reaped == pid {
			log.V(1).Info("reaped an orphaned postgres process", "pid", pid)
		}
	}
}

// stop translates the cause into a shutdown plan and carries it out, escalating once the
// first deadline passes.
func (s *Supervisor) stop(ctx context.Context, cause StopCause) {
	log := logf.FromContext(ctx)
	plan := TranslateStop(cause, s.ProbeState().Role, s.options.Timeouts)

	if plan.Checkpoint {
		s.checkpoint(ctx, plan.Timeout)
	}
	if err := s.tools.Stop(ctx, plan.Mode, plan.Timeout); err == nil {
		return
	}
	log.Info("escalating the shutdown", "from", plan.Mode, "to", plan.EscalateTo)
	if err := s.tools.Stop(ctx, plan.EscalateTo, plan.EscalateTimeout); err != nil {
		log.Error(err, "the escalated shutdown failed")
	}
}

func (s *Supervisor) checkpoint(ctx context.Context, timeout time.Duration) {
	log := logf.FromContext(ctx)
	checkpointCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := Connect(checkpointCtx, s.options.SocketDir, provision.PostgresPort)
	if err != nil {
		log.Error(err, "could not connect to checkpoint before shutting down")
		return
	}
	defer func() { _ = conn.Close(checkpointCtx) }()
	if err := Checkpoint(checkpointCtx, conn); err != nil {
		log.Error(err, "the pre-shutdown checkpoint failed")
	}
}

// observe re-reads the postmaster, converges the replication configuration, and
// republishes this member's status.
func (s *Supervisor) observe(ctx context.Context) {
	log := logf.FromContext(ctx)

	ping := s.tools.IsReady(ctx, s.options.SocketDir, provision.PostgresPort)
	// The WAL volume is measured from the filesystem rather than from PostgreSQL so that
	// the answer survives a postmaster that has already stopped, which is precisely the
	// state a candidate is in when the veto has to be evaluated.
	usage, usageErr := MeasureVolume(s.options.WALDir)
	s.update(func(state *ProbeState) {
		state.LastPing = ping
		if usageErr == nil {
			state.WALVolumeFull = usage.Full()
		}
	})
	if ping != pgtool.PingOK {
		return
	}

	connectCtx, cancel := context.WithTimeout(ctx, observeInterval)
	defer cancel()
	conn, err := Connect(connectCtx, s.options.SocketDir, provision.PostgresPort)
	if err != nil {
		log.V(1).Info("could not reach the local postmaster", "error", err)
		return
	}
	defer func() { _ = conn.Close(connectCtx) }()

	observation, err := Observe(connectCtx, conn)
	if err != nil {
		log.V(1).Info("could not observe the local postmaster", "error", err)
		return
	}
	observation.WALVolumeFull = usageErr == nil && usage.Full()
	s.update(func(state *ProbeState) {
		state.Role = observation.Role
		state.ReplayLag = observation.ReplayLag
		state.Observation = observation
		state.Observed = true
	})

	var contract *Contract
	if observation.Role == RolePrimary {
		if err := s.convergeSynchronousStandbys(connectCtx, observation); err != nil {
			log.Error(err, "could not converge synchronous_standby_names")
		}
		if read, err := CollationContract(connectCtx, conn); err == nil {
			contract = &read
		}
	}
	s.requestRestartIfPending(ctx, observation)
	instance := s.fetchInstance(ctx)
	s.report(ctx, observation)
	s.publishPrimaryState(ctx, instance, observation, contract)
	s.reconcileRole(ctx, observation, instance)
}

// requestRestartIfPending asks for one in-place restart per configuration.
//
// The restart requirement is read from pg_settings.pending_restart and paired with the
// configuration hash read back out of the same postmaster with current_setting(). Reading
// the hash from pg_show_all_file_settings() instead would let the two describe different
// instants, and a restart decided on a mismatched pair is a restart taken for a
// configuration nobody is running.
func (s *Supervisor) requestRestartIfPending(ctx context.Context, observation MemberObservation) {
	if !observation.PendingRestart || observation.ConfigSHA256 == "" {
		return
	}
	s.mutex.Lock()
	already := s.restartedFor[observation.ConfigSHA256]
	s.restartedFor[observation.ConfigSHA256] = true
	s.mutex.Unlock()
	if already {
		return
	}
	logf.FromContext(ctx).Info("a parameter needs a restart", "configHash", observation.ConfigSHA256)
	select {
	case s.commands <- CommandRestart:
	default:
	}
}

func (s *Supervisor) report(ctx context.Context, observation MemberObservation) {
	if s.options.Client == nil {
		return
	}
	if err := s.reporter().Report(ctx, observation, true); err != nil {
		logf.FromContext(ctx).Error(err, "could not report member status")
	}
}

// publishPrimaryState refreshes the instance-wide record the quorum gate is evaluated
// against. Only the member holding the role writes it, and it claims the role itself only
// when nobody holds it - which is the bootstrap case, where the first member has to publish
// itself so the others have something to clone from.
func (s *Supervisor) publishPrimaryState(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	observation MemberObservation,
	contract *Contract,
) {
	if s.options.Client == nil || instance == nil || observation.Role != RolePrimary {
		return
	}
	if current := instance.Status.CurrentPrimary; current != "" && current != s.options.Member {
		return
	}
	state := PrimaryState{
		ClaimRole:   instance.Status.CurrentPrimary == "",
		Epoch:       s.publishedEpoch(),
		Observation: observation,
		Contract:    contract,
	}
	if err := s.reporter().PublishPrimaryState(ctx, state); err != nil {
		logf.FromContext(ctx).Error(err, "could not publish the primary's view of the instance")
	}
}

func (s *Supervisor) reporter() Reporter {
	return Reporter{
		Client:    s.options.Client,
		Namespace: s.options.Namespace,
		Instance:  s.options.Instance,
		Member:    s.options.Member,
	}
}

// convergeSynchronousStandbys grows the quorum set as standbys start streaming.
//
// Patroni's ordering rule is the only published correct answer: increase the quorum set
// before numsync, decrease numsync before the quorum set, never both in one step. With a
// fixed "ANY 1" the growing direction is all that is exercised at bootstrap, and it is the
// direction that matters: naming a standby that has not connected yet would block every
// commit under dataDurability Required, bootstrap included.
//
// The shrinking direction is where the dangerous mistake lives. Under Required, dropping a
// standby out of the clause the moment it stops streaming would silently convert a stalled
// commit into an asynchronous one - the instance would keep serving writes nobody has
// acknowledged, which is exactly the durability the setting was chosen to buy. So under
// Required the set only ever grows.
func (s *Supervisor) convergeSynchronousStandbys(ctx context.Context, observation MemberObservation) error {
	members := s.memberNames()
	desiredMembers := ConvergeSyncMembers(observation.VotingMembers, observation.StreamingStandbys,
		members, s.durabilityRequired())
	desired := SynchronousStandbyNames(s.options.Config.Quorum, desiredMembers)
	if desired == observation.SyncStandbyNames {
		return nil
	}
	slots := make([]string, 0, len(observation.StreamingStandbys))
	for _, standby := range observation.StreamingStandbys {
		slots = append(slots, provision.ReplicationSlotName(standby))
	}
	replication := CurrentReplicationConfig(s.options.DataDir)
	replication.SynchronousStandbyNames = desired
	// Without synchronized_standby_slots a subscriber can consume changes a standby has
	// not flushed. After a promotion the synced slot is then behind the subscriber, and
	// the migration either errors on missing WAL or silently loses rows - which is why it
	// is set here, alongside the quorum set, rather than left for the migration to set.
	replication.SynchronizedStandbySlots = strings.Join(slots, ",")

	if err := s.writeConfig(ctx, &replication); err != nil {
		return err
	}
	return s.tools.Reload(ctx)
}

// memberNames is every member name this instance is allowed to have, which is what bounds
// the growing quorum set: a member the operator has retired stops being a legitimate voter
// even though the loaded clause still names it.
func (s *Supervisor) memberNames() []string {
	names := make([]string, 0, s.options.Config.Replicas)
	for serial := int32(1); serial <= s.options.Config.Replicas; serial++ {
		names = append(names, provision.MemberName(s.options.Instance, serial))
	}
	return names
}

func (s *Supervisor) durabilityRequired() bool {
	return s.options.Config.DataDurability != string(pgelasticv1alpha1.DataDurabilityPreferred)
}

// writeConfig renders the configuration files, raising the five enforced parameters to the
// floor this member's own control file reports.
func (s *Supervisor) writeConfig(ctx context.Context, replication *pgconf.ReplicationConfig) error {
	current := CurrentReplicationConfig(s.options.DataDir)
	if replication != nil {
		current = *replication
	}
	var controlData *pgtool.ControlData
	if data, err := s.tools.ControlData(ctx, s.options.DataDir); err == nil {
		controlData = &data
	}
	config := s.options.Config
	config.Postgres.PrimaryEpoch = s.publishedEpoch()
	_, err := WriteConfig(config, s.options.Member, current, s.options.DataDir, controlData)
	return err
}

// prepareLogFIFO creates the FIFO the logging collector writes into, and starts draining
// it, before any postmaster exists.
//
// The read end is opened O_RDWR on purpose: a FIFO whose only reader closes gives the
// syslogger EPIPE, and holding a write descriptor of our own means the pipe never reaches
// end of file however the drain goroutine happens to be scheduled.
func (s *Supervisor) prepareLogFIFO() error {
	if err := os.MkdirAll(s.options.LogDir, 0o750); err != nil {
		return err
	}
	path := filepath.Join(s.options.LogDir, provision.LogFIFOName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		return err
	}
	fifo, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	go func() {
		defer func() { _ = fifo.Close() }()
		buffer := make([]byte, 32*1024)
		for {
			read, err := fifo.Read(buffer)
			if read > 0 {
				_, _ = os.Stdout.Write(buffer[:read])
			}
			if err != nil {
				return
			}
		}
	}()
	return nil
}

// WaitForPostmaster blocks until the postmaster answers or the context expires. It is used
// by the bootstrap path, where there is a well-defined moment at which the temporary
// postmaster is ready for the bootstrap SQL.
func WaitForPostmaster(ctx context.Context, socketDir string, port int32) (*pgx.Conn, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, err := pgx.Connect(ctx, LocalDSN(socketDir, port, "postgres"))
		if err == nil {
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), err)
		case <-ticker.C:
		}
	}
}
