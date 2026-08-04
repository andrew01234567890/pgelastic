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
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// observeInterval is how often the agent re-reads its own postmaster and republishes its
// member status while nothing is happening.
const observeInterval = 2 * time.Second

// handoverInterval is how often it does so while a role change is in flight.
//
// The steady-state interval is a bargain about API chatter: nothing is waiting on the answer, so
// two seconds costs nothing. During a handover that bargain inverts, because this loop is on the
// critical path three separate times - the outgoing primary noticing it must hand over, the
// successor noticing it must promote, and the successor publishing that it has - and every client
// of the instance is held at the proxy for the sum of them. Two seconds of politeness each way was
// most of an eight-second pause.
//
// A role change is a bounded, rare, explicitly-signalled window, so polling hard through it costs
// a few extra reads of one Pod's own postmaster and nothing else. It is not a shorter timeout and
// changes no deadline: everything that was waiting for evidence still waits for the same evidence,
// it simply learns of it sooner.
const handoverInterval = 250 * time.Millisecond

// localReadTimeout bounds one look at this member's own postmaster: connect, observe, converge
// the synchronous clause, read the collation contract.
//
// It is deliberately its own constant rather than the observe cadence it used to borrow. While
// the cadence was fixed the two were the same number by coincidence, and the coincidence reads
// like a rule - so the obvious tidy-up, making the timeout track the chosen cadence, would give
// this work 250ms during exactly the handover it must not be starved in. observe() returning
// early skips reconcileRole, and a primary that skips reconcileRole stops renewing its lease.
const localReadTimeout = 2 * time.Second

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
	// CommandSwitchover stops the postmaster because the control plane is handing the role
	// over at a moment it chose. It ends the same way a fence does - this member rejoins as
	// a standby - and differs in exactly one thing, which is the whole point: the stop is
	// clean, so the rewind that follows has the shutdown checkpoint it needs and the next
	// start is not crash recovery.
	CommandSwitchover
	// CommandRejoin stops the postmaster, takes this member's history back onto the
	// primary's, and starts it again. It is a rejoin in place rather than an exit because
	// the alternative loses the status endpoint for the whole of a rewind - and for the
	// whole of a re-clone, which is minutes to hours - leaving the operator with silence at
	// exactly the moment the instance is at reduced redundancy.
	CommandRejoin
)

// postmasterOutcome is what the supervisor must do once one postmaster lifetime has ended.
type postmasterOutcome int

const (
	// outcomeStop is the end of the agent: the kubelet is asking, or the postmaster exited
	// on its own and restart_after_crash is off precisely so that becomes a visible
	// Kubernetes event rather than a silent self-heal.
	outcomeStop postmasterOutcome = iota
	// outcomeRestart is an in-place restart for a parameter that needs one.
	outcomeRestart
	// outcomeRejoin is a rewind or a re-clone, then a start.
	outcomeRejoin
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
	// roleChanging is whether the CR says a handover is in flight. It selects the observe
	// cadence and nothing else: it makes the loop look sooner, never act differently.
	roleChanging bool

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
	// session identifies this agent process, and dies with it. It is what lets the operator
	// tell a backup still being taken from one whose taker no longer exists.
	session string
	// backingUp admits one backup at a time. A backup outlives by minutes to hours the
	// observe tick that started it, so without this every tick would start another.
	backingUp bool
	// heldPosition is the last position this member was seen holding, and strandedSince is
	// when it stopped moving while a primary that is not this member existed. Divergence is
	// only ever evaluated once that has held for divergenceGrace, because a standby between
	// reconnections looks identical to one that can never reconnect until it has had time to
	// prove otherwise.
	heldPosition  ha.TimelinePosition
	strandedSince time.Time
	// rejoinPrimary is the member a queued rejoin must take this one back onto.
	rejoinPrimary string
	// databases reads pg_stat_database for the tenants this member holds, on a cadence of
	// its own rather than the observe tick's.
	databases *DatabaseScraper
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
		session:      NewSession(),
		databases: &DatabaseScraper{
			SocketDir: options.SocketDir,
			Port:      provision.PostgresPort,
			Password:  options.OpsPassword,
		},
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
		outcome, err := s.runPostmaster(ctx, signals)
		if err != nil {
			return err
		}
		if outcome == outcomeStop || ctx.Err() != nil {
			// restart_after_crash is off precisely so that a crashed postmaster becomes a
			// visible Kubernetes event rather than a silent self-heal. The agent honours
			// that: it does not respawn on its own, it exits and lets the kubelet record
			// the restart.
			return nil
		}
		if outcome == outcomeRejoin {
			if err := s.runRejoin(ctx); err != nil {
				return err
			}
			continue
		}
		log.Info("restarting the postmaster in place")
	}
}

// rejoinAttempts and rejoinRetryDelay bound how long a rejoin keeps trying before the agent
// gives up and lets the kubelet restart the container.
//
// A handful of attempts is worth having because the commonest reason a rejoin fails is that
// the primary is momentarily unreachable - which, in the middle of a failover, is exactly
// when a rejoin starts. Retrying forever is not: a member that can never rebuild has to
// become a Pod restart somebody notices rather than a status field somebody has to read.
const (
	rejoinAttempts   = 3
	rejoinRetryDelay = 10 * time.Second
)

// runRejoin takes this member's data directory back onto the primary's history.
//
// The probe state is published before the first byte moves, and cleared only once the whole
// thing is over. That is what makes the design's "a member silently re-cloning for ten
// minutes must be visible" true: the startup probe stops fighting a rewind that outlives
// every kubelet deadline, and the operator sees a member that is rebuilding rather than a
// member that is merely not ready.
func (s *Supervisor) runRejoin(ctx context.Context) error {
	log := logf.FromContext(ctx)
	primary := s.rejoinTarget()
	if primary == "" {
		return nil
	}
	s.noteRejoin(ctx, RejoinRewinding)
	defer s.noteRejoin(ctx, "")

	var lastErr error
	for attempt := 1; attempt <= rejoinAttempts; attempt++ {
		log.Info("rejoining the instance",
			"primary", primary, "member", s.options.Member, "attempt", attempt)
		lastErr = Rejoin(ctx, s.options, s.tools, primary, func(method RejoinMethod) {
			s.noteRejoin(ctx, method)
		})
		if lastErr == nil {
			s.clearStranded()
			log.Info("rejoined the instance", "primary", primary, "member", s.options.Member)
			return nil
		}
		log.Error(lastErr, "the rejoin did not finish", "primary", primary, "attempt", attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rejoinRetryDelay):
		}
	}
	return fmt.Errorf("rejoining %s: %w", primary, lastErr)
}

// runPostmaster spawns one postmaster and services the inner select until it exits.
func (s *Supervisor) runPostmaster(ctx context.Context, signals chan os.Signal) (postmasterOutcome, error) {
	log := logf.FromContext(ctx)

	command := exec.Command(s.postgresBinary(), "-D", s.options.DataDir)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return outcomeStop, fmt.Errorf("starting the postmaster: %w", err)
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

	// A timer rather than a ticker, so the cadence can be chosen again after every observation
	// rather than fixed when the loop starts.
	timer := time.NewTimer(observeInterval)
	defer timer.Stop()

	for {
		select {
		case err := <-exited:
			log.Info("postmaster exited", "error", err)
			return outcomeStop, nil
		case <-ctx.Done():
			s.stop(context.WithoutCancel(ctx), CauseKubelet)
			<-exited
			return outcomeStop, nil
		case received := <-signals:
			if received == syscall.SIGCHLD {
				s.reap(ctx)
				continue
			}
			s.stop(context.WithoutCancel(ctx), CauseKubelet)
			<-exited
			return outcomeStop, nil
		case command := <-s.commands:
			switch command {
			case CommandFence:
				s.stop(context.WithoutCancel(ctx), CauseFence)
				<-exited
				return outcomeStop, nil
			case CommandSwitchover:
				s.stop(context.WithoutCancel(ctx), CauseSwitchover)
				<-exited
				s.releasePrimaryLease(context.WithoutCancel(ctx))
				return outcomeStop, nil
			case CommandRejoin:
				s.stop(context.WithoutCancel(ctx), CauseSwitchover)
				<-exited
				return outcomeRejoin, nil
			case CommandRestart:
				s.stop(ctx, CauseSwitchover)
				<-exited
				return outcomeRestart, nil
			}
		case <-timer.C:
			s.observe(ctx)
			timer.Reset(s.observeCadence())
		}
	}
}

// observeCadence is how long to wait before looking again.
//
// Fast while this instance is in the middle of a role change, ordinary otherwise. The condition
// is the one the design already defines as the total signal for "a handover is in flight, freeze
// everything" - targetPrimary naming somebody other than the member currently holding the role -
// so this needs no new state and cannot disagree with the rest of the machinery about whether a
// handover is happening.
func (s *Supervisor) observeCadence() time.Duration {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.roleChanging {
		return handoverInterval
	}
	return observeInterval
}

// noteRoleChange records whether a role change is in flight, so the next cadence can be chosen
// from what the CR said rather than from a guess.
func (s *Supervisor) noteRoleChange(inFlight bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.roleChanging = inFlight
}

// roleChangeInFlight reads the total signal: targetPrimary naming somebody other than the member
// currently holding the role.
//
// An unreadable CR answers false. Not being able to read it proves nothing about whether a
// handover is happening, and the cost of being wrong in that direction is one ordinary tick.
func roleChangeInFlight(instance *pgelasticv1alpha1.PgInstance) bool {
	if instance == nil {
		return false
	}
	return ha.FailoverInProgress(instance.Status.CurrentPrimary, instance.Status.TargetPrimary)
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

// stop translates the cause into a shutdown plan and carries it out, taking the plan's
// second attempt once the first deadline passes.
func (s *Supervisor) stop(ctx context.Context, cause StopCause) {
	log := logf.FromContext(ctx)
	plan := TranslateStop(cause, s.ProbeState().Role, s.options.Timeouts)

	if plan.Checkpoint {
		s.checkpoint(ctx, plan.Timeout)
	}
	if err := s.tools.Stop(ctx, plan.Mode, plan.Timeout); err == nil {
		return
	}
	// A planned stop's second attempt is the same mode again rather than a harsher one, so
	// this is a retry rather than an escalation and the log says which it is.
	log.Info("the first shutdown attempt did not finish; taking the second",
		"cause", cause, "first", plan.Mode, "second", plan.EscalateTo,
		"escalating", plan.EscalateTo != plan.Mode)
	if err := s.tools.Stop(ctx, plan.EscalateTo, plan.EscalateTimeout); err != nil {
		log.Error(err, "the second shutdown attempt failed", "cause", cause)
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

// archiveObservation completes what PostgreSQL could not answer, and reports nothing at all
// for a member with no repository.
//
// The empty case is the load-bearing one. pg_stat_archiver records a successful archive
// whether or not anything was written anywhere: with no repository configured
// archive_command deliberately succeeds without pushing, so PostgreSQL would report a
// healthy archive for an instance whose WAL is going nowhere. Reporting nothing is the only
// answer that does not read as either a fault or a working archive.
func (s *Supervisor) archiveObservation(observation ArchiveObservation) ArchiveObservation {
	repository := s.options.Config.Backup
	if repository == nil || !repository.Configured() {
		return ArchiveObservation{}
	}
	observation.ReadyBacklog = ArchiveBacklog(s.options.WALDir)
	observation.LastFailureMessage = LastArchiveFailure(provision.ArchiveStatusFile, observation.LastFailedWAL)
	return observation
}

// scrapeDatabases reads pg_stat_database for the tenants on this member, at most once per
// the scraper's TTL however often the observe tick runs.
//
// A failure returns whatever the last successful scrape held rather than nothing. Losing the
// readings would move every tenant on this member into the operator's stale count, which
// refuses autoscaling for the pool - a heavier consequence than one round of counters being a
// few seconds old, and the wrong response to a query that timed out once.
func (s *Supervisor) scrapeDatabases(ctx context.Context) []provision.DatabaseReport {
	reports, fresh, err := s.databases.Scrape(ctx)
	if err != nil {
		logf.FromContext(ctx).V(1).Info("could not read pg_stat_database",
			"error", err, "servingLastReading", len(reports) > 0)
		return reports
	}
	if fresh {
		logf.FromContext(ctx).V(1).Info("read pg_stat_database", "databases", len(reports))
	}
	return reports
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
	// Measured on the same tick and from the same place, because the operator has no other
	// way to see inside a member's volumes and an autoscaler that cannot read usage cannot
	// act on it.
	dataUsage, dataUsageErr := MeasureVolume(s.options.DataDir)
	s.update(func(state *ProbeState) {
		state.LastPing = ping
		if usageErr == nil {
			state.WALVolumeFull = usage.Full()
			state.WALUsedBytes = usage.UsedBytes()
		}
		if dataUsageErr == nil {
			state.DataUsedBytes = dataUsage.UsedBytes()
		}
	})
	if ping != pgtool.PingOK {
		return
	}

	connectCtx, cancel := context.WithTimeout(ctx, localReadTimeout)
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
	if usageErr == nil {
		observation.WALUsedBytes = usage.UsedBytes()
	}
	if dataUsageErr == nil {
		observation.DataUsedBytes = dataUsage.UsedBytes()
	}
	observation.Archive = s.archiveObservation(observation.Archive)
	databases := s.scrapeDatabases(ctx)
	s.update(func(state *ProbeState) {
		state.ClientBackends = observation.ClientBackends
		state.Role = observation.Role
		state.ReplayLag = observation.ReplayLag
		state.Observation = observation
		state.Observed = true
		state.Databases = databases
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
	instance := s.fetchInstance(ctx)
	s.noteRoleChange(roleChangeInFlight(instance))
	s.requestRestartIfPending(ctx, observation, instance)
	s.report(ctx, observation)
	s.publishPrimaryState(ctx, instance, observation, contract)
	s.reconcileBackup(ctx, instance, observation)
	s.reconcileRole(ctx, observation, instance)
}

// requestRestartIfPending asks for one in-place restart per configuration, and only when
// the operator has said it is this member's turn.
//
// The restart requirement is read from pg_settings.pending_restart and paired with the
// configuration hash read back out of the same postmaster with current_setting(). Reading
// the hash from pg_show_all_file_settings() instead would let the two describe different
// instants, and a restart decided on a mismatched pair is a restart taken for a
// configuration nobody is running.
//
// The turn matters more than the pairing. One ConfigMap reaches all three members at once,
// so a member that restarts the moment it notices restarts alongside the other two, and an
// instance committing against "ANY 1" has exactly one member of slack to spend. That is the
// unordered restart the operator's rolling loop exists to replace, and this is the half of
// it that lives in the member: it acts only while named in the maintenance annotation.
func (s *Supervisor) requestRestartIfPending(
	ctx context.Context,
	observation MemberObservation,
	instance *pgelasticv1alpha1.PgInstance,
) {
	if !observation.PendingRestart || observation.ConfigSHA256 == "" || !s.restartPermitted(instance) {
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

// restartPermitted answers whether this member may stop its own postmaster for a parameter
// that needs it.
//
// Being named in the maintenance annotation is the operator saying this member is the one
// it is currently disrupting. A member that is not named waits, however stale its
// configuration is: the wait is what makes the restart a roll rather than a stampede.
//
// A member the operator is handing the role away from is excluded even while it is named,
// because the stop it is about to take belongs to the switchover. Restarting in place under
// it would bring the postmaster straight back as a primary the operator has already decided
// against, and the handover would then have to ask for the same stop a second time.
func (s *Supervisor) restartPermitted(instance *pgelasticv1alpha1.PgInstance) bool {
	if instance == nil || !ha.UnderMaintenance(instance.GetAnnotations(), s.options.Member) {
		return false
	}
	if instance.Status.CurrentPrimary != s.options.Member {
		return true
	}
	target := instance.Status.TargetPrimary
	return target == "" || target == ha.TargetPrimaryPending || target == s.options.Member
}

func (s *Supervisor) report(ctx context.Context, observation MemberObservation) {
	if s.options.Client == nil {
		return
	}
	if err := s.reporter().Report(ctx, observation, true, ""); err != nil {
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
	if observation.Archive.State != "" {
		state.Archive = &observation.Archive
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
		Session:   s.session,
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
