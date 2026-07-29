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

// Command pgelastic-instance is the instance manager. It is PID 1 in every Postgres pod,
// with the postmaster as an ordinary child process.
//
// One static binary with subcommands, copied into the pod by an init container that does
// nothing but cp. That indirection is what lets the agent be upgraded without rebuilding
// the PostgreSQL image, and what lets the same code serve as archive_command and
// restore_command without a second artefact to keep in step.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/agent"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const usage = `pgelastic-instance — the pgelastic instance manager

  run          supervise the postmaster as PID 1 and serve the three probes
  bootstrap    create this member's data directory, by initdb or by cloning the primary
  initdb       create a new data directory with every flag pinned
  join         clone this member from the current primary
  promote      promote this member out of recovery
  status       print what this member's own status endpoint reports about it
  wal-archive  archive one WAL segment (archive_command)
  wal-restore  restore one WAL segment (restore_command)
`

func main() {
	if code := run(os.Args[1:]); code != 0 {
		os.Exit(code)
	}
}

func run(args []string) int {
	logf.SetLogger(zap.New(zap.UseDevMode(false)))
	log := logf.Log.WithName("instance")

	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx = logf.IntoContext(ctx, log)

	var err error
	switch args[0] {
	case "run":
		// The supervisor installs its own signal handling, because translating a signal
		// into a PostgreSQL shutdown mode is the job, not merely cancelling a context.
		stop()
		err = runAgent(logf.IntoContext(context.Background(), log))
	case "bootstrap":
		err = runBootstrap(ctx)
	case "initdb", "join":
		// Both are reachable through bootstrap, which decides between them by reading the
		// CR. They stay addressable on their own for operational recovery.
		err = runBootstrap(ctx)
	case "promote":
		err = runPromote(ctx)
	case "status":
		err = runStatus(ctx, os.Stdout)
	case "wal-archive":
		err = runWALArchive(ctx, args[1:])
	case "wal-restore":
		err = runWALRestore(ctx, args[1:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	if err != nil {
		log.Error(err, "the instance manager failed", "subcommand", args[0])
		return 1
	}
	return 0
}

// options assembles the agent's configuration from the environment the operator stamped
// into the Pod.
func options(ctx context.Context, needClient bool) (agent.Options, error) {
	configPath := envOr(provision.EnvConfigFile, provision.ConfigMountPath+"/"+provision.ConfigFileName)
	config, err := agent.LoadAgentConfig(configPath)
	if err != nil {
		return agent.Options{}, fmt.Errorf("reading %s: %w", configPath, err)
	}

	statusPort := provision.StatusPort
	if raw := os.Getenv(provision.EnvStatusPort); raw != "" {
		parsed, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil {
			return agent.Options{}, parseErr
		}
		statusPort = int32(parsed)
	}

	built := agent.Options{
		Config:              config,
		Member:              os.Getenv(provision.EnvMember),
		Serial:              agent.SerialFromEnv(os.Getenv(provision.EnvSerial)),
		Namespace:           envOr(provision.EnvNamespace, config.Namespace),
		Instance:            envOr(provision.EnvInstance, config.Instance),
		DataDir:             envOr(provision.EnvDataDir, provision.DataDir),
		WALDir:              envOr(provision.EnvWALDir, provision.WALDir),
		SocketDir:           envOr(provision.EnvSocketDir, provision.SocketDir),
		LogDir:              envOr(provision.EnvLogDir, provision.LogDir),
		BinDir:              os.Getenv(provision.EnvBinDir),
		StatusPort:          statusPort,
		PeerService:         envOr(provision.EnvPeerService, config.PeerService),
		ReplicationPassword: os.Getenv(provision.EnvReplPassword),
		OpsPassword:         os.Getenv(provision.EnvOpsPassword),
		RewindPassword:      os.Getenv(provision.EnvRewindPassword),
		Timeouts:            agent.StopTimeoutsFrom(config),
	}
	if built.Member == "" {
		return agent.Options{}, errors.New(provision.EnvMember + " is not set")
	}
	if !needClient {
		return built, nil
	}
	built.Client, err = apiClient(ctx)
	if err != nil {
		return agent.Options{}, err
	}
	return built, nil
}

func apiClient(_ context.Context) (client.Client, error) {
	config, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}
	if err := pgelasticv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		return nil, err
	}
	return client.New(config, client.Options{Scheme: scheme.Scheme})
}

func runAgent(ctx context.Context) error {
	built, err := options(ctx, true)
	if err != nil {
		return err
	}
	supervisor := agent.NewSupervisor(built)
	server := &agent.StatusServer{Supervisor: supervisor, Options: built}

	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()

	group, groupCtx := errgroup.WithContext(serverCtx)
	group.Go(func() error { return server.Serve(groupCtx) })
	group.Go(func() error {
		defer cancelServer()
		return supervisor.Run(ctx)
	})
	return group.Wait()
}

func runBootstrap(ctx context.Context) error {
	built, err := options(ctx, true)
	if err != nil {
		return err
	}
	return agent.Bootstrap(ctx, built)
}

// runPromote runs the whole gated promotion sequence, never the local half on its own.
// There is deliberately no way to reach pg_ctl promote from the command line without the
// Lease and the quorum gate: a promotion that can be triggered locally is a promotion that
// can happen without the evidence that makes it safe.
func runPromote(ctx context.Context) error {
	built, err := options(ctx, true)
	if err != nil {
		return err
	}
	_, err = agent.Promote(ctx, built)
	return err
}

// runStatus prints this member's own report, read from the status server over the pod's
// loopback address.
//
// It exists because "what does this member think it is" is the first question asked when a
// failover has not happened, and the PostgreSQL image carries no HTTP client to ask with.
// Going through the status server rather than opening a database connection is deliberate:
// the answer has to be available precisely when PostgreSQL is not.
func runStatus(ctx context.Context, out io.Writer) error {
	built, err := options(ctx, false)
	if err != nil {
		return err
	}
	report, err := provision.FetchMemberReport(ctx,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(int(built.StatusPort))))
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// runWALArchive is archive_command. It is a subcommand of the same binary so that the
// archive path cannot drift from the agent that configured it.
func runWALArchive(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("wal-archive", flag.ContinueOnError)
	segment := flags.String("segment", "", "path to the segment, PostgreSQL's %p")
	name := flags.String("name", "", "segment file name, PostgreSQL's %f")
	if err := flags.Parse(args); err != nil {
		return err
	}
	built, err := options(ctx, false)
	if err != nil {
		return err
	}
	return agent.ArchiveWAL(ctx, built, *segment, *name)
}

// runWALRestore is restore_command.
func runWALRestore(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("wal-restore", flag.ContinueOnError)
	name := flags.String("name", "", "segment file name, PostgreSQL's %f")
	target := flags.String("target", "", "destination path, PostgreSQL's %p")
	rewind := flags.Bool("rewind", false,
		"disable prefetch for pg_rewind's backward single-pass WAL walk")
	if err := flags.Parse(args); err != nil {
		return err
	}
	built, err := options(ctx, false)
	if err != nil {
		return err
	}
	return agent.RestoreWAL(ctx, built, *name, *target, *rewind)
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
