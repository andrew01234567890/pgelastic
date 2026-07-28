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

package pgtool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// Toolchain locates the PostgreSQL binaries and the directories they act on.
type Toolchain struct {
	// BinDir holds the PostgreSQL binaries. Empty means resolve them from PATH.
	BinDir string
	// DataDir is PGDATA.
	DataDir string
	// WALDir is the pg_wal directory on the separate WAL volume. It is mandatory: a full
	// pg_wal PANICs the primary and takes every tenant on the instance with it.
	WALDir string
	// Stdout and Stderr receive the tools' output.
	Stdout io.Writer
	Stderr io.Writer
}

func (t Toolchain) path(tool string) string {
	if t.BinDir == "" {
		return tool
	}
	return filepath.Join(t.BinDir, tool)
}

// Run executes one PostgreSQL binary, folding its stderr into the returned error so a
// failure is diagnosable from the pod log alone.
func (t Toolchain) Run(ctx context.Context, tool string, args ...string) error {
	_, err := t.Output(ctx, tool, args...)
	return err
}

// Output executes one PostgreSQL binary and returns its standard output.
func (t Toolchain) Output(ctx context.Context, tool string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	command := exec.CommandContext(ctx, t.path(tool), args...)
	command.Stdout = &stdout
	command.Stderr = &stderr
	if t.Stderr != nil {
		command.Stderr = io.MultiWriter(&stderr, t.Stderr)
	}
	if err := command.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %v: %w: %s", tool, args, err, stderr.String())
	}
	return stdout.String(), nil
}

// ControlData reads and parses the control file of a data directory.
func (t Toolchain) ControlData(ctx context.Context, dataDir string) (ControlData, error) {
	output, err := t.Output(ctx, "pg_controldata", "-D", dataDir)
	if err != nil {
		return ControlData{}, err
	}
	return ParseControlData(output)
}

// InitDBOptions pins every flag explicitly so the intent survives a version bump.
type InitDBOptions struct {
	// Username is the bootstrap superuser. It has no password at all: it is reachable
	// only by peer authentication over a Unix socket in an emptyDir.
	Username string
	// Encoding, LocaleProvider and BuiltinLocale are the collation contract. The builtin
	// provider is the highest-leverage choice available here, because it removes ICU and
	// glibc version drift between instances - precisely the drift that would silently
	// break live tenant migration and cross-instance restore.
	Encoding       string
	LocaleProvider string
	BuiltinLocale  string
	// WALSegmentSizeMB is fixed at initdb and can never be changed afterwards.
	WALSegmentSizeMB int
}

// DefaultInitDBOptions are the pinned values from the design's bootstrap section.
func DefaultInitDBOptions() InitDBOptions {
	return InitDBOptions{
		Username:         "postgres",
		Encoding:         "UTF8",
		LocaleProvider:   "builtin",
		BuiltinLocale:    "C.UTF-8",
		WALSegmentSizeMB: 16,
	}
}

// InitDB creates the data directory with every flag pinned.
func (t Toolchain) InitDB(ctx context.Context, options InitDBOptions) error {
	return t.Run(ctx, "initdb",
		"--pgdata", t.DataDir,
		"--waldir", t.WALDir,
		"--data-checksums",
		"--wal-segsize", strconv.Itoa(options.WALSegmentSizeMB),
		"--encoding", options.Encoding,
		"--locale-provider", options.LocaleProvider,
		"--builtin-locale", options.BuiltinLocale,
		"--username", options.Username,
		"--auth-local", "peer",
		"--auth-host", "scram-sha-256",
	)
}

// BaseBackupOptions describes a replica clone.
type BaseBackupOptions struct {
	// Host, Port and User address the primary.
	Host string
	Port int32
	User string
	// SlotName is the persistent slot the standby will keep streaming from.
	SlotName string
	// CreateSlot asks pg_basebackup to create the slot as part of the backup, so the WAL
	// the clone needs cannot be recycled between the backup finishing and the standby
	// connecting. It is false when the slot already exists - which it does after a
	// promotion, because the new primary creates one for every other member before it
	// accepts writes - and pg_basebackup refuses to create a slot that is already there.
	CreateSlot bool
}

// BaseBackup clones a replica.
//
// The checkpoint is deliberately left at the default SPREAD rather than forced with
// -c fast: a fast checkpoint produces an I/O spike that starves the tenant queries this
// product exists to keep responsive. -X stream takes the WAL concurrently, and note that
// --max-rate would throttle only the data transfer and not that concurrent stream.
func (t Toolchain) BaseBackup(ctx context.Context, options BaseBackupOptions) error {
	arguments := []string{
		"--pgdata", t.DataDir,
		"--waldir", t.WALDir,
		"--wal-method", "stream",
		"--slot", options.SlotName,
	}
	if options.CreateSlot {
		arguments = append(arguments, "--create-slot")
	}
	arguments = append(arguments,
		"--host", options.Host,
		"--port", strconv.Itoa(int(options.Port)),
		"--username", options.User,
		"--no-password",
		"--progress",
		"--verbose",
	)
	return t.Run(ctx, "pg_basebackup", arguments...)
}

// StopMode is the shutdown mode passed to pg_ctl.
type StopMode string

const (
	// StopSmart waits for every client to disconnect.
	StopSmart StopMode = "smart"
	// StopFast disconnects clients and shuts down cleanly.
	StopFast StopMode = "fast"
	// StopImmediate aborts without a shutdown checkpoint, forcing crash recovery on the
	// next start.
	StopImmediate StopMode = "immediate"
)

// Stop shuts the postmaster down through pg_ctl.
func (t Toolchain) Stop(ctx context.Context, mode StopMode, timeout time.Duration) error {
	return t.Run(ctx, "pg_ctl",
		"-D", t.DataDir,
		"-m", string(mode),
		"-w",
		"-t", strconv.Itoa(int(timeout.Seconds())),
		"stop",
	)
}

// Promote turns a standby into a primary. PostgreSQL removes standby.signal itself, so
// the file must not be deleted by hand; its continued existence afterwards is a hard
// failure rather than something to clean up.
func (t Toolchain) Promote(ctx context.Context, timeout time.Duration) error {
	return t.Run(ctx, "pg_ctl",
		"-D", t.DataDir,
		"-w",
		"-t", strconv.Itoa(int(timeout.Seconds())),
		"promote",
	)
}

// Reload asks the postmaster to re-read its configuration files.
func (t Toolchain) Reload(ctx context.Context) error {
	return t.Run(ctx, "pg_ctl", "-D", t.DataDir, "reload")
}

// PingResult is the outcome of pg_isready, kept as the exit code's meaning rather than as
// a boolean, because the startup probe treats "rejecting connections" as success.
type PingResult int

const (
	// PingOK is PQPING_OK: the server is accepting connections.
	PingOK PingResult = 0
	// PingReject is PQPING_REJECT: the server is running but not yet accepting
	// connections, which during startup is exactly the state being waited for and is
	// therefore a success, not a failure.
	PingReject PingResult = 1
	// PingNoResponse is PQPING_NO_RESPONSE.
	PingNoResponse PingResult = 2
	// PingNoAttempt is PQPING_NO_ATTEMPT: the parameters were bad enough that no
	// connection was tried.
	PingNoAttempt PingResult = 3
)

// IsReady runs pg_isready against the local Unix socket.
func (t Toolchain) IsReady(ctx context.Context, socketDir string, port int32) PingResult {
	command := exec.CommandContext(ctx, t.path("pg_isready"),
		"-h", socketDir, "-p", strconv.Itoa(int(port)), "-q")
	if err := command.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return PingResult(exitErr.ExitCode())
		}
		return PingNoAttempt
	}
	return PingOK
}

// DataDirectoryInitialised reports whether a data directory already holds a control file.
func DataDirectoryInitialised(dataDir string) bool {
	_, err := os.Stat(filepath.Join(dataDir, "global", "pg_control"))
	return err == nil
}

// Rewind rewinds a diverged data directory back to its common ancestor with the source.
//
// The target must have been shut down cleanly first - pg_rewind refuses otherwise, and
// EnsureCleanShutdown exists for exactly that. --progress goes to stderr, which the agent
// folds into the pod log so a rewind that is merely slow can be told apart from one that
// has hung.
func (t Toolchain) Rewind(ctx context.Context, sourceConnInfo string) error {
	return t.Run(ctx, "pg_rewind",
		"--target-pgdata", t.DataDir,
		"--source-server", sourceConnInfo,
		"--progress",
	)
}

// ClusterStateShutDown reports whether a control file says the cluster stopped cleanly, in
// either of the two states pg_rewind accepts.
func ClusterStateShutDown(state string) bool {
	return state == "shut down" || state == "shut down in recovery"
}
