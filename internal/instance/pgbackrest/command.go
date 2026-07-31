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

package pgbackrest

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Command is one pgBackRest invocation, kept as an argv so a test can assert on the exact
// flags rather than on a string that happens to contain them.
type Command struct {
	// Args is the argv after the executable name.
	Args []string
}

// Invocation names the config file and stanza every command shares.
type Invocation struct {
	ConfigFile string
	Stanza     string
}

func (i Invocation) command(args ...string) Command {
	return Command{Args: append([]string{
		"--config=" + i.ConfigFile,
		"--stanza=" + i.Stanza,
	}, args...)}
}

// StanzaCreate initialises the repository for this stanza.
//
// It is idempotent against a stanza whose recorded system identifier and configuration
// match this one, and an error against a stanza that belongs to a different database. That
// refusal is the guard against two instances sharing one stanza path, and it is why the
// stanza is named after the system identifier rather than after anything an operator types.
func (i Invocation) StanzaCreate() Command { return i.command("stanza-create") }

// Check verifies that the repository is reachable and that archiving works end to end, by
// asking PostgreSQL to switch a segment and then finding it in the archive.
func (i Invocation) Check() Command { return i.command("check") }

// ArchivePush archives one segment. The argument is PostgreSQL's %p, the path to the
// segment inside pg_wal, not its bare name.
func (i Invocation) ArchivePush(segmentPath string) Command {
	return i.command("archive-push", segmentPath)
}

// ArchiveGet fetches one segment out of the archive. The arguments are PostgreSQL's %f and
// %p in that order.
//
// Prefetch is asynchronous by default, which is what makes replaying a long WAL history
// bearable. It is disabled for pg_rewind, which walks WAL backwards in a single pass: every
// segment read ahead there is a segment fetched and discarded, and the miss that ends the
// walk has to be reported immediately rather than after the prefetcher has finished.
func (i Invocation) ArchiveGet(name, destination string, prefetch bool) Command {
	command := i.command("archive-get")
	if !prefetch {
		command.Args = append(command.Args, "--no-archive-async")
	}
	command.Args = append(command.Args, name, destination)
	return command
}

// Info reports the repository catalogue for this stanza as JSON.
func (i Invocation) Info() Command { return i.command("--output=json", "info") }

// BackupAnnotation ties a repository object back to the PgBackup that asked for it.
const BackupAnnotation = "pgelastic-backup"

// Backup takes one physical backup.
//
// The annotation ties the repository object back to the PgBackup that asked for it, so the
// repository stays self-describing when the Kubernetes objects are gone - which is the case
// a backup exists for.
func (i Invocation) Backup(kind BackupType, name string) Command {
	return i.command("backup",
		"--type="+string(kind),
		"--annotation="+BackupAnnotation+"="+name,
	)
}

// Expire applies the retention policy configured for the stanza.
//
// It is a command of its own rather than a flag on backup because it has to be able to run
// when no backup is being taken: a repository whose instance stopped backing up would
// otherwise keep everything forever, and the storage bill is the first anybody hears of it.
func (i Invocation) Expire() Command { return i.command("expire") }

// BackupType is what pgBackRest calls the three kinds on the command line.
type BackupType string

const (
	// BackupFull stands alone.
	BackupFull BackupType = "full"
	// BackupDifferential is relative to the last full.
	BackupDifferential BackupType = "diff"
	// BackupIncremental is relative to the last backup of any type.
	BackupIncremental BackupType = "incr"
)

// RestoreOptions is where recovery starts and where it stops.
type RestoreOptions struct {
	// BackupID names the base backup. Empty lets pgBackRest choose the newest one that
	// ended before the target, which only has an answer for a target it can order.
	BackupID string
	// TargetType is pgBackRest's spelling: time, lsn, name, xid, immediate, or empty to
	// replay everything the archive holds.
	TargetType string
	// TargetValue is the target itself, unused for immediate.
	TargetValue string
	// Exclusive stops just before the target rather than just after it.
	Exclusive bool
	// Timeline is "latest" or a timeline number.
	Timeline string
	// RestoreCommand is what the recovered instance will fetch further segments with. It is
	// pinned rather than left to pgBackRest to generate, because the generated one does not
	// carry the configuration file this deployment needs and would fail on the first
	// segment - at the point where the base backup has already been downloaded.
	RestoreCommand string
}

// Restore writes a base backup into the data directory and leaves PostgreSQL configured to
// replay WAL onto it.
//
// --delta compares what is already in the data directory against the backup and fetches
// only the difference. On an empty directory that costs one listing; on a directory being
// restored over, it is the difference between minutes and hours.
func (i Invocation) Restore(options RestoreOptions) Command {
	command := i.command("restore", "--delta")
	if options.BackupID != "" {
		command.Args = append(command.Args, "--set="+options.BackupID)
	}
	if options.TargetType != "" {
		command.Args = append(command.Args, "--type="+options.TargetType)
	}
	if options.TargetValue != "" {
		command.Args = append(command.Args, "--target="+options.TargetValue)
	}
	if options.Exclusive {
		command.Args = append(command.Args, "--target-exclusive")
	}
	if options.Timeline != "" {
		command.Args = append(command.Args, "--target-timeline="+options.Timeline)
	}
	// Promote rather than pause. A recovery that reaches its target and then waits is an
	// instance that never becomes ready, which reads from outside as a restore that hung.
	if options.TargetType != "" {
		command.Args = append(command.Args, "--target-action=promote")
	}
	if options.RestoreCommand != "" {
		command.Args = append(command.Args,
			"--recovery-option=restore_command="+options.RestoreCommand)
	}
	return command
}

// Runner executes pgBackRest.
type Runner struct {
	// Binary is the executable, resolved from PATH when empty.
	Binary string
}

func (r Runner) binary() string {
	if r.Binary == "" {
		return Binary
	}
	return r.Binary
}

// Run executes one command and returns its standard output.
//
// Standard error is folded into the returned error rather than dropped, because
// archive_command's own output is discarded by PostgreSQL: without this the only record of
// why a segment failed to archive would be a non-zero exit code in the postmaster log.
func (r Runner) Run(ctx context.Context, command Command) (string, error) {
	var stdout, stderr bytes.Buffer
	process := exec.CommandContext(ctx, r.binary(), command.Args...) // #nosec G204 -- argv is built here, not taken from input
	process.Stdout = &stdout
	process.Stderr = &stderr
	if err := process.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s",
			r.binary(), strings.Join(command.Args, " "), err,
			strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
