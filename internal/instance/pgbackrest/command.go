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
