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

package migration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// DefaultDumpJobs is the parallelism given to pg_dump and pg_restore when the pool does not
// pick one. Both processes run on the target member, so the number is bounded by what the
// destination can absorb rather than by what the source can emit.
const DefaultDumpJobs int32 = 4

// CopyOffline moves the tenant's data with a parallel directory-format dump and restore.
//
// It runs entirely inside the target's container: pg_dump reaches the source over the pod
// network as the replication role, and pg_restore reaches the local postmaster over its
// Unix socket as the superuser. The dump lands on the target's data volume, outside PGDATA,
// which is the volume whose headroom preflight already measured.
//
// This is the whole reason the offline pause is measured in tens of seconds. The tenant is
// quiesced for the entire dump and restore, because a dump taken while writes continue is
// behind by whatever was written during it and offline has no replication stream to close
// that gap with.
func CopyOffline(ctx context.Context, shell Shell, plan Plan) error {
	jobs := plan.Concurrency
	if jobs < 1 {
		jobs = DefaultDumpJobs
	}
	for _, command := range []string{
		offlineDumpCommand(plan, jobs),
		offlineRestoreCommand(plan, jobs),
	} {
		output, err := shell.Run(ctx, plan.Target, []string{"sh", "-c", command})
		if err != nil {
			return fmt.Errorf("offline copy failed: %w: %s", err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

// DiscardDump removes the staged dump. It is run on every exit from the offline path,
// successful or not: a directory-format dump of a tenant is the size of the tenant, and
// leaving one behind on the target's data volume is how the next migration fails its
// storage headroom check for reasons nobody can find.
func DiscardDump(ctx context.Context, shell Shell, plan Plan) error {
	_, err := shell.Run(ctx, plan.Target, []string{"rm", "-rf", plan.DumpDir})
	return err
}

func offlineDumpCommand(plan Plan, jobs int32) string {
	// pg_dump creates the dump directory but not its parent, so the scratch root is made
	// first rather than left to fail on a fresh volume.
	return fmt.Sprintf(`set -e; mkdir -p %s; rm -rf %s; `+
		`pg_dump --format=directory --jobs=%s --no-owner --no-privileges `+
		`--quote-all-identifiers --file=%s --dbname=%s`,
		shellQuote(ScratchDir), shellQuote(plan.DumpDir), strconv.FormatInt(int64(jobs), 10),
		shellQuote(plan.DumpDir), shellQuote(plan.SourceConnInfo))
}

// offlineRestoreCommand restores schema and data together. --exit-on-error is what turns a
// half-restored database into a failed migration the tenant never sees, rather than a
// successful-looking one that is missing whatever errored.
//
// --clean --if-exists is what makes the phase survive its own retry. A restore that died
// part-way leaves objects behind, and a second attempt without the drops fails on the first
// of them - so the retry budget would be spent re-running a command that could never succeed.
// The drops are safe because the target database holds nothing but what this migration is
// restoring into it.
func offlineRestoreCommand(plan Plan, jobs int32) string {
	return fmt.Sprintf(`pg_restore --jobs=%s --no-owner --no-privileges --exit-on-error `+
		`--clean --if-exists --host=%s --username=postgres --dbname=%s %s`,
		strconv.FormatInt(int64(jobs), 10), shellQuote(socketDir),
		shellQuote(plan.Target.Database), shellQuote(plan.DumpDir))
}
