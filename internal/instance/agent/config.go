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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// configFileMode keeps the generated files unreadable to anything but the postgres user:
// override.conf carries the replication password inside primary_conninfo.
const configFileMode os.FileMode = 0o600

// LoadAgentConfig reads the operator's decisions off the mounted ConfigMap.
func LoadAgentConfig(path string) (provision.AgentConfig, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return provision.AgentConfig{}, err
	}
	var config provision.AgentConfig
	if err := json.Unmarshal(document, &config); err != nil {
		return provision.AgentConfig{}, err
	}
	return config, nil
}

// WriteConfig renders and writes every configuration file the agent owns and returns the
// hash identifying the result.
//
// The hash is computed over the file bodies and then appended to custom.conf as a custom
// GUC. Reading it back with current_setting() - never from pg_show_all_file_settings() -
// is what guarantees the hash and pending_restart describe the same reload rather than two
// different instants separated by a rewrite nobody observed.
func WriteConfig(
	config provision.AgentConfig,
	member string,
	replication pgconf.ReplicationConfig,
	dataDir string,
	controlData *pgtool.ControlData,
) (string, error) {
	instance := config.Postgres
	instance.MemberName = member
	replication.RestoreCommand = restoreCommand(config)

	settings := pgconf.RenderCustomConf(instance)
	if controlData != nil {
		settings = applyEnforcedFloor(settings, *controlData)
	}

	custom := pgconf.FormatSettings("pgelastic operator parameters", settings)
	override := pgconf.FormatSettings("pgelastic replication and recovery",
		pgconf.RenderOverrideConf(replication))
	hba := pgconf.RenderHBA(config.HBA)
	ident := pgconf.RenderIdent()
	hash := pgconf.Hash(custom, override, hba, ident)

	files := map[string]string{
		pgconf.CustomConfFile:   custom + pgconf.HashLine(hash),
		pgconf.OverrideConfFile: override,
		pgconf.HBAFile:          hba,
		pgconf.IdentFile:        ident,
	}
	for name, contents := range files {
		if err := writeFileAtomically(filepath.Join(dataDir, name), contents); err != nil {
			return "", err
		}
	}
	return hash, nil
}

// restoreCommand is how PostgreSQL fetches a segment it no longer has in pg_wal.
//
// It is decided here rather than by each caller because there are four of them - initdb, a
// standby's first join, a rejoin and a promotion - and a restore_command present on three
// of the four is a member that silently cannot catch up from the archive. What it buys on a
// standby is the difference between falling off the primary's wal_keep_size and having to
// re-clone, and reading the missing segments out of the repository instead.
//
// It is set on a primary too, where it is inert: archive recovery is the only thing that
// consults it, and a primary is not in archive recovery. A member that is demoted then has
// it already, rather than acquiring it in the same reload that put it into recovery.
func restoreCommand(config provision.AgentConfig) string {
	if config.Backup == nil || !config.Backup.Configured() {
		return ""
	}
	return fmt.Sprintf("%s wal-restore --name %%f --target %%p", provision.AgentBinary)
}

// writeFileAtomically replaces a configuration file in one step that either happens or
// does not.
//
// os.WriteFile truncates the target and then writes into it, so a crash in between leaves
// a file that is empty or half a file. That is not a cosmetic loss here: override.conf is
// read back before every rewrite precisely so that replacing it wholesale does not discard
// the settings the rewrite was not about, and a primary that restarts having lost
// synchronous_standby_names acknowledges commits no standby has seen. The rename is the
// atomic step; the two fsyncs are what make it survive the power cut rather than only the
// process dying.
func writeFileAtomically(path, contents string) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { _ = os.Remove(name) }()

	if err := writeAndSync(temporary, contents); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func writeAndSync(file *os.File, contents string) error {
	defer func() { _ = file.Close() }()
	if err := file.Chmod(configFileMode); err != nil {
		return err
	}
	if _, err := file.WriteString(contents); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

// syncDirectory makes the rename itself durable. Without it the new contents are on disk
// and the directory entry still points at the old inode.
func syncDirectory(path string) error {
	directory, err := os.Open(path) // #nosec G304 -- the data directory is the agent's own
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

// applyEnforcedFloor raises the five enforced parameters to max(desired, pg_controldata).
//
// It runs on every non-first start, not only after a restore. A primary restarted with a
// higher value leaves every standby unable to begin recovery at all - PostgreSQL FATALs
// with "recovery aborted because of insufficient parameter settings" - and the standby has
// no way to learn the new value except from the control file it just replayed.
func applyEnforcedFloor(settings []pgconf.Setting, controlData pgtool.ControlData) []pgconf.Setting {
	desired := make(map[string]int32, len(pgconf.EnforcedParameters))
	for _, setting := range settings {
		if !isEnforced(setting.Name) {
			continue
		}
		value, err := strconv.ParseInt(setting.Value, 10, 32)
		if err != nil {
			continue
		}
		desired[setting.Name] = int32(value)
	}
	floored := pgtool.EnforcedFloor(desired, controlData)
	for i, setting := range settings {
		if value, ok := floored[setting.Name]; ok {
			settings[i].Value = strconv.Itoa(int(value))
		}
	}
	return settings
}

func isEnforced(name string) bool {
	return slices.Contains(pgconf.EnforcedParameters, name)
}

// EnsureIncludes appends the two include directives to postgresql.conf, exactly once.
// postgresql.conf is otherwise never touched after initdb wrote it: everything pgelastic
// decides lives in the two files it owns outright, so a PostgreSQL version that changes
// its own defaults changes them here too.
func EnsureIncludes(dataDir string) error {
	path := filepath.Join(dataDir, pgconf.PostgresqlConfFile)
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.Contains(string(contents), pgconf.CustomConfFile) {
		return nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, configFileMode)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	_, err = file.WriteString("\n# Added once by pgelastic at initdb.\n" + pgconf.IncludeDirectives)
	return err
}

// StandbySignal is the file whose presence puts PostgreSQL into recovery.
const StandbySignal = "standby.signal"

// SetStandbySignal creates or removes standby.signal.
//
// It is only ever used to put a member *into* recovery. Coming out of it is PostgreSQL's
// job: pg_ctl promote removes the file itself, and a file still present sixty seconds
// after a promotion is a hard failure to be reported, not a leftover to be tidied away.
func SetStandbySignal(dataDir string, standby bool) error {
	path := filepath.Join(dataDir, StandbySignal)
	if !standby {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, configFileMode)
	if err != nil {
		return err
	}
	return file.Close()
}
