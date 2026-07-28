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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const (
	standbyMember = "pg-a-2"
	quorumOfOne   = `ANY 1 ("pg-a-2")`
	quorumOfTwo   = `ANY 1 ("pg-a-2","pg-a-3")`
)

func agentConfig() provision.AgentConfig {
	return provision.AgentConfig{
		Instance:  "pg-a",
		Namespace: "saas-prod",
		Replicas:  3,
		Quorum:    "ANY 1",
		Postgres: pgconf.InstanceConfig{
			Capacity:        pgconf.DeriveCapacity(50, 4, 3, 4),
			SocketDirectory: "/var/run/postgresql",
			Port:            5432,
		},
		HBA: pgconf.HBAConfig{
			PeerSources:     []string{"all"},
			ReplicationRole: provision.ReplicationRole,
			OpsRole:         provision.OpsRole,
		},
	}
}

func readFile(t *testing.T, dir, name string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func TestWriteConfigProducesTheFourOwnedFiles(t *testing.T) {
	dir := t.TempDir()
	hash, err := WriteConfig(agentConfig(), "pg-a-1", pgconf.ReplicationConfig{}, dir, nil)
	if err != nil {
		t.Fatalf("WriteConfig = %v", err)
	}
	if hash == "" {
		t.Fatal("the configuration hash is what binds the file to the reload that loaded it")
	}
	for _, name := range []string{
		pgconf.CustomConfFile, pgconf.OverrideConfFile, pgconf.HBAFile, pgconf.IdentFile,
	} {
		if readFile(t, dir, name) == "" {
			t.Errorf("%s is empty", name)
		}
	}
}

func TestWriteConfigInjectsTheHashAsACustomGUC(t *testing.T) {
	dir := t.TempDir()
	hash, err := WriteConfig(agentConfig(), "pg-a-1", pgconf.ReplicationConfig{}, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	custom := readFile(t, dir, pgconf.CustomConfFile)
	if !strings.Contains(custom, pgconf.GUCConfigSHA256+" = '"+hash+"'") {
		t.Fatal("the hash must be readable back out of the postmaster with current_setting()")
	}
	// The hash covers the bodies, not itself: a file whose hash line changed the hash
	// could never settle.
	if pgconf.ParseSettings(custom)[pgconf.GUCConfigSHA256] != hash {
		t.Error("the hash line must parse back to the same value")
	}
}

func TestWriteConfigSetsTheMemberNameAsClusterName(t *testing.T) {
	dir := t.TempDir()
	if _, err := WriteConfig(agentConfig(), standbyMember, pgconf.ReplicationConfig{}, dir, nil); err != nil {
		t.Fatal(err)
	}
	if got := pgconf.ParseSettings(readFile(t, dir, pgconf.CustomConfFile))[pgconf.GUCClusterName]; got != standbyMember {
		t.Errorf("cluster_name = %q, want the member name", got)
	}
}

func TestWriteConfigRaisesTheEnforcedParametersToTheControlFile(t *testing.T) {
	dir := t.TempDir()
	controlData := pgtool.ControlData{
		SystemIdentifier: "1",
		EnforcedSettings: map[string]int32{
			pgconf.GUCMaxConnections: 900, pgconf.GUCMaxWorkerProcesses: 4,
		},
	}
	if _, err := WriteConfig(agentConfig(), standbyMember,
		pgconf.ReplicationConfig{}, dir, &controlData); err != nil {
		t.Fatal(err)
	}
	settings := pgconf.ParseSettings(readFile(t, dir, pgconf.CustomConfFile))
	if settings[pgconf.GUCMaxConnections] != "900" {
		t.Errorf("max_connections = %q, want the control file's higher 900", settings[pgconf.GUCMaxConnections])
	}
	if settings[pgconf.GUCMaxWorkerProcesses] != "16" {
		t.Errorf("max_worker_processes = %q, want the higher desired 16",
			settings[pgconf.GUCMaxWorkerProcesses])
	}
}

func TestWriteConfigReplacesOverrideConfWholesale(t *testing.T) {
	dir := t.TempDir()
	standby := pgconf.ReplicationConfig{
		Standby:         true,
		PrimaryConnInfo: "host=pg-a-1 dbname=postgres",
		PrimarySlotName: "pgelastic_pg_a_2",
	}
	if _, err := WriteConfig(agentConfig(), standbyMember, standby, dir, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteConfig(agentConfig(), standbyMember, pgconf.ReplicationConfig{}, dir, nil); err != nil {
		t.Fatal(err)
	}
	settings := pgconf.ParseSettings(readFile(t, dir, pgconf.OverrideConfFile))
	if settings["primary_conninfo"] != "" || settings["primary_slot_name"] != "" {
		t.Errorf("override.conf = %v, want a promoted member's old conninfo gone entirely", settings)
	}
}

func TestEnsureIncludesIsAppendedExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, pgconf.PostgresqlConfFile)
	if err := os.WriteFile(path, []byte("# initdb output\nlisten_addresses = 'localhost'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := EnsureIncludes(dir); err != nil {
			t.Fatal(err)
		}
	}
	contents := readFile(t, dir, pgconf.PostgresqlConfFile)
	if strings.Count(contents, pgconf.CustomConfFile) != 1 {
		t.Errorf("postgresql.conf = %q, want exactly one include of custom.conf", contents)
	}
	if !strings.Contains(contents, "listen_addresses = 'localhost'") {
		t.Error("initdb's own output must survive")
	}
}

func TestSetStandbySignal(t *testing.T) {
	dir := t.TempDir()
	if err := SetStandbySignal(dir, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, StandbySignal)); err != nil {
		t.Fatalf("standby.signal was not created: %v", err)
	}
	if err := SetStandbySignal(dir, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, StandbySignal)); !os.IsNotExist(err) {
		t.Fatal("standby.signal was not removed")
	}
	if err := SetStandbySignal(dir, false); err != nil {
		t.Fatalf("removing an absent standby.signal must be a no-op: %v", err)
	}
}

func TestSynchronousStandbyNamesGrowsWithTheStreamingSet(t *testing.T) {
	if got := SynchronousStandbyNames("ANY 1", nil); got != "" {
		t.Errorf("with no standby streaming, want an empty clause, got %q: naming an absent "+
			"standby blocks every commit under dataDurability Required", got)
	}
	if got := SynchronousStandbyNames("ANY 1", []string{standbyMember}); got != quorumOfOne {
		t.Errorf("one standby = %q", got)
	}
	got := SynchronousStandbyNames("ANY 1", []string{"pg-a-3", standbyMember})
	if got != quorumOfTwo {
		t.Errorf("two standbys = %q, want them sorted so the clause is stable", got)
	}
}

func TestParseSyncStandbyNames(t *testing.T) {
	cases := []struct {
		clause  string
		numSync int32
		members []string
	}{
		{quorumOfTwo, 1, []string{standbyMember, "pg-a-3"}},
		{`FIRST 2 (a, b, c)`, 2, []string{"a", "b", "c"}},
		{"", 0, nil},
		{"garbage", 0, nil},
	}
	for _, testCase := range cases {
		numSync, members := ParseSyncStandbyNames(testCase.clause)
		if numSync != testCase.numSync {
			t.Errorf("ParseSyncStandbyNames(%q) numSync = %d, want %d",
				testCase.clause, numSync, testCase.numSync)
		}
		if !slices.Equal(members, testCase.members) {
			t.Errorf("ParseSyncStandbyNames(%q) members = %v, want %v",
				testCase.clause, members, testCase.members)
		}
	}
}

func TestPrimaryConnInfoCarriesDBName(t *testing.T) {
	conninfo := PrimaryConnInfo("pg-a-1.pg-a-peers.saas-prod.svc", standbyMember, "secret")
	if !strings.Contains(conninfo, "dbname=") {
		t.Error("slot synchronisation errors out without dbname=, and only at failover time")
	}
	if !strings.Contains(conninfo, "application_name=pg-a-2") {
		t.Error("application_name is what synchronous_standby_names names")
	}
}
