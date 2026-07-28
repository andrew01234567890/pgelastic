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

package pgconf

import (
	"slices"
	"strings"
	"testing"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	userParameterName  = "work_mem"
	userParameterValue = "8MB"
)

func settingValue(settings []Setting, name string) (string, bool) {
	for _, setting := range settings {
		if setting.Name == name {
			return setting.Value, true
		}
	}
	return "", false
}

func TestClassifyOwnedParameters(t *testing.T) {
	cases := []struct {
		name      string
		parameter string
		ownership Ownership
		context   SettingContext
	}{
		{"max_connections is computed and needs a restart", GUCMaxConnections, OwnershipFixed, ContextPostmaster},
		{"wal_level is pinned to logical", GUCWALLevel, OwnershipBlocked, ContextPostmaster},
		{"allow_alter_system is pinned off", GUCAllowAlterSystem, OwnershipBlocked, ContextSighup},
		{"restart_after_crash is pinned off", GUCRestartAfterCrash, OwnershipBlocked, ContextSighup},
		{"io_method is pinned to worker", GUCIOMethod, OwnershipBlocked, ContextPostmaster},
		{"archive_mode is pinned on", GUCArchiveMode, OwnershipBlocked, ContextPostmaster},
		{"logging_collector is pinned on", GUCLoggingCollector, OwnershipBlocked, ContextPostmaster},
		{"synchronous_standby_names is computed", GUCSynchronousStandbyNames, OwnershipFixed, ContextSighup},
		{"work_mem belongs to the tenant", userParameterName, OwnershipUser, ContextUser},
		{"maintenance_work_mem belongs to the tenant", "maintenance_work_mem", OwnershipUser, ContextUser},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			owned := Classify(testCase.parameter)
			if owned.Ownership != testCase.ownership {
				t.Errorf("ownership = %q, want %q", owned.Ownership, testCase.ownership)
			}
			if owned.Context != testCase.context {
				t.Errorf("context = %q, want %q", owned.Context, testCase.context)
			}
		})
	}
}

func TestBlockedValuesRequiredBySection46(t *testing.T) {
	want := map[string]string{
		GUCWALLevel:             "logical",
		GUCTrackCommitTimestamp: valueOn,
		GUCAllowAlterSystem:     valueOff,
		GUCRestartAfterCrash:    valueOff,
		GUCIOMethod:             "worker",
		GUCArchiveMode:          valueOn,
		GUCLoggingCollector:     valueOn,
	}
	blocked := BlockedDefaults()
	for name, value := range want {
		if blocked[name] != value {
			t.Errorf("blocked %s = %q, want %q", name, blocked[name], value)
		}
	}
}

func TestWALLogHintsIsOwnedButNeverEmitted(t *testing.T) {
	owned := Classify(GUCWALLogHints)
	if owned.Ownership != OwnershipBlocked {
		t.Fatalf("wal_log_hints ownership = %q, want Blocked", owned.Ownership)
	}
	if !owned.Omit {
		t.Fatal("wal_log_hints must be omitted from the configuration entirely")
	}
	if _, present := BlockedDefaults()[GUCWALLogHints]; present {
		t.Fatal("wal_log_hints must not appear among the emitted blocked defaults")
	}
	settings := RenderCustomConf(InstanceConfig{})
	if _, present := settingValue(settings, GUCWALLogHints); present {
		t.Fatal("wal_log_hints must not appear in custom.conf")
	}
}

func TestUserParametersDropsOwnedNames(t *testing.T) {
	kept, dropped := UserParameters(map[string]pgelasticv1alpha1.GUCValue{
		userParameterName:      userParameterValue,
		GUCMaxConnections:      "5000",
		GUCAllowAlterSystem:    "on",
		"maintenance_work_mem": "512MB",
	})
	if len(kept) != 2 || kept[userParameterName] != userParameterValue || kept["maintenance_work_mem"] != "512MB" {
		t.Errorf("kept = %v, want only the two tenant parameters", kept)
	}
	if !slices.Equal(dropped, []string{GUCAllowAlterSystem, GUCMaxConnections}) {
		t.Errorf("dropped = %v, want the two owned parameters in sorted order", dropped)
	}
}

func TestRenderCustomConfDropsOwnedUserParameters(t *testing.T) {
	settings := RenderCustomConf(InstanceConfig{
		Capacity:       DeriveCapacity(50, 4, 3, 4),
		UserParameters: map[string]string{GUCMaxConnections: "5000", userParameterName: userParameterValue},
	})
	maxConnections, _ := settingValue(settings, GUCMaxConnections)
	if maxConnections != "72" {
		t.Errorf("max_connections = %q, want the derived 72", maxConnections)
	}
	workMem, _ := settingValue(settings, userParameterName)
	if workMem != userParameterValue {
		t.Errorf("work_mem = %q, want 8MB", workMem)
	}
}

func TestRenderCustomConfEmitsSharedPreloadLibrariesExplicitly(t *testing.T) {
	settings := RenderCustomConf(InstanceConfig{})
	value, present := settingValue(settings, GUCSharedPreloadLibraries)
	if !present {
		t.Fatal("shared_preload_libraries must be emitted even when it equals the boot default")
	}
	if value != "" {
		t.Errorf("shared_preload_libraries = %q, want the empty default", value)
	}
}

func TestDeriveCapacity(t *testing.T) {
	cases := []struct {
		name            string
		allocatable     int32
		concurrentDumps int32
		wantOverhead    int32
		wantMax         int32
	}{
		{"no logical dumps sits at the floor of the band", 400, 0, 10, 418},
		{"the default dump budget lands mid-band", 400, 4, 14, 422},
		{"a large dump budget is capped at the ceiling", 400, 32, 16, 424},
		{"the dev class stays small", 50, 4, 14, 72},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			capacity := DeriveCapacity(testCase.allocatable, testCase.concurrentDumps, 3, 4)
			if capacity.AgentOverhead != testCase.wantOverhead {
				t.Errorf("agent overhead = %d, want %d", capacity.AgentOverhead, testCase.wantOverhead)
			}
			if capacity.MaxConnections != testCase.wantMax {
				t.Errorf("max_connections = %d, want %d", capacity.MaxConnections, testCase.wantMax)
			}
			sum := capacity.Allocatable + capacity.SuperuserReserved + capacity.Reserved + capacity.AgentOverhead
			if capacity.MaxConnections != sum {
				t.Errorf("max_connections = %d, want the sum of its parts %d", capacity.MaxConnections, sum)
			}
			if capacity.SuperuserReserved != 3 || capacity.Reserved != 5 {
				t.Errorf("reserves = %d/%d, want 3/5", capacity.SuperuserReserved, capacity.Reserved)
			}
		})
	}
}

func TestDeriveCapacitySizesReplicationForStandbysAndMigrations(t *testing.T) {
	capacity := DeriveCapacity(100, 4, 3, 4)
	if capacity.WALSenders != 6 {
		t.Errorf("max_wal_senders = %d, want two standbys plus four migration slots", capacity.WALSenders)
	}
	if capacity.ReplicationSlots != 6 {
		t.Errorf("max_replication_slots = %d, want two standbys plus four migration slots", capacity.ReplicationSlots)
	}
}

func TestLookupSizingClass(t *testing.T) {
	class, err := LookupSizingClass("gp-8")
	if err != nil {
		t.Fatalf("LookupSizingClass(gp-8) = %v", err)
	}
	if class.AllocatableConnections != 400 {
		t.Errorf("gp-8 allocatable = %d, want 400", class.AllocatableConnections)
	}
	if _, err := LookupSizingClass("gp-nonexistent"); err == nil {
		t.Fatal("an unknown class must be an error, never a silent fallback")
	}
}

func TestHashDistinguishesFieldBoundaries(t *testing.T) {
	if Hash("ab", "c") == Hash("a", "bc") {
		t.Fatal("hash must not be invariant under moving bytes between parts")
	}
	first, second := Hash("a", "b"), Hash("a", string([]byte{'b'}))
	if first != second {
		t.Fatal("hash must be stable")
	}
}

func TestRenderCustomConfIsDeterministic(t *testing.T) {
	config := InstanceConfig{
		MemberName:     "pg-a-1",
		Capacity:       DeriveCapacity(50, 4, 3, 4),
		UserParameters: map[string]string{userParameterName: userParameterValue, "random_page_cost": "1.1"},
	}
	first := FormatSettings("custom", RenderCustomConf(config))
	for range 8 {
		if FormatSettings("custom", RenderCustomConf(config)) != first {
			t.Fatal("rendering must be deterministic or the config hash is meaningless")
		}
	}
}

func TestFormatSettingsQuotesAndEscapes(t *testing.T) {
	rendered := FormatSettings("custom", []Setting{{Name: GUCArchiveCommand, Value: "shim 'x' %p"}})
	if !strings.Contains(rendered, "archive_command = 'shim ''x'' %p'") {
		t.Errorf("rendered = %q, want a quoted value with doubled single quotes", rendered)
	}
}

func TestRenderHBADeniesByDefault(t *testing.T) {
	rendered := RenderHBA(HBAConfig{
		ProxySources:    []string{"10.244.0.0/16"},
		PeerSources:     []string{"10.244.0.0/16"},
		ReplicationRole: "pgelastic_repl",
		OpsRole:         "pgelastic_ops",
	})
	lines := strings.Split(strings.TrimSpace(rendered), "\n")
	if lines[len(lines)-1] != "host all all all reject" {
		t.Errorf("last line = %q, want the deny-by-default catch-all", lines[len(lines)-1])
	}
	if !strings.Contains(rendered, "local all postgres peer") {
		t.Error("the superuser must be reachable by peer authentication on the Unix socket")
	}
	if strings.Contains(rendered, "trust") {
		t.Error("no rule may grant trust authentication")
	}
	if !strings.Contains(rendered, "host all pgelastic_repl 10.244.0.0/16 scram-sha-256") {
		t.Error("slot synchronisation opens an ordinary connection, not a replication one, " +
			"so the replication role must be admitted to databases too")
	}
}

func TestIncludeDirectivesOrderOverrideLast(t *testing.T) {
	custom := strings.Index(IncludeDirectives, CustomConfFile)
	override := strings.Index(IncludeDirectives, OverrideConfFile)
	if custom < 0 || override < 0 || override < custom {
		t.Fatalf("include directives = %q, want override.conf included after custom.conf", IncludeDirectives)
	}
}
