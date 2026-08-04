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

	maintenanceWorkMem      = "maintenance_work_mem"
	maintenanceWorkMemValue = "512MB"
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
		{"maintenance_work_mem belongs to the tenant", maintenanceWorkMem, OwnershipUser, ContextUser},
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
		userParameterName:   userParameterValue,
		GUCMaxConnections:   "5000",
		GUCAllowAlterSystem: "on",
		maintenanceWorkMem:  maintenanceWorkMemValue,
	})
	if len(kept) != 2 || kept[userParameterName] != userParameterValue || kept[maintenanceWorkMem] != maintenanceWorkMemValue {
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

// pg_hba matches the first record that fits, so this ordering is the whole of whether the
// metrics scrape can authenticate at all. The agent's OS identity is postgres; below "local
// all all peer" a socket connection asking for pgelastic_ops is checked against the peer
// identity postgres and refused, leaving the bootstrap superuser as the only reachable role.
func TestTheOpsRoleReachesTheSocketAboveThePeerCatchAll(t *testing.T) {
	// No sources at all: the record is a local one, so it must not depend on any CIDR
	// having been configured.
	rendered := RenderHBA(HBAConfig{OpsRole: "pgelastic_ops"})
	ops := strings.Index(rendered, "local all pgelastic_ops scram-sha-256")
	catchAll := strings.Index(rendered, "local all all peer")
	if ops < 0 {
		t.Fatalf("pg_hba = %q, want a local record for the ops role", rendered)
	}
	if catchAll < 0 || ops > catchAll {
		t.Errorf("pg_hba = %q, want the ops record above the peer catch-all", rendered)
	}
}

func TestIncludeDirectivesOrderOverrideLast(t *testing.T) {
	custom := strings.Index(IncludeDirectives, CustomConfFile)
	override := strings.Index(IncludeDirectives, OverrideConfFile)
	if custom < 0 || override < 0 || override < custom {
		t.Fatalf("include directives = %q, want override.conf included after custom.conf", IncludeDirectives)
	}
}

func TestUserParametersDropsNamesThatEscapeTheirLine(t *testing.T) {
	injected := "# pgelastic\nfsync = off\n#"
	kept, dropped := UserParameters(map[string]pgelasticv1alpha1.GUCValue{
		injected:              "1",
		"work_mem = '1MB'\nx": "1",
		maintenanceWorkMem:    maintenanceWorkMemValue,
	})
	if len(kept) != 1 || kept[maintenanceWorkMem] != maintenanceWorkMemValue {
		t.Errorf("kept = %v, want only the well-formed parameter", kept)
	}
	if len(dropped) != 2 {
		t.Errorf("dropped = %v, want both malformed names", dropped)
	}
}

func TestUserParametersDropsValuesThatEscapeTheirLine(t *testing.T) {
	kept, dropped := UserParameters(map[string]pgelasticv1alpha1.GUCValue{
		userParameterName: "8MB'\nfsync = off\n#",
	})
	if len(kept) != 0 {
		t.Errorf("kept = %v, want nothing: the value carries a second directive", kept)
	}
	if !slices.Equal(dropped, []string{"work_mem"}) {
		t.Errorf("dropped = %v, want work_mem", dropped)
	}
}

// A user parameter must not be able to reach a durability GUC that the ownership table
// blocks by name, which is exactly what a name or value carrying a newline would do.
func TestNoUserParameterCanRewriteABlockedDurabilitySetting(t *testing.T) {
	for name, value := range map[string]string{
		"#\nfsync = off\n#":         "1",
		"x\nfull_page_writes = off": "1",
		userParameterName:           "8MB'\nfsync = off\n#'",
		userParameterName + "2":     "8MB'\r\nsynchronous_commit = off\r\n#'",
	} {
		rendered := FormatSettings("custom", RenderCustomConf(InstanceConfig{
			Capacity:       DeriveCapacity(50, 4, 3, 4),
			UserParameters: map[string]string{name: value},
		}))
		for line := range strings.SplitSeq(rendered, "\n") {
			trimmed := strings.TrimSpace(line)
			for _, blocked := range []string{"fsync", "full_page_writes", GUCSynchronousCommit} {
				if !strings.HasPrefix(trimmed, blocked+" ") && !strings.HasPrefix(trimmed, blocked+"=") {
					continue
				}
				if strings.Contains(trimmed, "off") {
					t.Errorf("parameter %q = %q rendered %q", name, value, trimmed)
				}
			}
		}
	}
}

func TestRenderableParameterAcceptsTheNamesPostgresAccepts(t *testing.T) {
	for _, name := range []string{"work_mem", "_x", "pgelastic.primary_epoch", "a1"} {
		if !RenderableParameter(name, "1") {
			t.Errorf("RenderableParameter(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "1abc", "a.b.c", "a b", "a=b", "a'b", "a#b", ".x", "x."} {
		if RenderableParameter(name, "1") {
			t.Errorf("RenderableParameter(%q) = true, want false", name)
		}
	}
}

// IsOwned answers "does the operator have an opinion about this?" and IsPinned answers "may
// the user set it?". Those were the same question while every owned parameter was pinned, and
// a call site asking the wrong one now either drops an override that should have won or
// refuses a parameter the user is entitled to set.
func TestOwnershipSeparatesHavingAnOpinionFromForbiddingOne(t *testing.T) {
	// max_connections is the canonical Fixed parameter: it is the unit the pool's rating,
	// the reservation ledger, oversubscription and chargeback are all denominated in.
	if !IsPinned(GUCMaxConnections) {
		t.Error("max_connections is settable by a tenant, which hands it capacity the pool never sold")
	}
	if !IsOwned(GUCMaxConnections) {
		t.Error("max_connections is not owned at all")
	}

	// An unknown parameter is neither owned nor pinned - the table enumerates what pgelastic
	// takes, not what PostgreSQL offers.
	if IsOwned("some_extension.setting") || IsPinned("some_extension.setting") {
		t.Error("a parameter nobody owns was claimed by the ownership table")
	}

	for _, name := range PinnedNames() {
		if !IsOwned(name) {
			t.Errorf("%s is pinned but not owned, which cannot be true", name)
		}
	}
}

// The level exists so a computed value can be a default rather than a decision. If a Tuned
// parameter were dropped like a Fixed one, the override would be silently discarded and the
// whole level would be a no-op that looked like it worked.
func TestATunedParametersUserValueSurvivesIntoTheRenderedConfiguration(t *testing.T) {
	const tuned = "pgelastic_test.tuned"
	ownedParameters[tuned] = Owned{Ownership: OwnershipTuned, Context: ContextUser}
	t.Cleanup(func() { delete(ownedParameters, tuned) })

	if IsPinned(tuned) {
		t.Fatal("a Tuned parameter refuses the user value it exists to accept")
	}
	kept, dropped := UserParameters(map[string]pgelasticv1alpha1.GUCValue{tuned: "7"})
	if kept[tuned] != "7" {
		t.Errorf("the user's value was dropped: kept=%v dropped=%v", kept, dropped)
	}
}

// PostgreSQL 19 doubled max_locks_per_transaction's default to 128, and said in its own release
// note that settings "must now be doubled to match their capacity in previous releases". The
// lock table is max_locks_per_transaction x (max_connections + max_prepared_transactions), so
// carrying 18's literal onto a 19 postmaster halves the lock capacity of every instance - and
// the first symptom is "out of shared memory" on a tenant doing nothing unusual.
func TestTheLockTableKeepsItsCapacityAcrossAMajor(t *testing.T) {
	for _, testCase := range []struct {
		major int
		want  string
	}{
		{major: 0, want: "64"},
		{major: 18, want: "64"},
		{major: 19, want: "128"},
		{major: 20, want: "128"},
	} {
		settings := RenderCustomConf(InstanceConfig{Major: testCase.major})

		var rendered string
		for _, setting := range settings {
			if setting.Name == GUCMaxLocksPerTransaction {
				rendered = setting.Value
			}
		}
		if rendered != testCase.want {
			t.Errorf("major %d renders %s = %q, want %q",
				testCase.major, GUCMaxLocksPerTransaction, rendered, testCase.want)
		}
	}
}
