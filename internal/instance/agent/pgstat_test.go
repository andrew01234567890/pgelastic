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
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// A tenant's own object must not be able to stand in for the catalog the scrape reads. The
// search_path pin is set at run time and is asserted against a real server elsewhere; what is
// checkable here is the second half of the protection, which is that every catalog reference
// names its schema whether or not the pin is in place.
func TestTheBatchNamesTheSchemaOfEveryCatalogItReads(t *testing.T) {
	query := DatabaseStatsQuery
	for _, reference := range []string{
		"pg_catalog.pg_stat_database",
		"pg_catalog.pg_database",
		"pg_catalog.pg_database_size(",
	} {
		if !strings.Contains(query, reference) {
			t.Errorf("the batch does not reference %s, so a tenant object of that name could "+
				"be read instead:\n%s", reference, query)
		}
	}
	for _, bare := range []string{" pg_stat_database", " pg_database ", " pg_database_size("} {
		if strings.Contains(query, bare) {
			t.Errorf("the batch carries an unqualified %q:\n%s", strings.TrimSpace(bare), query)
		}
	}
}

// The maintenance databases carry the control plane's own bootstrap traffic and hold no
// tenant, so counting them would attribute the operator's transactions to whichever tenant
// was named the same.
func TestTheBatchLeavesOutEveryDatabaseNoTenantIsIn(t *testing.T) {
	query := DatabaseStatsQuery
	if !strings.Contains(query, "NOT d.datistemplate") {
		t.Errorf("the batch does not exclude template databases:\n%s", query)
	}
	if !strings.Contains(query, "d.datallowconn") {
		t.Errorf("the batch does not exclude databases that refuse connections:\n%s", query)
	}
	if !strings.Contains(query, "d.datname <> ALL($1::text[])") {
		t.Errorf("the maintenance databases are not excluded by parameter, which is what "+
			"keeps a database name out of the SQL text:\n%s", query)
	}
	want := []string{maintenanceDatabase, pristineTemplate, templateDatabase}
	if !slices.Equal(maintenanceDatabases, want) {
		t.Errorf("the scrape excludes %v, want %v", maintenanceDatabases, want)
	}
}

// The report is the only channel these numbers travel on, so a field that does not survive
// JSON is a counter that silently reads as zero on the far side - which is indistinguishable
// from a tenant that did nothing.
func TestEveryDatabaseCounterSurvivesTheWire(t *testing.T) {
	reset := metav1.NewTime(time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC))
	sent := provision.MemberReport{
		Member: "pg-1",
		Databases: []provision.DatabaseReport{{
			Name:         "tenant-alpha",
			OID:          16385,
			NumBackends:  7,
			XactCommit:   1,
			XactRollback: 2,
			BlksRead:     3,
			BlksHit:      4,
			TupReturned:  5,
			TupFetched:   6,
			TupModified:  7,
			Deadlocks:    8,
			StatsReset:   &reset,
			SizeBytes:    9,
		}},
	}

	encoded, err := json.Marshal(sent)
	if err != nil {
		t.Fatalf("encoding the report: %v", err)
	}
	var received provision.MemberReport
	if err := json.Unmarshal(encoded, &received); err != nil {
		t.Fatalf("decoding the report: %v", err)
	}
	if len(received.Databases) != 1 {
		t.Fatalf("the report carries %d databases, want 1", len(received.Databases))
	}
	// The reset instant is compared on its own because metav1.Time decodes into the local
	// zone, so two values describing the same instant are not the same struct.
	arrived, want := received.Databases[0], sent.Databases[0]
	if arrived.StatsReset == nil || !arrived.StatsReset.Time.Equal(want.StatsReset.Time) {
		t.Errorf("statsReset arrived as %v, want %v", arrived.StatsReset, want.StatsReset)
	}
	arrived.StatsReset, want.StatsReset = nil, nil
	if !reflect.DeepEqual(arrived, want) {
		t.Errorf("the database arrived as %+v, want %+v", arrived, want)
	}
}

// A member that has never scraped reports nothing rather than an empty list. The two are
// different facts: no databases means the member holds no tenants, and no reading means
// nobody knows - which is what the operator counts as stale.
func TestAMemberWithNoReadingReportsNoDatabasesRatherThanNone(t *testing.T) {
	report := MemberReportOf("pg-1", ProbeState{})
	if report.Databases != nil {
		t.Errorf("databases = %+v, want nil until a scrape has succeeded", report.Databases)
	}

	scraped := []provision.DatabaseReport{{Name: "tenant-alpha", OID: 16385}}
	report = MemberReportOf("pg-1", ProbeState{Databases: scraped})
	if !reflect.DeepEqual(report.Databases, scraped) {
		t.Errorf("databases = %+v, want the last scrape %+v", report.Databases, scraped)
	}
}

// Without a password there is no way to authenticate as pgelastic_ops, and the one thing the
// scrape must not do in that case is fall back to the connection it already has - which is
// the bootstrap superuser, over peer, which is exactly the identity this path exists to keep
// off a metrics connection.
func TestAScrapeWithNoOpsPasswordDoesNothingRatherThanFallingBack(t *testing.T) {
	scraper := &DatabaseScraper{SocketDir: t.TempDir(), Port: 5432}
	reports, fresh, err := scraper.Scrape(t.Context())
	if err != nil || fresh || reports != nil {
		t.Errorf("scrape = (%+v, %v, %v), want no readings, not fresh and no error",
			reports, fresh, err)
	}
}
