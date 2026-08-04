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
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/verify/pgtest"
)

// The scrape's privileges are a claim about PostgreSQL, and a claim about PostgreSQL that is
// only ever run against a stub is how this milestone's sibling shipped a privilege check by
// the wrong name. So the batch is run against a real server, as a role holding exactly what
// bootstrap grants pgelastic_ops and nothing else.
const (
	scrapeRole     = provision.OpsRole
	scrapePassword = "scrape-password"
	tenantRole     = "tenant_alpha"
	tenantDatabase = "tenant_alpha_db"
)

// realPostgres starts a server, creates the ops role with exactly the grants bootstrap gives
// it, and creates one tenant database with some traffic on it.
//
// CONNECT on the tenant database is deliberately never granted to the ops role, and PUBLIC's
// is revoked, so that this proves the thing worth proving: the scrape reads a tenant's
// counters and its size without being admitted to it. pg_monitor carries pg_read_all_stats,
// which is what makes pg_database_size answerable for a database this connection is not in.
func realPostgres(t *testing.T) string {
	t.Helper()
	dsn := pgtest.Start(t)

	ctx := t.Context()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting as the bootstrap superuser: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	for _, statement := range []string{
		`CREATE ROLE ` + scrapeRole + ` LOGIN PASSWORD '` + scrapePassword + `'`,
		`GRANT pg_monitor, pg_signal_backend, pg_use_reserved_connections TO ` + scrapeRole,
		`CREATE ROLE ` + tenantRole + ` LOGIN PASSWORD 'tenant'`,
		`CREATE DATABASE ` + tenantDatabase + ` OWNER ` + tenantRole,
		`REVOKE CONNECT ON DATABASE ` + tenantDatabase + ` FROM PUBLIC`,
		`GRANT CONNECT ON DATABASE ` + tenantDatabase + ` TO ` + tenantRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing the connection string: %v", err)
	}
	config.Database = tenantDatabase
	config.User, config.Password = tenantRole, "tenant"
	tenant, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connecting as the tenant: %v", err)
	}
	defer func() { _ = tenant.Close(ctx) }()
	for _, statement := range []string{
		`CREATE TABLE ledger (id int PRIMARY KEY, note text)`,
		`INSERT INTO ledger SELECT g, 'row ' || g FROM generate_series(1, 500) g`,
		`UPDATE ledger SET note = note || '!' WHERE id % 2 = 0`,
		`DELETE FROM ledger WHERE id % 100 = 0`,
		`SELECT count(*) FROM ledger`,
	} {
		if _, err := tenant.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	return dsn
}

// connectAs opens a connection to one database. An empty user keeps whatever the container's
// own connection string carries, which is its bootstrap superuser.
func connectAs(t *testing.T, ctx context.Context, dsn, database, user, password string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing the connection string: %v", err)
	}
	config.Database = database
	if user != "" {
		config.User, config.Password = user, password
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connecting to %s as %s: %v", database, config.User, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(ctx)) })
	return conn
}

func findDatabase(reports []provision.DatabaseReport, name string) (provision.DatabaseReport, bool) {
	for _, report := range reports {
		if report.Name == name {
			return report, true
		}
	}
	return provision.DatabaseReport{}, false
}

// The whole of the batch, against a real server, on one container.
//
// One rather than one per specification, and the subtests are ordered rather than independent
// because two of them change the server for the ones after: the reset moves stats_reset for
// every later read, and the shadowing objects stay in the database the scrape connects to.
// Four containers were four PostgreSQL images starting at once inside a suite that already
// starts others, and it flaked in exactly the way container contention flakes - a wait
// strategy timing out sixty seconds into a test that takes two.
func TestTheScrapeAgainstARealServer(t *testing.T) {
	dsn := realPostgres(t)

	t.Run("reads a tenant's counters without being admitted to it", func(t *testing.T) {
		theBatchReadsATenantsCounters(t, dsn)
	})
	// Before the reset, because it asserts on counters the reset would zero.
	t.Run("serves a second scrape inside the TTL from its cache", func(t *testing.T) {
		aSecondScrapeInsideTheTTLDoesNotTouchTheServer(t, dsn)
	})
	t.Run("carries a reset as an instant rather than losing it", func(t *testing.T) {
		aResetIsCarriedAsAnInstant(t, dsn)
	})
	// Last, because the objects it creates stay behind in the scrape's own database.
	t.Run("cannot have a tenant's own object stand in for the catalog", func(t *testing.T) {
		aTenantsOwnObjectCannotStandInForTheCatalog(t, dsn)
	})
}

// Everything the batch claims, asserted against the server it claims it of: a monitoring role
// with no CONNECT on the tenant database reads its counters, its backend count and its size,
// and the maintenance databases are not in the answer.
func theBatchReadsATenantsCounters(t *testing.T, dsn string) {
	ctx := t.Context()
	conn := connectAs(t, ctx, dsn, maintenanceDatabase, scrapeRole, scrapePassword)

	reports, err := ReadDatabaseStats(ctx, conn)
	if err != nil {
		t.Fatalf("reading pg_stat_database as %s: %v", scrapeRole, err)
	}

	tenant, found := findDatabase(reports, tenantDatabase)
	if !found {
		t.Fatalf("the scrape returned %+v, want the tenant database among them", reports)
	}
	if tenant.OID == 0 {
		t.Error("the tenant database has no oid, so a drop and recreate could not be told apart")
	}
	if tenant.XactCommit <= 0 {
		t.Errorf("xact_commit = %d, want the tenant's committed transactions", tenant.XactCommit)
	}
	// 500 inserted, 250 updated and 5 deleted: the assertion is that all three row-level
	// counters are summed rather than one of them being reported as the whole.
	if tenant.TupModified < 750 {
		t.Errorf("tup_modified = %d, want at least the 755 rows the tenant wrote - "+
			"inserts, updates and deletes have to be summed", tenant.TupModified)
	}
	if tenant.TupReturned <= 0 || tenant.TupFetched <= 0 || tenant.BlksHit <= 0 {
		t.Errorf("tup_returned = %d, tup_fetched = %d, blks_hit = %d, want all three read",
			tenant.TupReturned, tenant.TupFetched, tenant.BlksHit)
	}
	if tenant.SizeBytes <= 0 {
		t.Error("pg_database_size answered zero: pg_monitor carries pg_read_all_stats, which " +
			"is what makes a database this connection cannot enter measurable at all")
	}
	if tenant.StatsReset != nil {
		t.Errorf("stats_reset = %v on a server nobody has reset", tenant.StatsReset)
	}

	for _, excluded := range maintenanceDatabases {
		if _, present := findDatabase(reports, excluded); present {
			t.Errorf("the scrape returned %s, which holds no tenant", excluded)
		}
	}
}

// A reset is the case the accumulator's whole delta machinery exists for, and it is only
// detectable if the scrape carries the instant. A fresh server reports null, so the field
// has to survive both states.
func aResetIsCarriedAsAnInstant(t *testing.T, dsn string) {
	ctx := t.Context()
	conn := connectAs(t, ctx, dsn, maintenanceDatabase, scrapeRole, scrapePassword)

	before := time.Now().Add(-time.Minute)
	admin := connectAs(t, ctx, dsn, tenantDatabase, "", "")
	if _, err := admin.Exec(ctx, `SELECT pg_stat_reset()`); err != nil {
		t.Fatalf("resetting the tenant's statistics: %v", err)
	}

	reports, err := ReadDatabaseStats(ctx, conn)
	if err != nil {
		t.Fatalf("reading pg_stat_database: %v", err)
	}
	tenant, found := findDatabase(reports, tenantDatabase)
	if !found {
		t.Fatalf("the scrape returned %+v, want the tenant database among them", reports)
	}
	if tenant.StatsReset == nil {
		t.Fatal("stats_reset is absent after pg_stat_reset(), so a reset is invisible and " +
			"every counter after it reads as a decrease")
	}
	if !tenant.StatsReset.After(before) {
		t.Errorf("stats_reset = %v, want an instant after the test started", tenant.StatsReset)
	}
}

// CVE-2018-1058 in miniature: a tenant that creates its own object named after a catalog must
// not have it read in the catalog's place. Both halves of the protection are in play here -
// the pinned search_path and the schema qualification - and the object is created in the
// database the scrape itself connects to, which is the only place a search_path could reach.
func aTenantsOwnObjectCannotStandInForTheCatalog(t *testing.T, dsn string) {
	ctx := t.Context()

	admin := connectAs(t, ctx, dsn, maintenanceDatabase, "", "")
	for _, statement := range []string{
		`CREATE TABLE public.pg_stat_database (datid oid, numbackends int)`,
		`CREATE TABLE public.pg_database (oid oid, datname name)`,
		`CREATE FUNCTION public.pg_database_size(oid) RETURNS bigint AS $$ SELECT 0::bigint $$ LANGUAGE sql`,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	conn := connectAs(t, ctx, dsn, maintenanceDatabase, scrapeRole, scrapePassword)
	reports, err := ReadDatabaseStats(ctx, conn)
	if err != nil {
		t.Fatalf("reading pg_stat_database: %v", err)
	}
	tenant, found := findDatabase(reports, tenantDatabase)
	if !found {
		t.Fatalf("the scrape returned %+v: a table in public was read instead of the catalog",
			reports)
	}
	if tenant.SizeBytes <= 0 {
		t.Error("pg_database_size answered zero, which is what the shadowing function in " +
			"public returns: the qualified name did not win")
	}
}

// The TTL is what stops a scrape interval shorter than the query cost piling up, and it is
// asserted through the scraper rather than on its fields so that the caching and the batch
// are one thing.
func aSecondScrapeInsideTheTTLDoesNotTouchTheServer(t *testing.T, dsn string) {
	ctx := t.Context()

	dials := 0
	scraper := &DatabaseScraper{
		TTL: time.Hour,
		Open: func(dial context.Context) (*pgx.Conn, error) {
			dials++
			config, err := pgx.ParseConfig(dsn)
			if err != nil {
				return nil, err
			}
			config.Database, config.User, config.Password = maintenanceDatabase, scrapeRole, scrapePassword
			return pgx.ConnectConfig(dial, config)
		},
	}

	first, fresh, err := scraper.Scrape(ctx)
	if err != nil || !fresh {
		t.Fatalf("the first scrape = (%v, %v), want a fresh reading", fresh, err)
	}
	second, fresh, err := scraper.Scrape(ctx)
	if err != nil || fresh {
		t.Fatalf("the second scrape = (%v, %v), want the cached reading", fresh, err)
	}
	if dials != 1 {
		t.Errorf("the scraper dialled %d times inside one TTL, want 1", dials)
	}
	if len(first) == 0 || len(second) != len(first) {
		t.Errorf("the cached reading holds %d databases and the fresh one %d",
			len(second), len(first))
	}

	scraper.TTL = time.Nanosecond
	if _, fresh, err = scraper.Scrape(ctx); err != nil || !fresh {
		t.Fatalf("the scrape after the TTL expired = (%v, %v), want a fresh reading", fresh, err)
	}
	if dials != 2 {
		t.Errorf("the scraper dialled %d times over two TTLs, want 2", dials)
	}
}
