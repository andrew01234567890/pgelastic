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
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/pgversion"
)

// Scrape pacing and bounds.
const (
	// DatabaseStatsTTL is how long one scrape's answer is served for.
	//
	// It exists so that a scrape interval shorter than the query costs cannot pile up. The
	// agent's observe tick is two seconds and drops to 250ms through a handover; without a
	// floor this batch would run on every one of them, and a batch that takes longer than the
	// interval starts the next one before the last has finished.
	DatabaseStatsTTL = 30 * time.Second
	// DatabaseStatsTimeout bounds one whole scrape - connecting, reading the version and
	// running the batch.
	//
	// It is short because the scrape runs inside the supervisor's observe tick, and a tick
	// that blocks stops this member republishing its own position. Nothing waits on these
	// numbers: a scrape that gives up leaves the previous reading in place and the operator
	// counts the pool as stale, which is a visible state rather than a silent stall.
	DatabaseStatsTimeout = 2 * time.Second
	// databaseStatsStatementTimeout is the server-side bound on the batch alone. The client
	// deadline above cannot stop a query that is already running inside PostgreSQL, so
	// without this an abandoned scrape leaves a backend holding a snapshot behind it.
	databaseStatsStatementTimeout = "1500ms"
)

// The databases initdb creates, named rather than written out wherever they are needed.
//
// maintenanceDatabase is spelled the same as the postmaster's executable and means something
// entirely different; templateDatabase is the one every tenant database is created from, and
// so the one whose ACL every tenant database inherits.
const (
	maintenanceDatabase = "postgres"
	pristineTemplate    = "template0"
	templateDatabase    = "template1"
)

// maintenanceDatabases are the databases that belong to pgelastic and its postmaster rather
// than to a tenant. Nothing places a tenant in them, so their counters describe the control
// plane's own bootstrap traffic and would be attributed to whichever tenant happened to be
// named the same.
var maintenanceDatabases = []string{maintenanceDatabase, pristineTemplate, templateDatabase}

// databaseStatsVersions is the set of PostgreSQL majors the batch below is correct on.
//
// It is unconstrained, and that is a finding rather than an omission. Every column the batch
// reads - the eight counters, numbackends, stats_reset - has been in pg_stat_database since
// long before 18 and is unchanged in 19, and pg_database_size has not moved either. There is
// nothing here to gate.
//
// The gate is wired anyway, and this is the declaration a query that does need one changes.
// pg_stat_wal lost four columns in 18 and pg_stat_subscription_stats renamed one in 19, so
// the second query added here is likely to be the first one that has majors to name; making
// it declare them then is a one-line change rather than the introduction of a mechanism.
var databaseStatsVersions = pgversion.Range{}

// DatabaseStatsQuery is the batch: one round trip that reads every tenant database's
// cumulative counters, its backend count, its size and when its statistics were last reset.
//
// It is one query rather than one per database on purpose. At the design point of ~200
// tenants per pool, a per-database round trip is 200 connections and 200 snapshots per
// scrape against an instance whose scarce resource is connections.
//
// tup_modified is the sum of the three row-level counters because pg_stat_database has no
// such column: what a placement decision wants is how much the tenant wrote, and inserts,
// updates and deletes are one fact split three ways for reasons that have nothing to do with
// capacity.
//
// Every catalog reference is schema-qualified even though search_path is pinned around it.
// The two protections are not redundant: the pin stops a tenant's own objects shadowing a
// catalog name, and the qualification means a future caller that forgets the pin still reads
// the catalog rather than whatever a tenant left in public.
const DatabaseStatsQuery = `SELECT d.datname,
       d.oid::bigint,
       s.numbackends,
       s.xact_commit,
       s.xact_rollback,
       s.blks_read,
       s.blks_hit,
       s.tup_returned,
       s.tup_fetched,
       s.tup_inserted + s.tup_updated + s.tup_deleted,
       s.deadlocks,
       s.stats_reset,
       pg_catalog.pg_database_size(d.oid)
  FROM pg_catalog.pg_stat_database s
  JOIN pg_catalog.pg_database d ON d.oid = s.datid
 WHERE NOT d.datistemplate
   AND d.datallowconn
   AND d.datname <> ALL($1::text[])
 ORDER BY d.datname`

// DatabaseScraper reads pg_stat_database on a bounded cadence and remembers the last answer.
//
// It authenticates as pgelastic_ops rather than as the bootstrap superuser. The role already
// holds pg_monitor, which is what makes pg_stat_database's counters visible for databases
// this connection is not in and what makes pg_database_size answerable for them; it holds
// nothing else, so a scrape is the least privileged connection the instance opens.
type DatabaseScraper struct {
	SocketDir string
	Port      int32
	// Password is the ops role's, from the instance's Secret by way of the agent's
	// environment. An empty one disables the scrape rather than falling back to the
	// superuser, because falling back would quietly restore exactly the identity this path
	// exists to avoid.
	Password string
	// TTL and Timeout default to DatabaseStatsTTL and DatabaseStatsTimeout.
	TTL     time.Duration
	Timeout time.Duration
	// Open dials the postmaster. Nil is the production path - the Unix socket, as
	// pgelastic_ops - and a test supplies its own so that the caching and the batch are
	// exercised as one thing rather than only where they meet.
	Open func(context.Context) (*pgx.Conn, error)

	mutex  sync.Mutex
	cached []provision.DatabaseReport
	// attemptedAt paces attempts rather than successes, so a failing scrape is retried at
	// the TTL instead of on every tick.
	attemptedAt time.Time
}

// Scrape returns the current readings, running the batch only if the cached ones have aged
// out. The boolean says whether the answer came from PostgreSQL this call, which is what
// distinguishes a genuinely fresh reading from one being served again.
func (s *DatabaseScraper) Scrape(ctx context.Context) ([]provision.DatabaseReport, bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Password == "" && s.Open == nil {
		return nil, false, nil
	}
	// The TTL paces attempts, not successes. Bounding only the success path leaves the
	// failure path unbounded, and the failure path is the one that piles up: a wrong
	// password, a pg_hba that has not rolled yet, a postmaster refusing connections - each
	// would re-dial on every observe tick, which is every two seconds at rest and every 250
	// milliseconds through a handover. A scrape that cannot connect must not become a
	// connection storm against a server that is already struggling.
	if !s.attemptedAt.IsZero() && time.Since(s.attemptedAt) < s.ttl() {
		return s.cached, false, nil
	}
	s.attemptedAt = time.Now()

	scrapeCtx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()

	reports, err := s.read(scrapeCtx)
	if err != nil {
		return s.cached, false, err
	}
	s.cached = reports
	return reports, true, nil
}

func (s *DatabaseScraper) ttl() time.Duration {
	if s.TTL > 0 {
		return s.TTL
	}
	return DatabaseStatsTTL
}

func (s *DatabaseScraper) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DatabaseStatsTimeout
}

// read opens one connection, runs the batch and closes it again.
//
// The connection is not kept. An agent that held one open would hold a backend slot on every
// member for the life of the pod, on an instance whose whole product is backend slots, to
// save a connect every thirty seconds.
func (s *DatabaseScraper) read(ctx context.Context) ([]provision.DatabaseReport, error) {
	open := s.Open
	if open == nil {
		open = func(dial context.Context) (*pgx.Conn, error) {
			return ConnectAsOps(dial, s.SocketDir, s.Port, maintenanceDatabase, s.Password)
		}
	}
	conn, err := open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	return ReadDatabaseStats(ctx, conn)
}

// ConnectAsOps opens one connection over the Unix socket as pgelastic_ops.
//
// The password is set on the parsed configuration rather than written into a DSN, so a
// generated secret containing a quote or a space cannot change the meaning of the
// keyword/value string it would otherwise have been pasted into.
func ConnectAsOps(
	ctx context.Context,
	socketDir string,
	port int32,
	database, password string,
) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d user=%s dbname=%s",
		socketDir, port, provision.OpsRole, database))
	if err != nil {
		return nil, err
	}
	config.Password = password
	return pgx.ConnectConfig(ctx, config)
}

// ReadDatabaseStats runs the batch inside one read-only transaction.
//
// Three disciplines, each of which has bitten somebody: the transaction is read only so a
// scrape cannot write; search_path is pinned to the catalog with pg_temp last, so a tenant
// that created its own pg_stat_database cannot have it read instead; and statement_timeout
// bounds the batch inside the server, because a client that gives up does not stop a query
// that has already started.
func ReadDatabaseStats(ctx context.Context, conn *pgx.Conn) ([]provision.DatabaseReport, error) {
	transaction, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	if _, err := transaction.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp;`+
			`SET LOCAL statement_timeout = '`+databaseStatsStatementTimeout+`'`); err != nil {
		return nil, err
	}

	version, err := serverVersion(ctx, transaction)
	if err != nil {
		return nil, err
	}
	if !databaseStatsVersions.Includes(version) {
		return nil, nil
	}

	rows, err := transaction.Query(ctx, DatabaseStatsQuery, maintenanceDatabases)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reports []provision.DatabaseReport
	for rows.Next() {
		var report provision.DatabaseReport
		var statsReset *time.Time
		if err := rows.Scan(
			&report.Name,
			&report.OID,
			&report.NumBackends,
			&report.XactCommit,
			&report.XactRollback,
			&report.BlksRead,
			&report.BlksHit,
			&report.TupReturned,
			&report.TupFetched,
			&report.TupModified,
			&report.Deadlocks,
			&statsReset,
			&report.SizeBytes,
		); err != nil {
			return nil, err
		}
		if statsReset != nil {
			report.StatsReset = &metav1.Time{Time: *statsReset}
		}
		reports = append(reports, report)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return reports, transaction.Commit(ctx)
}

// serverVersion reads server_version_num, which is what the version gate is evaluated
// against. version() is not orderable - during a beta cycle it carries a suffix, and the rest
// of the time it carries whatever the distribution built it with.
func serverVersion(ctx context.Context, transaction pgx.Tx) (pgversion.Version, error) {
	var num int
	if err := transaction.QueryRow(ctx,
		`SELECT current_setting('server_version_num')::int`).Scan(&num); err != nil {
		return pgversion.Version{}, err
	}
	return pgversion.FromNum(num)
}
