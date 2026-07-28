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

// Package migration moves one tenant's database from the instance it is bound to onto
// another instance in the same pool, through an eight-phase machine whose every departure
// from the happy path leaves the tenant serving from the source.
//
// The package is split so that the decision and the effect are testable apart: Decide is a
// pure function of an Observation, and every effect reaches PostgreSQL through the SQL and
// Shell ports rather than through a connection of its own.
package migration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Endpoint names one database on one member of one PgInstance.
type Endpoint struct {
	// Namespace and Instance identify the PgInstance.
	Namespace string
	// Instance is the PgInstance name.
	Instance string
	// Member addresses one specific pod. Empty means whichever member is currently the
	// primary, which is the only correct target for anything that writes or that has to
	// read a value the standbys do not have.
	Member string
	// Database is the database the session is opened on.
	Database string
}

// WithDatabase returns the same endpoint against a different database.
func (e Endpoint) WithDatabase(database string) Endpoint {
	e.Database = database
	return e
}

// WithMember returns the same endpoint against one named member.
func (e Endpoint) WithMember(member string) Endpoint {
	e.Member = member
	return e
}

// Row is one result row rendered as text.
//
// Every value a migration reads back is a count, an LSN, a digest, a catalog name or a
// GUC, so a text projection loses nothing. It also lets one port be satisfied both by a
// pgx connection and by psql running inside the pod, which is what an operator running
// outside the cluster needs: a Pod CIDR is not routable from a developer's machine.
type Row []string

// SQL is the single port every migration step reaches PostgreSQL through.
//
// A statement may be a script of several statements separated by semicolons, of which at
// most one produces rows. The verifier needs that: the settings that pin text rendering have
// to be in force on the same session as the digest they make deterministic, and splitting
// them into a separate call would leave the port free to serve the two halves from different
// sessions.
//
// Every implementation reaches PostgreSQL as the bootstrap superuser over the member's Unix
// socket. That is not an implementation detail to be optimised away later: the superuser has
// no password at all by design, so there is no TCP route to it, and a migration needs
// superuser to create a subscription and to fence a database.
type SQL interface {
	Exec(ctx context.Context, at Endpoint, statement string) error
	Query(ctx context.Context, at Endpoint, statement string) ([]Row, error)
}

// Shell runs one of the PostgreSQL command-line tools inside a member's container. It is
// separate from SQL because pg_dump and pg_restore are processes rather than statements,
// and because the offline path's parallelism is a property of those processes.
type Shell interface {
	Run(ctx context.Context, at Endpoint, argv []string) ([]byte, error)
}

// ErrNoRows reports a scalar query that returned nothing. It is distinct from a zero
// result: a count of zero is an answer, an absent row means the object being asked about
// does not exist.
var ErrNoRows = errors.New("query returned no rows")

func scalar(ctx context.Context, sql SQL, at Endpoint, statement string) (string, error) {
	rows, err := sql.Query(ctx, at, statement)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("%w: %s", ErrNoRows, statement)
	}
	return rows[0][0], nil
}

func scalarInt64(ctx context.Context, sql SQL, at Endpoint, statement string) (int64, error) {
	value, err := scalar(ctx, sql, at, statement)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("expected an integer from %s: %w", statement, err)
	}
	return parsed, nil
}

// firstColumn projects the first column of every row, which is the shape of every "which
// objects are wrong" query the preflight gate asks.
func firstColumn(ctx context.Context, sql SQL, at Endpoint, statement string) ([]string, error) {
	rows, err := sql.Query(ctx, at, statement)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		values = append(values, row[0])
	}
	return values, nil
}

// QuoteIdentifier renders a name for use as a SQL identifier.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteQualified renders a schema-qualified relation name.
func QuoteQualified(schema, name string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(name)
}

// QuoteLiteral renders a value as a SQL string literal. standard_conforming_strings is on
// throughout pgelastic, so doubling the quote is the whole escape.
func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// UserSchemaPredicate is the filter every catalog query in this package shares: user
// schemas only, spelled once so the verifier and the preflight gate cannot disagree about
// what belongs to the tenant.
const UserSchemaPredicate = `n.nspname NOT IN ('pg_catalog', 'information_schema') ` +
	`AND n.nspname NOT LIKE 'pg\_toast%' AND n.nspname NOT LIKE 'pg\_temp%'`
