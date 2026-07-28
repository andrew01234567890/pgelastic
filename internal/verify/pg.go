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

package verify

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

// DefaultTable is the relation the patroni-set workload writes to.
const DefaultTable = "set"

var identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// quoteIdentifier rejects anything that is not a plain lower-case identifier rather than
// attempting to escape it: the table name reaches SQL by string concatenation because it
// is a relation name, and a verifier that can be talked into running arbitrary SQL cannot
// be trusted to report on durability.
func quoteIdentifier(name string) (string, error) {
	if !identifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid table name %q: must match %s", name, identifierPattern)
	}
	return `"` + name + `"`, nil
}

// EnsureSchema creates the set relation if it does not already exist.
func EnsureSchema(ctx context.Context, conn *pgx.Conn, table string) error {
	ident, err := quoteIdentifier(table)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+ident+" (value bigint PRIMARY KEY)")
	return err
}

// ErrNotPrimary reports that the endpoint being read is in recovery. R must be read from
// the surviving primary; reading a replica would let replication lag masquerade as a lost
// commit.
var ErrNotPrimary = errors.New("endpoint is in recovery, not a primary")

// ReadSet reads R. When requirePrimary is set the connection is rejected unless it is on
// a node out of recovery.
func ReadSet(ctx context.Context, conn *pgx.Conn, table string, requirePrimary bool) ([]int64, error) {
	ident, err := quoteIdentifier(table)
	if err != nil {
		return nil, err
	}
	if requirePrimary {
		var inRecovery bool
		if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
			return nil, err
		}
		if inRecovery {
			return nil, ErrNotPrimary
		}
	}
	rows, err := conn.Query(ctx, "SELECT value FROM "+ident+" ORDER BY value")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	observed := make([]int64, 0)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		observed = append(observed, v)
	}
	return observed, rows.Err()
}
