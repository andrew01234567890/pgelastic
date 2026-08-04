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

package tenantuser_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/tenantuser"
	"github.com/andrew01234567890/pgelastic/internal/verify/pgtest"
)

// The catalog is asked real questions here because the stub in tenantuser_test.go answers a
// canned row and never parses the SQL - it says so itself. Every defect in these queries is
// therefore invisible to the unit tests by construction, and the first thing that sees one is
// a 120-second e2e timeout with no log line.

// A login's first observation is of a role that does not exist. That is not an error state:
// it is the state the reconcile exists to leave, and `Exists: false` is the answer that makes
// Ensure issue CREATE ROLE.
func TestObserveAnswersForARoleThatHasNeverBeenCreated(t *testing.T) {
	sql, database := connect(t)
	spec := tenantuser.Spec{Role: "pgtu_never_created", Database: database}

	state, err := tenantuser.Observe(t.Context(), sql, tenantuser.Endpoint{}, spec)
	if err != nil {
		t.Fatalf("observing a login whose role has never been created: %v", err)
	}
	if state.Exists {
		t.Error("a role that was never created is reported as existing")
	}
	if state.MayConnect {
		t.Error("a role that does not exist is reported as holding CONNECT")
	}
}

// Ensure has to reach CREATE ROLE from that first observation, and the login it leaves has to
// hold CONNECT on its tenant's database and be settled against its own spec.
func TestEnsureCreatesTheLoginItObservedWasMissing(t *testing.T) {
	sql, database := connect(t)
	ctx := t.Context()
	// The tenant's own provisioning revokes CONNECT from PUBLIC, so the grant this package
	// issues is the only reason its login can reach the database. Without the revoke every
	// role answers "may connect" and the assertion below would pass on any code.
	if err := sql.Exec(ctx, tenantuser.Endpoint{},
		`REVOKE CONNECT ON DATABASE `+migration.QuoteIdentifier(database)+` FROM PUBLIC`); err != nil {
		t.Fatalf("revoking CONNECT from PUBLIC: %v", err)
	}
	spec := tenantuser.Spec{Role: "pgtu_created_by_ensure", Database: database, Login: true}

	state, err := tenantuser.Ensure(ctx, sql, tenantuser.Endpoint{}, spec)
	if err != nil {
		t.Fatalf("provisioning a login: %v", err)
	}
	if !state.Exists {
		t.Fatal("Ensure returned without the role existing")
	}
	if !state.CanLogin {
		t.Error("a login spec produced a role that cannot log in")
	}
	if !state.MayConnect {
		t.Error("the login was not granted CONNECT on its tenant's database")
	}
	if !state.Settled(spec) {
		t.Errorf("a freshly provisioned login is not settled against its own spec: %+v", state)
	}
}

// connect returns a SQL port onto a throwaway PostgreSQL, and the database it opened.
func connect(t *testing.T) (migration.SQL, string) {
	t.Helper()
	dsn := pgtest.Start(t)

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing the test DSN: %v", err)
	}
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("connecting to the test PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("closing the test connection: %v", err)
		}
	})
	return directSQL{conn: conn}, config.Database
}

// directSQL is the migration.SQL port over one ordinary connection. The production
// implementation execs psql inside a member Pod; what both share, and all these tests need, is
// a real PostgreSQL parsing and executing the statement text this package builds.
type directSQL struct {
	conn *pgx.Conn
}

func (s directSQL) Exec(ctx context.Context, _ migration.Endpoint, statement string) error {
	_, err := s.conn.Exec(ctx, statement)
	return err
}

func (s directSQL) Query(ctx context.Context, _ migration.Endpoint, statement string) ([]migration.Row, error) {
	rows, err := s.conn.Query(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var answer []migration.Row
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(migration.Row, 0, len(values))
		for column, value := range values {
			text, ok := value.(string)
			if !ok {
				// Every column these queries project is already text, so anything else is
				// the query having changed shape rather than a conversion to paper over.
				return nil, fmt.Errorf("column %d of %q is %T, not text", column, statement, value)
			}
			row = append(row, text)
		}
		answer = append(answer, row)
	}
	return answer, rows.Err()
}
