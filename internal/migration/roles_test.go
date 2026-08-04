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

package migration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	heldDatabase = "acme_prod"
	connect      = "CONNECT"
	alice        = "alice"
	bob          = "bob"
	carol        = "carol"
	temporary    = "TEMPORARY"
	liveInstance = "pg-live"
)

func heldTarget() Endpoint {
	return Endpoint{Namespace: "tenants", Instance: liveInstance, Database: heldDatabase}
}

func tenantRoles(names ...string) []RoleSpec {
	roles := make([]RoleSpec, 0, len(names))
	for _, name := range names {
		roles = append(roles, RoleSpec{Name: name})
	}
	return roles
}

// The roles to hold out are enumerated on the recovered instance, whose catalog is the
// source's as it stood at the restore point. A role dropped from the live cluster since is
// therefore a normal thing to find in that list, not a fault - and treating it as one used to
// abandon the hold-out partway, leaving whichever roles came before it revoked.
func TestARoleThatIsGoneFromTheLiveClusterIsSkipped(t *testing.T) {
	sql := newFakeSQL().answer("aclexplode",
		Row{alice, connect}, Row{alice, temporary}, Row{carol, connect})

	held, err := HoldTenantOut(context.Background(), sql, heldTarget(),
		tenantRoles(alice, bob, carol))

	if err != nil {
		t.Fatalf("holding out: %v", err)
	}
	if len(held) != 2 {
		t.Fatalf("held = %v, want only the two roles that still hold privileges", held)
	}
	for _, statement := range sql.statement {
		if strings.Contains(statement, `"bob"`) {
			t.Errorf("a statement named a role that does not exist: %q", statement)
		}
	}
	if !strings.Contains(sql.joined(), `REVOKE CONNECT, TEMPORARY ON DATABASE "acme_prod" FROM "alice"`) {
		t.Errorf("alice was not held out: %s", sql.joined())
	}
	if !strings.Contains(sql.joined(), `REVOKE CONNECT ON DATABASE "acme_prod" FROM "carol"`) {
		t.Errorf("carol was not held out: %s", sql.joined())
	}
}

// Readmission puts back what was read, per role. Granting a fixed CONNECT, TEMPORARY would
// hand TEMPORARY to a role whose owner had deliberately revoked it - the restore widening the
// database's privileges as a side effect of running, which nobody asked it to do.
func TestReadmissionPutsBackOnlyWhatWasHeld(t *testing.T) {
	sql := newFakeSQL().answer("aclexplode",
		Row{alice, connect}, Row{alice, temporary}, Row{carol, connect})

	held, err := HoldTenantOut(context.Background(), sql, heldTarget(),
		tenantRoles(alice, carol))
	if err != nil {
		t.Fatalf("holding out: %v", err)
	}
	if err := ReadmitTenant(context.Background(), sql, heldTarget(), held); err != nil {
		t.Fatalf("readmitting: %v", err)
	}

	if !strings.Contains(sql.joined(), `GRANT CONNECT, TEMPORARY ON DATABASE "acme_prod" TO "alice"`) {
		t.Errorf("alice did not get back both privileges: %s", sql.joined())
	}
	if !strings.Contains(sql.joined(), `GRANT CONNECT ON DATABASE "acme_prod" TO "carol"`) {
		t.Errorf("carol did not get CONNECT back: %s", sql.joined())
	}
	if strings.Contains(sql.joined(), `TEMPORARY ON DATABASE "acme_prod" TO "carol"`) {
		t.Errorf("carol was granted a privilege she never held: %s", sql.joined())
	}
}

// A hold-out that gives up on the first failing role leaves the roles it had already revoked
// from locked out of a live production database. Every role has to be attempted, and what was
// read has to come back regardless, because that is what readmission works from.
func TestAFailedHoldOutStillReportsEveryRoleItRead(t *testing.T) {
	sql := newFakeSQL().answer("aclexplode",
		Row{alice, connect}, Row{bob, connect}, Row{carol, connect}).
		fail(`FROM "bob"`, errors.New("permission denied"))

	held, err := HoldTenantOut(context.Background(), sql, heldTarget(),
		tenantRoles(alice, bob, carol))

	if err == nil {
		t.Fatal("a failing revoke was reported as success")
	}
	if len(held) != 3 {
		t.Fatalf("held = %v, want all three so readmission can undo the ones that did revoke", held)
	}
	if !strings.Contains(sql.joined(), `FROM "carol"`) {
		t.Errorf("carol was never attempted, so the hold-out did not hold: %s", sql.joined())
	}
}

// The same for readmission, and for the same reason: giving up on the first failure is how one
// unreachable role turns into several.
func TestReadmissionAttemptsEveryRole(t *testing.T) {
	sql := newFakeSQL().fail(`TO "bob"`, errors.New("permission denied"))
	held := []Held{
		{Role: alice, Privileges: []string{connect}},
		{Role: bob, Privileges: []string{connect}},
		{Role: carol, Privileges: []string{connect}},
	}

	err := ReadmitTenant(context.Background(), sql, heldTarget(), held)

	if err == nil {
		t.Fatal("a failing grant was reported as success")
	}
	if !strings.Contains(sql.joined(), `TO "carol"`) {
		t.Errorf("carol was never readmitted: %s", sql.joined())
	}
}

// A readmission that cannot be issued because the context is already dead is the case the
// whole hold-out/readmit pair exists for: the copy is killed by a rolled Pod or a lost lease,
// and both of those cancel the reconcile's context. Running the undo on that same context
// would leave the roles revoked on a live production database.
func TestReadmissionRunsOnADeadContext(t *testing.T) {
	sql := newFakeSQL()
	held := []Held{{Role: alice, Privileges: []string{connect}}}

	dead, cancel := context.WithCancel(context.Background())
	cancel()
	detached, stop := context.WithTimeout(context.WithoutCancel(dead), time.Minute)
	defer stop()

	if err := ReadmitTenant(detached, sql, heldTarget(), held); err != nil {
		t.Fatalf("readmitting on a detached context: %v", err)
	}
	if !strings.Contains(sql.joined(), `TO "alice"`) {
		t.Errorf("nothing was issued, so the role stays locked out: %s", sql.joined())
	}
	if detached.Err() != nil {
		t.Errorf("the detached context inherited the cancellation: %v", detached.Err())
	}
}
