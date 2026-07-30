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
	"strings"
	"testing"
)

// PostgreSQL truncates identifiers at 63 bytes silently rather than refusing them, so a name
// that overflows does not fail - it collides. The digest is last precisely so truncation
// cannot reach it.
func TestABackendRoleNameFitsTheIdentifierLimit(t *testing.T) {
	name := BackendRoleName(
		"a-namespace-name-that-is-about-as-long-as-kubernetes-permits",
		"a-tenant-name-that-is-also-about-as-long-as-kubernetes-permits")
	if len(name) > 63 {
		t.Fatalf("the derived role name is %d bytes: %s", len(name), name)
	}
	if !strings.HasPrefix(name, BackendRolePrefix) {
		t.Fatalf("the role is not identifiable as pgelastic's: %s", name)
	}
}

// Two tenants of the same name in different namespaces must not share a role. Roles are
// cluster-global, so sharing one is a privilege union - and once the role carries a
// credential, a merge of two identities.
func TestTwoTenantsOfTheSameNameGetDifferentRoles(t *testing.T) {
	if BackendRoleName("alpha", "acme") == BackendRoleName("beta", "acme") {
		t.Fatal("two tenants named acme in different namespaces derived one role")
	}
}

// The name is a function of the tenant's identity and nothing else. spec.owner is not an
// input, so editing it cannot rename the role that owns every object in the database - and two
// tenants in one namespace still get their own role.
func TestTheRoleNameIsAFunctionOfTheTenantIdentityAlone(t *testing.T) {
	acme := BackendRoleName("alpha", "acme")
	if acme != BackendRoleName("alpha", "acme") {
		t.Fatal("the derivation is not deterministic, so a reconcile could rename a live role")
	}
	if acme == BackendRoleName("alpha", "globex") {
		t.Fatal("two tenants in one namespace derived the same role")
	}
	if !strings.Contains(acme, "acme") {
		t.Fatalf("the role is not identifiable as the tenant's in \\du or pg_stat_activity: %s", acme)
	}
}

// Azure's contained-user model requires two same-named users in different databases to be
// wholly independent identities. PostgreSQL cannot give that - pg_authid is shared - so the
// name carries it, or one tenant's `app` and another's become one role holding the union of
// both their privileges.
func TestTwoTenantsUsersOfTheSameNameAreDifferentRoles(t *testing.T) {
	alpha := TenantUserRoleName("prod", "acme", "app")
	beta := TenantUserRoleName("prod", "globex", "app")
	if alpha == beta {
		t.Fatalf("two tenants' users called app derived one role: %s", alpha)
	}
	if len(alpha) > 63 {
		t.Fatalf("the derived role name is %d bytes: %s", len(alpha), alpha)
	}
	for _, want := range []string{"acme", "app"} {
		if !strings.Contains(alpha, want) {
			t.Fatalf("the role is not identifiable in pg_stat_activity: %s", alpha)
		}
	}
}

// A tenant's own users must be distinct from each other too, and from the tenant's owner role.
func TestATenantsUsersAreDistinctFromEachOtherAndFromItsOwner(t *testing.T) {
	app := TenantUserRoleName("prod", "acme", "app")
	reporting := TenantUserRoleName("prod", "acme", "reporting")
	owner := BackendRoleName("prod", "acme")
	if app == reporting || app == owner || reporting == owner {
		t.Fatalf("a tenant's roles collided: %s %s %s", app, reporting, owner)
	}
}
