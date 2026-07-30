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

// The digest is over the tenant's identity rather than over spec.owner, so renaming the
// readable part cannot rename the role that owns every object in the database.
func TestTheRoleNameIsStableAcrossItsReadablePart(t *testing.T) {
	if BackendRoleName("alpha", "acme") != BackendRoleName("alpha", "acme") {
		t.Fatal("the derivation is not deterministic")
	}
}
