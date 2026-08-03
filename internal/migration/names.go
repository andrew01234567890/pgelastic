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
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// Object name prefixes. Every physical object a migration creates on PostgreSQL carries
// one, which is what lets the orphan sweeper recognise its own litter without keeping a
// catalog of its own - and a sweeper that has to trust a catalog it may have lost is
// exactly the sweeper that leaves a slot pinning the primary's WAL forever.
const (
	// PublicationPrefix marks a publication created on the source.
	PublicationPrefix = "pgelastic_pub_"
	// SlotPrefix marks the logical replication slot created on the source.
	SlotPrefix = "pgelastic_mig_"
	// SubscriptionPrefix marks the subscription created on the target.
	SubscriptionPrefix = "pgelastic_sub_"
)

// ObjectPrefixes is every prefix the sweeper looks for.
var ObjectPrefixes = []string{PublicationPrefix, SlotPrefix, SubscriptionPrefix}

// nameBudget bounds the transliterated part of a generated name. PostgreSQL identifiers
// are 63 bytes; the longest prefix is 14 and the digest suffix is 9, so 40 leaves room to
// spare and keeps a truncated name from ever colliding with an untruncated one.
const nameBudget = 40

// PublicationName is the publication one migration owns on its source.
func PublicationName(namespace, name string) string {
	return objectName(PublicationPrefix, namespace, name)
}

// SlotName is the logical replication slot one migration owns on its source.
func SlotName(namespace, name string) string {
	return objectName(SlotPrefix, namespace, name)
}

// SubscriptionName is the subscription one migration owns on its target.
func SubscriptionName(namespace, name string) string {
	return objectName(SubscriptionPrefix, namespace, name)
}

// objectName derives a legal, unique PostgreSQL object name from a namespaced object.
//
// The digest rather than the namespace carries uniqueness: two namespaces may hold
// migrations of the same name, and truncating a long name would otherwise let them collide
// on a slot - which would mean one migration dropping the other's slot during cleanup.
func objectName(prefix, namespace, name string) string {
	digest := sha256.Sum256([]byte(namespace + "/" + name))
	return prefix + transliterate(name, nameBudget) + "_" + hex.EncodeToString(digest[:4])
}

// transliterate reduces a Kubernetes name to the character set PostgreSQL object names
// admit without quoting, so that every generated name can be spelled unquoted in the
// catalog queries the sweeper runs.
//
// The budget is a parameter because a name built from two Kubernetes names has to divide
// 63 bytes between them, and a name built from one does not.
func transliterate(name string, budget int) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(name) {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			builder.WriteRune(character)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= budget {
			break
		}
	}
	return builder.String()
}

// BackendRolePrefix marks every role pgelastic owns on a tenant's behalf.
const BackendRolePrefix = "pgt_"

// BackendRoleName is the cluster-global name of the role a tenant's backend sessions run as.
//
// Namespaced because PostgreSQL roles are cluster-global and pg_authid is a shared catalog, so
// two tenants that happen to choose the same spec.owner would otherwise share one role - a
// silent privilege union with no error, and, once tenant roles carry credentials, a merge of
// two identities. Azure recovers the same property by containment: a database user there is
// "independent (in all aspects) from a user who has the same name and the same password in
// another database in the same server". PostgreSQL has no containment to lean on, so the name
// has to carry it.
//
// The digest is over the tenant's namespace and name rather than over spec.owner, so editing
// the readable part never renames the role that owns every object in the database. Truncation
// therefore cannot cause a collision: a fixed-width digest is the last thing in the name, and
// identifiers are silently truncated at 63 bytes rather than rejected.
func BackendRoleName(namespace, name string) string {
	return objectName(BackendRolePrefix, namespace, name)
}

// SchemaStampPrefix marks a target database whose schema copy has committed. It is a
// prefix rather than an exact value because any pgelastic stamp answers the only question
// asked of it - has a complete copy of this tenant's schema already been applied here -
// and a migration that refused to recognise another's stamp would try to copy on top of it.
const SchemaStampPrefix = "pgelastic:schema-copied-by:"

// SchemaStamp is the comment one migration writes on its target database, in the same
// transaction that applies the schema.
func SchemaStamp(namespace, name string) string {
	return SchemaStampPrefix + namespace + "/" + name
}

// ScratchDir is where the offline path writes its dump. It sits on the data volume but
// outside PGDATA, so the directory PostgreSQL owns stays free of files it did not create
// while the dump still lands on the volume whose headroom preflight checked.
const ScratchDir = provision.DataMountPath + "/pgelastic-migration"

// DumpDir is the per-migration dump directory under ScratchDir.
func DumpDir(namespace, name string) string {
	return ScratchDir + "/" + transliterate(namespace, nameBudget) + "_" +
		transliterate(name, nameBudget)
}

// TenantUserRolePrefix marks every role pgelastic owns on behalf of one of a tenant's logins.
//
// Distinct from BackendRolePrefix so that \du and pg_stat_activity separate a tenant's owner
// from its logins at a glance, and so that no derivation of one can ever equal a derivation of
// the other however the readable halves are truncated. "pgt_" is not a prefix of "pgtu_", so
// anything matching on either prefix still partitions the two cleanly.
const TenantUserRolePrefix = "pgtu_"

// loginNameBudget is what each readable half of a login's role name may spend. The layout is
// 5 + 20 + 1 + 20 + 1 + 8 = 55, which leaves the digest inside 63 bytes for any pair of names
// Kubernetes admits - and the digest being last is what makes truncation impossible rather than
// merely unlikely.
const loginNameBudget = 20

// TenantUserRoleName is the cluster-global name of one of a tenant's logins.
//
// Namespaced for the same reason the tenant's own role is: two tenants may each have a user
// called `app`, and Azure's contained-user model requires those to be wholly independent
// identities. PostgreSQL cannot give that - pg_authid is shared - so the name has to carry it,
// or one tenant's `app` and another's become one role with the union of both their privileges.
//
// The digest is last and covers all three inputs. Both of those matter and neither is
// decorative: PostgreSQL truncates an over-long identifier silently rather than refusing it, so
// a digest anywhere but the end is a digest that can be cut off - and one covering only the
// tenant would leave two of that tenant's own logins separated by nothing but the readable
// suffix truncation eats first.
//
// Derived from the tenant's and the login's Kubernetes identity rather than from spec.userName,
// which is a wire-protocol string the client sends in its startup packet and must stay free to
// change without renaming a role that may already own objects.
func TenantUserRoleName(namespace, tenant, login string) string {
	digest := sha256.Sum256([]byte(namespace + "/" + tenant + "/" + login))
	return TenantUserRolePrefix +
		transliterate(tenant, loginNameBudget) + "_" +
		transliterate(login, loginNameBudget) + "_" +
		hex.EncodeToString(digest[:4])
}
