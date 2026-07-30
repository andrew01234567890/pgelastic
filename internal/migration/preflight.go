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
	"fmt"
	"slices"
	"strings"
)

// CheckName identifies one preflight gate.
type CheckName string

const (
	// CheckReplicaIdentity is the gate logical replication silently fails without: an
	// UPDATE or DELETE against a table with no replica identity is dropped on the target
	// and reported nowhere.
	CheckReplicaIdentity CheckName = "ReplicaIdentity"
	// CheckPreparedTransactions refuses a source holding a prepared transaction. It pins
	// the oldest xmin, cannot be replicated, and cannot be drained by quiescing the tenant.
	CheckPreparedTransactions CheckName = "PreparedTransactions"
	// CheckSourceUtilization refuses to start while the source is busy, because logical
	// decoding consumes exactly the capacity the move exists to relieve.
	CheckSourceUtilization CheckName = "SourceUtilization"
	// CheckCollationContract compares the two databases' text-handling identity. A move
	// across a collation difference produces indexes silently inconsistent with their heap
	// ordering: wrong results, no error.
	CheckCollationContract CheckName = "CollationContract"
	// CheckStorageHeadroom requires room for the tenant to exist twice, which it does from
	// the initial copy until the rollback window closes.
	CheckStorageHeadroom CheckName = "StorageHeadroom"
	// CheckTenantIsCold refuses to move a hot tenant.
	CheckTenantIsCold CheckName = "TenantIsCold"
	// CheckAdmissionInvariants asserts what tenant admission already guarantees. Large
	// objects and non-allowlisted extensions are refused at signup precisely so they cannot
	// appear here; finding one means admission has a hole, and reporting it as an ordinary
	// preflight refusal would hide that.
	CheckAdmissionInvariants CheckName = "AdmissionInvariants"
	// CheckFailoverSlotStack is the online path's precondition. Without the full PG18
	// stack a failover during the migration destroys the slot, and without
	// synchronized_standby_slots the subscriber can consume changes no standby has flushed
	// - after which the synced slot is behind the subscriber and rows are lost in silence.
	CheckFailoverSlotStack CheckName = "FailoverSlotStack"
	// CheckTenantRoleAttributes refuses to carry a role holding an attribute a tenant should
	// never have. A migration is not a route by which a tenant acquires SUPERUSER, and
	// CREATEROLE in particular would let it mint cluster-global roles of its own on the far
	// side - which is the one attribute that defeats every naming scheme above it.
	CheckTenantRoleAttributes CheckName = "TenantRoleAttributes"
	// CheckDatabaseConnectPrivilege refuses a source database that still lets PUBLIC connect.
	// Roles are cluster-global, so on an instance where tenant roles can authenticate, PUBLIC
	// CONNECT means every tenant's role can open a session on this tenant's database. Refusing
	// here means the migration is never asked to carry that posture forward.
	CheckDatabaseConnectPrivilege CheckName = "DatabaseConnectPrivilege"
)

// Check is one gate's verdict. Detail is written for a human reading a condition message at
// three in the morning, so it names the objects that failed rather than the rule.
type Check struct {
	Name   CheckName
	Passed bool
	Detail string
}

// PreflightResult is the whole gate's verdict.
type PreflightResult struct {
	Checks []Check
}

// Passed reports whether every check passed. An empty result has not run and does not pass.
func (r PreflightResult) Passed() bool {
	if len(r.Checks) == 0 {
		return false
	}
	for _, check := range r.Checks {
		if !check.Passed {
			return false
		}
	}
	return true
}

// Failures lists the checks that refused.
func (r PreflightResult) Failures() []Check {
	failures := make([]Check, 0, len(r.Checks))
	for _, check := range r.Checks {
		if !check.Passed {
			failures = append(failures, check)
		}
	}
	return failures
}

// Message is the condition message. It enumerates every failing check with its detail,
// because a gate that refuses without saying which objects are wrong forces an operator to
// re-derive the answer by hand.
func (r PreflightResult) Message() string {
	failures := r.Failures()
	if len(failures) == 0 {
		return fmt.Sprintf("all %d preflight checks passed", len(r.Checks))
	}
	parts := make([]string, 0, len(failures))
	for _, failure := range failures {
		parts = append(parts, string(failure.Name)+": "+failure.Detail)
	}
	return fmt.Sprintf("%d of %d preflight checks refused this migration; %s",
		len(failures), len(r.Checks), strings.Join(parts, "; "))
}

// PreflightInput is everything the gate needs that does not come from the source database.
type PreflightInput struct {
	Source Endpoint
	Target Endpoint
	// SourceStandbys are the source instance's non-primary members, each of which carries
	// half of the failover-slot contract.
	SourceStandbys []string
	// Online selects whether the failover-slot stack is required. Offline never opens a
	// slot, so the stack is irrelevant to it.
	Online bool

	RequireReplicaIdentity     bool
	ForbidPreparedTransactions bool
	RequireColdTenant          bool
	// MaxSourceUtilizationPercent is forbidMoveWhenSourceUtilizationAbovePercent.
	MaxSourceUtilizationPercent int32
	// TenantIsCold is the tenant controller's verdict. A nil value is not a pass: an
	// unobserved tenant is one whose coldness nobody has established.
	TenantIsCold *bool
	// TargetFreeBytes is the headroom on the target's data volume. Zero disables the check
	// only when TenantBytes is also unknown.
	TargetFreeBytes int64
	// ExpectedCollation is the pair of contracts the operator recorded for the two
	// instances. It is compared as well as the live catalog values, because a contract the
	// operator believes and a contract PostgreSQL has are two different claims.
	ExpectedCollation ContractPair
	// AllowedExtensions is the pool's curated allowlist, against which the source's
	// installed extensions are asserted.
	AllowedExtensions []string
}

// ContractPair is the operator's record of the two instances' collation contracts.
type ContractPair struct {
	SourceRecorded string
	TargetRecorded string
}

// RunPreflight evaluates every gate and returns them all, passed or not. It deliberately
// does not stop at the first failure: an operator fixing one refusal only to meet the next
// one on the following reconcile learns the truth one round trip at a time.
func RunPreflight(ctx context.Context, sql SQL, in PreflightInput) PreflightResult {
	checks := []Check{
		checkAdmissionInvariants(ctx, sql, in.Source, in.AllowedExtensions),
		checkCollation(ctx, sql, in),
		checkStorageHeadroom(ctx, sql, in),
		checkSourceUtilization(ctx, sql, in),
		checkTenantIsCold(in),
		checkTenantRoleAttributes(ctx, sql, in.Source),
		checkDatabaseConnectPrivilege(ctx, sql, in.Source),
	}
	if in.RequireReplicaIdentity {
		checks = append(checks, checkReplicaIdentity(ctx, sql, in.Source))
	}
	if in.ForbidPreparedTransactions {
		checks = append(checks, checkPreparedTransactions(ctx, sql, in.Source))
	}
	if in.Online {
		checks = append(checks, CheckFailoverSlots(ctx, sql, in.Source, in.SourceStandbys))
	}
	return PreflightResult{Checks: checks}
}

const replicaIdentityQuery = `SELECT n.nspname || '.' || c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND ` + UserSchemaPredicate + `
  AND c.relreplident = 'd'
  AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary AND i.indisvalid)
ORDER BY 1`

func checkReplicaIdentity(ctx context.Context, sql SQL, at Endpoint) Check {
	offenders, err := firstColumn(ctx, sql, at, replicaIdentityQuery)
	if err != nil {
		return failed(CheckReplicaIdentity, "could not read the source's replica identities: "+err.Error())
	}
	if len(offenders) > 0 {
		return failed(CheckReplicaIdentity, fmt.Sprintf(
			"%d relation(s) have neither a PRIMARY KEY nor an explicit REPLICA IDENTITY, so logical "+
				"replication would silently drop their UPDATEs and DELETEs on the target: %s. "+
				"Add a primary key, or ALTER TABLE ... REPLICA IDENTITY FULL, and retry",
			len(offenders), strings.Join(offenders, ", ")))
	}
	return passed(CheckReplicaIdentity, "every replicated relation carries a replica identity")
}

func checkPreparedTransactions(ctx context.Context, sql SQL, at Endpoint) Check {
	gids, err := firstColumn(ctx, sql, at,
		`SELECT gid FROM pg_prepared_xacts WHERE database = current_database() ORDER BY 1`)
	if err != nil {
		return failed(CheckPreparedTransactions, "could not read pg_prepared_xacts: "+err.Error())
	}
	if len(gids) > 0 {
		return failed(CheckPreparedTransactions, fmt.Sprintf(
			"%d prepared transaction(s) are open and pin the source's oldest xmin; they can be neither "+
				"replicated nor drained: %s. COMMIT PREPARED or ROLLBACK PREPARED them and retry",
			len(gids), strings.Join(gids, ", ")))
	}
	return passed(CheckPreparedTransactions, "no prepared transactions are open on the source")
}

// AlwaysInstalledExtensions are present in every database initdb creates and are therefore
// not evidence of anything a tenant asked for.
var AlwaysInstalledExtensions = []string{"plpgsql"}

// checkAdmissionInvariants asserts the two properties tenant admission guarantees.
//
// It is an assertion rather than a check: large objects have no representation in logical
// replication and an extension outside the pool's curated allowlist can differ in version
// between source and target, so both are refused at signup precisely so that Online stays
// the normal path. A hit here means admission has a hole. Reporting it as an ordinary
// preflight refusal would let that hole go on being invisible while migrations quietly
// degraded to the offline path one tenant at a time.
func checkAdmissionInvariants(ctx context.Context, sql SQL, at Endpoint, allowed []string) Check {
	largeObjects, err := scalarInt64(ctx, sql, at, `SELECT count(*)::text FROM pg_largeobject_metadata`)
	if err != nil {
		return failed(CheckAdmissionInvariants, "could not assert the admission invariants: "+err.Error())
	}

	permitted := make(map[string]bool, len(allowed)+len(AlwaysInstalledExtensions))
	for _, name := range slices.Concat(AlwaysInstalledExtensions, allowed) {
		permitted[name] = true
	}
	installed, err := firstColumn(ctx, sql, at, `SELECT extname FROM pg_extension ORDER BY 1`)
	if err != nil {
		return failed(CheckAdmissionInvariants, "could not read the source's extensions: "+err.Error())
	}
	var unlisted []string
	for _, name := range installed {
		if !permitted[name] {
			unlisted = append(unlisted, name)
		}
	}

	var breaches []string
	if largeObjects > 0 {
		breaches = append(breaches, fmt.Sprintf("%d large object(s)", largeObjects))
	}
	if len(unlisted) > 0 {
		breaches = append(breaches, "extension(s) outside the pool's allowlist: "+strings.Join(unlisted, ", "))
	}
	if len(breaches) > 0 {
		return failed(CheckAdmissionInvariants, fmt.Sprintf(
			"the source holds %s, which tenant admission refuses outright. Finding them here means "+
				"admission let them in, not that this migration should degrade to the offline path",
			strings.Join(breaches, " and ")))
	}
	return passed(CheckAdmissionInvariants,
		"the admission invariants tenant signup enforces still hold on the source")
}

// collationQuery reads the identity that decides whether two databases can exchange data
// at all. Every column is part of the tuple; comparing a subset is how a difference gets
// through.
//
// Every column is cast to text rather than concatenated as it comes: datlocprovider is the
// "char" type, and "text || char" is ambiguous enough that PostgreSQL refuses to choose an
// operator at all.
const collationQuery = `SELECT pg_encoding_to_char(d.encoding)::text || '|' || d.datcollate::text
  || '|' || d.datctype::text || '|' || d.datlocprovider::text || '|' || coalesce(d.datlocale, '')::text
  || '|' || coalesce(d.daticurules, '')::text
FROM pg_database d WHERE d.datname = current_database()`

func checkCollation(ctx context.Context, sql SQL, in PreflightInput) Check {
	source, err := scalar(ctx, sql, in.Source, collationQuery)
	if err != nil {
		return failed(CheckCollationContract, "could not read the source's collation tuple: "+err.Error())
	}
	target, err := scalar(ctx, sql, in.Target.WithDatabase("postgres"), collationQuery)
	if err != nil {
		return failed(CheckCollationContract, "could not read the target's collation tuple: "+err.Error())
	}
	if source != target {
		return failed(CheckCollationContract, fmt.Sprintf(
			"the source and target disagree on encoding|collate|ctype|provider|locale|icurules: %q versus %q. "+
				"Moving across that difference produces indexes silently inconsistent with their heap ordering",
			source, target))
	}
	if recorded := in.ExpectedCollation; recorded.SourceRecorded != recorded.TargetRecorded {
		return failed(CheckCollationContract, fmt.Sprintf(
			"the live tuples agree but the operator's recorded contracts do not: %q versus %q",
			recorded.SourceRecorded, recorded.TargetRecorded))
	}
	return passed(CheckCollationContract, "the collation tuple is byte-identical on both instances: "+source)
}

func checkStorageHeadroom(ctx context.Context, sql SQL, in PreflightInput) Check {
	size, err := scalarInt64(ctx, sql, in.Source, `SELECT pg_database_size(current_database())::text`)
	if err != nil {
		return failed(CheckStorageHeadroom, "could not read the tenant's size: "+err.Error())
	}
	if in.TargetFreeBytes <= 0 {
		return failed(CheckStorageHeadroom, fmt.Sprintf(
			"the target's free space is unknown, so headroom for the tenant's %d bytes cannot be established",
			size))
	}
	if in.TargetFreeBytes < size {
		return failed(CheckStorageHeadroom, fmt.Sprintf(
			"the tenant is %d bytes and the target has %d bytes free; the tenant has to exist on both "+
				"instances from the initial copy until the rollback window closes",
			size, in.TargetFreeBytes))
	}
	return passed(CheckStorageHeadroom, fmt.Sprintf(
		"the target has %d bytes free for a %d byte tenant", in.TargetFreeBytes, size))
}

const utilizationQuery = `SELECT (100 * (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')
  / greatest(current_setting('max_connections')::numeric, 1))::bigint::text`

func checkSourceUtilization(ctx context.Context, sql SQL, in PreflightInput) Check {
	if in.MaxSourceUtilizationPercent <= 0 {
		return passed(CheckSourceUtilization, "no source utilization ceiling is configured")
	}
	percent, err := scalarInt64(ctx, sql, in.Source, utilizationQuery)
	if err != nil {
		return failed(CheckSourceUtilization, "could not read the source's connection utilization: "+err.Error())
	}
	if percent > int64(in.MaxSourceUtilizationPercent) {
		return failed(CheckSourceUtilization, fmt.Sprintf(
			"the source is at %d%% of its connection budget, above the %d%% ceiling; logical decoding "+
				"would consume exactly the capacity this move exists to relieve",
			percent, in.MaxSourceUtilizationPercent))
	}
	return passed(CheckSourceUtilization, fmt.Sprintf(
		"the source is at %d%% of its connection budget, under the %d%% ceiling",
		percent, in.MaxSourceUtilizationPercent))
}

func checkTenantIsCold(in PreflightInput) Check {
	if !in.RequireColdTenant {
		return passed(CheckTenantIsCold, "the coldness requirement is disabled for this migration")
	}
	if in.TenantIsCold == nil {
		return failed(CheckTenantIsCold,
			"the tenant's utilization has not been observed, so its coldness is unestablished rather than true")
	}
	if !*in.TenantIsCold {
		return failed(CheckTenantIsCold,
			"the tenant is above the pool's hotTenantUtilizationThresholdPercent; moving it would consume "+
				"exactly the resource that is already scarce")
	}
	return passed(CheckTenantIsCold, "the tenant has stayed cold for the whole observation window")
}

// privilegedRoleQuery finds carried roles holding an attribute no tenant role should have.
// Reads pg_roles, never pg_authid: nothing here needs a credential, and asking for one would
// make a preflight check a place where credentials are read.
const privilegedRoleQuery = `SELECT r.rolname || ':' || concat_ws(',',
    CASE WHEN r.rolsuper THEN 'SUPERUSER' END,
    CASE WHEN r.rolcreatedb THEN 'CREATEDB' END,
    CASE WHEN r.rolcreaterole THEN 'CREATEROLE' END,
    CASE WHEN r.rolreplication THEN 'REPLICATION' END,
    CASE WHEN r.rolbypassrls THEN 'BYPASSRLS' END)
  FROM pg_roles r
 WHERE r.rolname = ANY (%s)
   AND (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
 ORDER BY 1`

func checkTenantRoleAttributes(ctx context.Context, sql SQL, source Endpoint) Check {
	roles, err := EnumerateTenantRoles(ctx, sql, source)
	if err != nil {
		return failed(CheckTenantRoleAttributes, err.Error())
	}
	if len(roles) == 0 {
		return passed(CheckTenantRoleAttributes, "the tenant's database depends on no roles of its own")
	}
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	offenders, err := firstColumn(ctx, sql, source, fmt.Sprintf(privilegedRoleQuery, textArray(names)))
	if err != nil {
		return failed(CheckTenantRoleAttributes, err.Error())
	}
	if len(offenders) > 0 {
		return failed(CheckTenantRoleAttributes, fmt.Sprintf(
			"these roles hold attributes a tenant role must not carry onto another instance: %s",
			strings.Join(offenders, ", ")))
	}
	return passed(CheckTenantRoleAttributes, fmt.Sprintf(
		"all %d carried role(s) are unprivileged", len(roles)))
}

const publicConnectQuery = `SELECT count(*)::text FROM pg_database d,
  aclexplode(coalesce(d.datacl, acldefault('d'::"char", d.datdba))) e
 WHERE d.datname = current_database() AND e.grantee = 0 AND e.privilege_type = 'CONNECT'`

func checkDatabaseConnectPrivilege(ctx context.Context, sql SQL, source Endpoint) Check {
	granted, err := scalarInt64(ctx, sql, source, publicConnectQuery)
	if err != nil {
		return failed(CheckDatabaseConnectPrivilege, err.Error())
	}
	if granted > 0 {
		return failed(CheckDatabaseConnectPrivilege,
			"PUBLIC holds CONNECT on this database, so every role on the target instance could "+
				"open a session on it; REVOKE CONNECT ON DATABASE ... FROM PUBLIC first")
	}
	return passed(CheckDatabaseConnectPrivilege, "PUBLIC cannot connect to this database")
}

func passed(name CheckName, detail string) Check {
	return Check{Name: name, Passed: true, Detail: detail}
}

func failed(name CheckName, detail string) Check {
	return Check{Name: name, Passed: false, Detail: detail}
}
