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

// Package tenantdbtest is an in-memory stand-in for the PostgreSQL cluster a tenant is
// provisioned on.
//
// It is a catalog rather than a script of canned answers, because the properties worth
// testing are catalog properties: that a second pass issues no DDL, that a role must exist
// before the database it owns, that a duplicate CREATE fails the way PostgreSQL fails it.
// A fake that answers by matching statement fragments can be made to agree with any of
// those without any of them being true.
//
// It lives in its own package so the tenant lifecycle's specs and this package's own specs
// assert against the same PostgreSQL, and so nothing it defines can reach a binary.
package tenantdbtest

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// database is one row of the fake pg_database.
type database struct {
	oid        int64
	owner      string
	allowsConn bool
}

// Cluster answers the SQL port with the catalog it holds. The zero value is not usable;
// build one with NewCluster.
type Cluster struct {
	mutex      sync.Mutex
	roles      map[string]int32
	roleConfig map[string]map[string]string
	databases  map[string]*database
	failures   map[string]error
	nextOID    int64

	// concealed hides objects from the next catalog read only. It is how a spec produces
	// the one race that cannot be closed in SQL: a read that says absent, followed by a
	// CREATE that says the object is already there.
	concealed map[string]bool

	// statements is every statement in the order it arrived, which is what lets a spec
	// assert that a reconcile issued no DDL at all.
	statements []string
}

var _ migration.SQL = (*Cluster)(nil)

// firstOID is where the fake's oid counter starts. It is past PostgreSQL's own reserved
// range so a spec asserting on an oid cannot pass by matching a hard-coded small number.
const firstOID int64 = 16384

// bootstrapSuperuser is the role initdb leaves behind, and the database named after it is
// where every CREATE DATABASE is issued from.
const bootstrapSuperuser = "postgres"

// NewCluster returns a cluster holding only what a real one is initdb'd with.
func NewCluster() *Cluster {
	return &Cluster{
		roles: map[string]int32{bootstrapSuperuser: -1},
		databases: map[string]*database{
			bootstrapSuperuser: {oid: 5, owner: bootstrapSuperuser, allowsConn: true},
		},
		failures:  map[string]error{},
		concealed: map[string]bool{},
		nextOID:   firstOID,
	}
}

// ConcealOnce hides the named role and database from the next catalog read, leaving the
// catalog itself untouched.
func (c *Cluster) ConcealOnce(names ...string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, name := range names {
		c.concealed[name] = true
	}
}

// FailOn makes every statement containing fragment fail, which is how a spec reaches the
// failure branch without inventing a transport error.
func (c *Cluster) FailOn(fragment string, err error) *Cluster {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.failures[fragment] = err
	return c
}

// Heal removes a previously configured failure.
func (c *Cluster) Heal(fragment string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.failures, fragment)
}

// Fence makes an existing database refuse connections, the state a migration leaves its
// source in.
func (c *Cluster) Fence(name string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if found, ok := c.databases[name]; ok {
		found.allowsConn = false
	}
}

// HasDatabase reports a row in the fake pg_database.
func (c *Cluster) HasDatabase(name string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.databases[name]
	return ok
}

// HasRole reports a row in the fake pg_roles.
func (c *Cluster) HasRole(name string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.roles[name]
	return ok
}

// RoleSetting is what ALTER ROLE ... SET left in the role's rolconfig, or the empty string.
func (c *Cluster) RoleSetting(role, name string) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.roleConfig[role][strings.ToLower(name)]
}

// renderRoleConfig spells rolconfig the way array_to_string does, in a stable order.
func (c *Cluster) renderRoleConfig(role string) string {
	settings := c.roleConfig[role]
	if len(settings) == 0 {
		return ""
	}
	names := make([]string, 0, len(settings))
	for name := range settings {
		names = append(names, name)
	}
	slices.Sort(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		lines = append(lines, name+"="+settings[name])
	}
	// The separator the real projection uses, and deliberately not a newline: the transport
	// splits rows on a newline, so a fake that joined with one would agree with an
	// implementation that could never read its own answer back.
	return strings.Join(lines, "\x1e")
}

// ConnectionLimit is the role's rolconnlimit, or zero if there is no such role.
func (c *Cluster) ConnectionLimit(name string) int32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.roles[name]
}

// OwnerOf is the role owning a database, or the empty string.
func (c *Cluster) OwnerOf(name string) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if found, ok := c.databases[name]; ok {
		return found.owner
	}
	return ""
}

// Statements is every statement the cluster has been asked, in order.
func (c *Cluster) Statements() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return append([]string(nil), c.statements...)
}

// Forget drops the recorded statements, so a spec can assert on one reconcile rather than
// on every reconcile that came before it.
func (c *Cluster) Forget() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.statements = nil
}

// Ran counts the recorded statements containing fragment.
func (c *Cluster) Ran(fragment string) int {
	count := 0
	for _, statement := range c.Statements() {
		if strings.Contains(statement, fragment) {
			count++
		}
	}
	return count
}

// Exec applies a statement to the catalog.
func (c *Cluster) Exec(_ context.Context, _ migration.Endpoint, statement string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.statements = append(c.statements, statement)
	if err := c.matchFailure(statement); err != nil {
		return err
	}
	return c.apply(statement)
}

// Query answers a statement from the catalog.
func (c *Cluster) Query(_ context.Context, at migration.Endpoint, statement string) ([]migration.Row, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.statements = append(c.statements, statement)
	if err := c.matchFailure(statement); err != nil {
		return nil, err
	}
	switch {
	case strings.Contains(statement, "current_database()"):
		return c.currentDatabase(at)
	case strings.Contains(statement, "pg_roles"):
		return c.observe(statement)
	default:
		return nil, fmt.Errorf("no fake answer for %q", statement)
	}
}

func (c *Cluster) apply(statement string) error {
	names := identifiers(statement)
	switch {
	case strings.HasPrefix(statement, "CREATE ROLE"):
		return c.createRole(names)
	case strings.HasPrefix(statement, "CREATE DATABASE"):
		return c.createDatabase(names)
	case strings.HasPrefix(statement, "ALTER ROLE"):
		return c.alterRole(statement, names)
	case strings.HasPrefix(statement, "DROP DATABASE"):
		return c.dropDatabase(names)
	case strings.HasPrefix(statement, "DROP ROLE"):
		return c.dropRole(names)
	case strings.HasPrefix(statement, "REVOKE "), strings.HasPrefix(statement, "GRANT "):
		// Recorded rather than modelled. What these say is asserted by reading the statements
		// back, and modelling an ACL well enough to answer questions about it would be
		// modelling the part of PostgreSQL the e2e exists to ask.
		return nil
	default:
		return fmt.Errorf(`syntax error at or near "%s"`, statement)
	}
}

func (c *Cluster) createRole(names []string) error {
	if len(names) != 1 {
		return fmt.Errorf("CREATE ROLE names %d identifiers", len(names))
	}
	if _, ok := c.roles[names[0]]; ok {
		return fmt.Errorf(`role "%s" already exists`, names[0])
	}
	c.roles[names[0]] = -1
	return nil
}

func (c *Cluster) createDatabase(names []string) error {
	if len(names) != 2 {
		return fmt.Errorf("CREATE DATABASE names %d identifiers", len(names))
	}
	name, owner := names[0], names[1]
	if _, ok := c.databases[name]; ok {
		return fmt.Errorf(`database "%s" already exists`, name)
	}
	if _, ok := c.roles[owner]; !ok {
		return fmt.Errorf(`role "%s" does not exist`, owner)
	}
	c.databases[name] = &database{oid: c.nextOID, owner: owner, allowsConn: true}
	c.nextOID++
	return nil
}

var connectionLimitPattern = regexp.MustCompile(`CONNECTION LIMIT (-?\d+)`)

// roleSettingPattern matches the ALTER ROLE ... SET the tier-2 limits are applied with. The
// value is a quoted literal, and PostgreSQL doubles an embedded quote, so the capture stops at
// the first unescaped one.
var roleSettingPattern = regexp.MustCompile(`SET ([a-zA-Z_][a-zA-Z0-9_]*) = '([^']*)'`)

func (c *Cluster) alterRole(statement string, names []string) error {
	if len(names) != 1 {
		return fmt.Errorf(`syntax error at or near "%s"`, statement)
	}
	if _, ok := c.roles[names[0]]; !ok {
		return fmt.Errorf(`role "%s" does not exist`, names[0])
	}
	// A credential is accepted and not stored. What the fake could say about a verifier is
	// only what it was handed; whether PostgreSQL accepts the form, and whether the proxy can
	// then authenticate against it, is a question for a real postmaster.
	if strings.Contains(statement, " PASSWORD ") {
		return nil
	}
	// ALTER ROLE ... SET <guc> = '<value>' lands in rolconfig, which is a different column
	// from rolconnlimit and is read back as a "name=value" list.
	if set := roleSettingPattern.FindStringSubmatch(statement); set != nil {
		if c.roleConfig == nil {
			c.roleConfig = map[string]map[string]string{}
		}
		if c.roleConfig[names[0]] == nil {
			c.roleConfig[names[0]] = map[string]string{}
		}
		c.roleConfig[names[0]][strings.ToLower(set[1])] = set[2]
		return nil
	}
	match := connectionLimitPattern.FindStringSubmatch(statement)
	if match == nil {
		return fmt.Errorf(`syntax error at or near "%s"`, statement)
	}
	limit, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil {
		return err
	}
	c.roles[names[0]] = int32(limit)
	return nil
}

func (c *Cluster) dropDatabase(names []string) error {
	if len(names) != 1 {
		return fmt.Errorf("DROP DATABASE names %d identifiers", len(names))
	}
	delete(c.databases, names[0])
	return nil
}

func (c *Cluster) dropRole(names []string) error {
	if len(names) != 1 {
		return fmt.Errorf("DROP ROLE names %d identifiers", len(names))
	}
	for name, held := range c.databases {
		if held.owner == names[0] {
			return fmt.Errorf(`role "%s" cannot be dropped because it owns database "%s"`, names[0], name)
		}
	}
	delete(c.roles, names[0])
	return nil
}

func (c *Cluster) currentDatabase(at migration.Endpoint) ([]migration.Row, error) {
	found, ok := c.databases[at.Database]
	if !ok {
		return nil, fmt.Errorf(`database "%s" does not exist`, at.Database)
	}
	if !found.allowsConn {
		return nil, fmt.Errorf(`database "%s" is not currently accepting connections`, at.Database)
	}
	return []migration.Row{{at.Database}}, nil
}

var (
	rolePattern     = regexp.MustCompile(`rolname = '([^']*)'`)
	databasePattern = regexp.MustCompile(`datname = '([^']*)'`)
)

func (c *Cluster) observe(statement string) ([]migration.Row, error) {
	role := rolePattern.FindStringSubmatch(statement)
	name := databasePattern.FindStringSubmatch(statement)
	if role == nil || name == nil {
		return nil, fmt.Errorf("no fake answer for %q", statement)
	}

	roles, limit, config := "0", "-1", ""
	if held, ok := c.roles[role[1]]; ok && !c.concealed[role[1]] {
		roles, limit = "1", strconv.FormatInt(int64(held), 10)
		config = c.renderRoleConfig(role[1])
	}
	// datallowconn comes back as the "1"/"0" the query casts it to, not as psql's displayed
	// "t"/"f": a fake that answered in the displayed spelling would agree with a client that
	// reads every database as refusing connections.
	oid, allowsConn := "0", "0"
	if found, ok := c.databases[name[1]]; ok && !c.concealed[name[1]] {
		oid = strconv.FormatInt(found.oid, 10)
		if found.allowsConn {
			allowsConn = "1"
		}
	}
	clear(c.concealed)
	return []migration.Row{{roles, oid, allowsConn, limit, config}}, nil
}

func (c *Cluster) matchFailure(statement string) error {
	for fragment, err := range c.failures {
		if strings.Contains(statement, fragment) {
			return err
		}
	}
	return nil
}

// identifiers pulls the double-quoted names out of a statement in the order they appear,
// undoubling the escaping QuoteIdentifier applied.
func identifiers(statement string) []string {
	var names []string
	rest := statement
	for {
		start := strings.Index(rest, `"`)
		if start < 0 {
			return names
		}
		rest = rest[start+1:]
		var name strings.Builder
		for {
			end := strings.Index(rest, `"`)
			if end < 0 {
				return append(names, name.String())
			}
			name.WriteString(rest[:end])
			rest = rest[end+1:]
			if !strings.HasPrefix(rest, `"`) {
				break
			}
			name.WriteString(`"`)
			rest = rest[1:]
		}
		names = append(names, name.String())
	}
}
