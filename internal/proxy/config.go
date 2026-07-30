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

package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Instance is one member of the pool as the proxy needs to see it: an address, a capacity
// boundary, and the credentials that member's own bootstrap issued.
//
// The credentials are per instance and not per pool. Each PgInstance generates its own role
// passwords, so one shared backend password would authenticate against exactly one member
// of a multi-instance pool and fail on every other.
type Instance struct {
	Name               string
	Address            string
	BackendConnections int32
	User               string
	Password           string
}

// Tenant is one tenant's routing entry and capacity claim.
//
// Name is the value a client's connection resolves to, which for a pool behind one Service
// is the database name. It is not the PgTenant object's name: the object name is a
// Kubernetes identifier nothing on the PostgreSQL wire ever sends.
type Tenant struct {
	Name                 string
	Instance             string
	Guaranteed           int32
	Burstable            int32
	Weight               int32
	Priority             int32
	MaxClientConnections int32
	// BackendRole is the PostgreSQL role this tenant's backend sessions run as, and
	// BackendSaltedPassword the client half of the SCRAM credential that proves it. The proxy
	// is the SCRAM client on that leg, so a server-side verifier would be useless to it.
	//
	// Empty until the tenant controller has published them. The fleet refuses such a tenant
	// rather than falling back to the instance identity: a fallback would put tenant SQL back
	// on pgelastic_ops at exactly the moment nobody is watching, which is a config-propagation
	// lag rather than a state anybody would notice.
	BackendRole           string
	BackendSaltedPassword string
	BackendSalt           string
	BackendIterations     int32
	// CredentialGeneration makes a rotation evict the links opened under the old credential.
	// It is part of the pool key, so a bump means the old key is unreachable from any new
	// binding and its links drain out rather than being handed to somebody.
	CredentialGeneration int32
}

// User is one SCRAM identity the proxy authenticates a client against.
type User struct {
	Name string
	// Tenant is the tenant this login belongs to, and the only one it may reach. The proxy
	// authenticates a client against Name and resolves its tenant from a different part of the
	// same startup packet, so without this the two are unrelated and a client holding one
	// tenant's password reaches any tenant it can name.
	Tenant string
	// Verifier is a PostgreSQL rolpassword SCRAM secret. Preferred over Password: the proxy
	// then never holds anything it could replay against the backend.
	Verifier string
	Password string
}

// Config is everything the rendered document is derived from.
type Config struct {
	Pool      *pgelasticv1alpha1.PgElasticPool
	Instances []Instance
	Tenants   []Tenant
	Users     []User
	// ClientTLS and BackendCA are set when the corresponding material is mounted, which is
	// what decides whether the TLS sections are emitted at all.
	ClientTLS bool
	BackendCA bool
	// Control is set when the control listener's certificates have been reconciled. It is
	// one flag rather than two because the listener is never rendered without them: the
	// proxy refuses a control address with no client CA, which is what keeps a rendering
	// mistake from exposing every tenant's gate.
	Control bool
}

// Document is a rendered configuration and the two identities derived from it.
type Document struct {
	// TOML is the whole document, which is what the Secret carries.
	TOML string
	// Version identifies this document. Replicas report it back once applied, so a fleet
	// converged on a configuration is distinguishable from one still adopting it.
	Version string
	// StructuralHash covers only the half a running replica cannot adopt. It goes into the
	// pod template, so a change to the routing table or to a tenant's claim rewrites the
	// Secret without rolling a single pod.
	StructuralHash string
}

// Render produces the document, deterministically.
//
// Determinism is not tidiness. The Secret is rewritten on every reconcile, and an unsorted
// map iteration would produce a different document every pass — which changes the hash,
// which changes the pod template, which rolls the whole fleet and drops every client on it,
// forever, for no reason. Every collection below is sorted before it is written.
func (c Config) Render() Document {
	full := c.render(true)
	structural := c.render(false)
	version := fmt.Sprintf("%d-%s", c.Pool.Generation, shortHash(full))
	return Document{
		TOML:           "configVersion = " + tomlString(version) + "\n\n" + full,
		Version:        version,
		StructuralHash: shortHash(structural),
	}
}

// render writes the document, with or without the half a running replica can adopt.
func (c Config) render(dynamic bool) string {
	var out strings.Builder
	c.renderListen(&out)
	c.renderBackend(&out)
	c.renderAuth(&out, dynamic)
	c.renderDrainAndMetrics(&out)
	c.renderControl(&out)
	c.renderRouting(&out, dynamic)
	c.renderPool(&out, dynamic)
	c.renderReload(&out)
	c.renderInstances(&out, dynamic)
	return out.String()
}

func (c Config) renderListen(out *strings.Builder) {
	out.WriteString("[listen]\n")
	writeString(out, "address", fmt.Sprintf("0.0.0.0:%d", DefaultClientPort))
	writeInt(out, "maxClientConnections", int64(maxClientConnections(c.Pool)))
	writeInt(out, "clientLoginSeconds", seconds(timeouts(c.Pool).ClientLogin, 10))
	if c.ClientTLS {
		writeBool(out, "requireTls", requireTLS(c.Pool))
		out.WriteString("\n[listen.tls]\n")
		writeString(out, "certificateFile", TLSDir+"/tls.crt")
		writeString(out, "keyFile", TLSDir+"/tls.key")
	}
	out.WriteString("\n")
}

// renderBackend writes the pool-wide backend leg. Every instance overrides its address and
// its credentials, so what is left here is the settings that genuinely are pool-wide — and
// the address of the first instance, which the schema requires and which the fleet only
// uses when no instances are declared at all.
func (c Config) renderBackend(out *strings.Builder) {
	out.WriteString("[backend]\n")
	address := "127.0.0.1:5432"
	user := "postgres"
	if len(c.Instances) > 0 {
		first := c.sortedInstances()[0]
		address, user = first.Address, first.User
	}
	writeString(out, "address", address)
	writeString(out, "user", user)
	writeInt(out, "connectSeconds", seconds(timeouts(c.Pool).Connect, 5))
	out.WriteString("\n[backend.tls]\n")
	writeString(out, "mode", backendTLSMode(c.Pool, c.BackendCA))
	if c.BackendCA {
		writeString(out, "caFile", BackendCADir+"/ca.crt")
	}
	out.WriteString("\n")
}

// renderAuth writes the pool's SCRAM posture, and the logins themselves only into the dynamic
// half.
//
// scramIterations stays structural because it is part of every verifier rather than a tunable:
// changing it invalidates every credential derived under the old one, which is a different
// process rather than a document a running one can adopt. The logins are the opposite. A pool
// exists to have tenants added to it, and leaving them structural meant onboarding one rolled
// every replica and dropped every other tenant's clients - the exact outcome renderControl's
// own comment says [routing].tenants is excluded to avoid.
func (c Config) renderAuth(out *strings.Builder, dynamic bool) {
	out.WriteString("[auth]\n")
	writeInt(out, "scramIterations", int64(scramIterations(c.Pool)))
	out.WriteString("\n")

	if !dynamic {
		return
	}
	users := slices.Clone(c.Users)
	slices.SortFunc(users, func(a, b User) int { return strings.Compare(a.Name, b.Name) })
	for _, user := range users {
		out.WriteString("[[auth.users]]\n")
		writeString(out, "name", user.Name)
		writeString(out, "tenant", user.Tenant)
		if user.Verifier != "" {
			writeString(out, "verifier", user.Verifier)
		} else {
			writeString(out, "password", user.Password)
		}
		out.WriteString("\n")
	}
}

func (c Config) renderDrainAndMetrics(out *strings.Builder) {
	out.WriteString("[drain]\n")
	writeInt(out, "shutdownSeconds", seconds(timeouts(c.Pool).Shutdown, 60))
	out.WriteString("\n[metrics]\n")
	writeString(out, "address", fmt.Sprintf("0.0.0.0:%d", metricsPort(c.Pool)))
	out.WriteString("\n")
}

// renderControl writes the lease-bound cutover API's listener.
//
// Written into both halves of the document and therefore covered by the structural hash: a
// listen address and a trust root are things a running process cannot adopt, so changing
// either has to roll the fleet. That is the opposite of [routing].tenants, which is
// deliberately excluded so that adding a tenant restarts nothing.
//
// It is bound on all interfaces rather than on localhost, because the caller is the
// operator reaching the replica across the network. What makes that safe is the mutual TLS
// below and not the bind address: the proxy refuses to serve this port at all without a
// client CA and a name to check against it.
func (c Config) renderControl(out *strings.Builder) {
	if !c.Control {
		return
	}
	out.WriteString("[control]\n")
	writeString(out, "address", fmt.Sprintf("0.0.0.0:%d", DefaultControlPort))
	writeInt(out, "defaultLeaseTtlMs", ControlLeaseTTLMillis)
	writeInt(out, "maxLeaseTtlMs", ControlMaxLeaseTTLMillis)
	out.WriteString("\n[control.tls]\n")
	writeString(out, "certificateFile", ControlTLSDir+"/tls.crt")
	writeString(out, "keyFile", ControlTLSDir+"/tls.key")
	writeString(out, "clientCaFile", ControlTLSDir+"/ca.crt")
	writeString(out, "clientName", ControlClientName(c.Pool.Name, c.Pool.Namespace))
	out.WriteString("\n")
}

// The lease a cutover holds a tenant under.
//
// The ceiling is what bounds how long a killed operator can hold a tenant's clients still,
// so it is deliberately far above the default and far below anything an operator would call
// an outage. The default is short because the holder is expected to renew: a controller that
// dies mid-cutover should release the tenant in seconds, not at the ceiling.
const (
	ControlLeaseTTLMillis    = 15_000
	ControlMaxLeaseTTLMillis = 120_000
)

func (c Config) renderRouting(out *strings.Builder, dynamic bool) {
	out.WriteString("[routing]\n")
	discriminators := tenantDiscriminators(c.Pool)
	quoted := make([]string, 0, len(discriminators))
	for _, discriminator := range discriminators {
		quoted = append(quoted, tomlString(string(discriminator)))
	}
	fmt.Fprintf(out, "tenantDiscriminators = [%s]\n", strings.Join(quoted, ", "))
	writeString(out, "startupOptionKey", startupOptionKey(c.Pool))
	if instances := c.sortedInstances(); len(instances) > 0 {
		writeString(out, "defaultInstance", instances[0].Name)
	}
	if dynamic {
		out.WriteString("tenants = { ")
		entries := make([]string, 0, len(c.Tenants))
		for _, tenant := range c.sortedTenants() {
			if tenant.Instance == "" {
				continue
			}
			entries = append(entries,
				tomlString(tenant.Name)+" = "+tomlString(tenant.Instance))
		}
		out.WriteString(strings.Join(entries, ", "))
		out.WriteString(" }\n")
	}
	out.WriteString("\n")
}

func (c Config) renderPool(out *strings.Builder, dynamic bool) {
	pooling := c.Pool.Spec.Pooling
	out.WriteString("[pool]\n")
	writeString(out, "mode", poolMode(pooling))
	writeInt(out, "backendConnections", int64(c.Pool.Spec.Capacity.BackendConnections))
	writeInt(out, "headroomPercent", int64(headroomPercent(c.Pool)))
	writeInt(out, "maxClientConnections", int64(maxClientConnections(c.Pool)))
	writeString(out, "resetPolicy", resetPolicy(pooling))
	writeInt(out, "queryWaitSeconds", seconds(timeouts(c.Pool).Checkout, 30))
	writeInt(out, "notifyAfterSeconds", int64(queryWaitNotifySeconds(c.Pool)))
	writeInt(out, "queueDepthPerTenant", int64(queueDepthPerTenant(c.Pool)))
	writeInt(out, "maxServerStatements", int64(preparedStatementsLimit(pooling)))
	writeInt(out, "serverLifetimeSeconds", serverLifetimeSeconds(pooling))
	out.WriteString("\n")

	if !dynamic {
		return
	}
	for _, tenant := range c.sortedTenants() {
		out.WriteString("[[pool.tenants]]\n")
		writeString(out, "name", tenant.Name)
		writeInt(out, "guaranteed", int64(tenant.Guaranteed))
		writeInt(out, "burstable", int64(tenant.Burstable))
		writeInt(out, "weight", int64(tenant.Weight))
		writeInt(out, "priority", int64(tenant.Priority))
		// Omitted rather than written as zero when nobody has set one: a zero here is a
		// tenant that may hold no client sockets at all, which is the opposite of "no
		// per-tenant cap was configured".
		if tenant.MaxClientConnections > 0 {
			writeInt(out, "maxClientConnections", int64(tenant.MaxClientConnections))
		}
		// The identity this tenant's backend sessions run as, and the credential that proves
		// it. Here rather than in [[instances]] because that half is structural: a credential
		// rotation there would roll the whole fleet, and the point of rotating is that nobody
		// notices. Omitted together when the tenant controller has not published them yet -
		// the fleet refuses such a tenant rather than falling back to the instance identity,
		// so a partial rollout costs one tenant one reconcile instead of running every tenant
		// as the control plane.
		if tenant.BackendRole != "" && tenant.BackendSaltedPassword != "" {
			writeString(out, "backendRole", tenant.BackendRole)
			writeString(out, "backendSaltedPassword", tenant.BackendSaltedPassword)
			writeString(out, "backendSalt", tenant.BackendSalt)
			writeInt(out, "backendIterations", int64(tenant.BackendIterations))
			writeInt(out, "credentialGeneration", int64(tenant.CredentialGeneration))
		}
		out.WriteString("\n")
	}
}

func (c Config) renderReload(out *strings.Builder) {
	out.WriteString("[reload]\n")
	writeInt(out, "intervalMs", ReloadIntervalMillis)
	writeBool(out, "reportToPod", true)
	out.WriteString("\n[reload.secret]\n")
	writeString(out, "namespace", c.Pool.Namespace)
	writeString(out, "name", ConfigSecretName(c.Pool.Name))
	writeString(out, "key", ConfigKey)
	out.WriteString("\n")
}

func (c Config) renderInstances(out *strings.Builder, dynamic bool) {
	for _, instance := range c.sortedInstances() {
		out.WriteString("[[instances]]\n")
		writeString(out, "name", instance.Name)
		writeString(out, "address", instance.Address)
		// An instance's allocatable capacity belongs in the dynamic half, and it is the roll
		// that proves why. It is derived from the member's published capacity, which the
		// operator withholds whenever the instance leaves Ready - so every rolling restart
		// moves this number, and while it was structural, rolling ONE instance changed the pod
		// template hash and restarted the entire proxy fleet. Every client of every tenant on
		// every other instance was dropped by a restart that had nothing to do with them.
		//
		// Which instance a fleet fronts, and how it dials it, are still structural: those are
		// things a running process cannot adopt.
		if dynamic && instance.BackendConnections > 0 {
			writeInt(out, "backendConnections", int64(instance.BackendConnections))
		}
		writeString(out, "user", instance.User)
		writeString(out, "password", instance.Password)
		out.WriteString("\n")
	}
}

func (c Config) sortedInstances() []Instance {
	instances := slices.Clone(c.Instances)
	slices.SortFunc(instances, func(a, b Instance) int { return strings.Compare(a.Name, b.Name) })
	return instances
}

func (c Config) sortedTenants() []Tenant {
	tenants := slices.Clone(c.Tenants)
	slices.SortFunc(tenants, func(a, b Tenant) int { return strings.Compare(a.Name, b.Name) })
	return tenants
}

// ReloadIntervalMillis bounds how long a published change takes to reach a replica. It is
// the whole propagation latency: the operator writes the Secret and each replica reads it
// on its next tick.
const ReloadIntervalMillis = 1000

func writeString(out *strings.Builder, key, value string) {
	out.WriteString(key)
	out.WriteString(" = ")
	out.WriteString(tomlString(value))
	out.WriteByte('\n')
}

func writeInt(out *strings.Builder, key string, value int64) {
	fmt.Fprintf(out, "%s = %d\n", key, value)
}

func writeBool(out *strings.Builder, key string, value bool) {
	fmt.Fprintf(out, "%s = %t\n", key, value)
}

// tomlString quotes a value as a TOML basic string.
//
// Hand-written rather than strconv.Quote: Go escapes a non-printable rune as \xNN, which
// TOML does not accept, so a password containing one would produce a document the fleet
// refuses to parse — and the failure would surface as an unexplained CrashLoopBackOff
// rather than as a rendering error.
func tomlString(value string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&out, `\u%04X`, r)
				continue
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func timeouts(pool *pgelasticv1alpha1.PgElasticPool) pgelasticv1alpha1.PoolTimeouts {
	if pool.Spec.Timeouts != nil {
		return *pool.Spec.Timeouts
	}
	return pgelasticv1alpha1.PoolTimeouts{}
}

// seconds rounds up, because every one of these bounds a wait: a 500ms timeout rendered as
// zero would mean "no timeout" to the proxy, which is the opposite of what was asked for.
func seconds(duration *metav1.Duration, fallback int64) int64 {
	if duration == nil || duration.Duration <= 0 {
		return fallback
	}
	return int64((duration.Duration + time.Second - 1) / time.Second)
}

func poolMode(pooling *pgelasticv1alpha1.PoolingConfig) string {
	if pooling != nil && pooling.Mode == pgelasticv1alpha1.PoolModeSession {
		return "session"
	}
	return "transaction"
}

func resetPolicy(pooling *pgelasticv1alpha1.PoolingConfig) string {
	if pooling == nil || pooling.ResetMode == "" {
		return "discardAll"
	}
	switch pooling.ResetMode {
	case pgelasticv1alpha1.ResetNone:
		return "none"
	case pgelasticv1alpha1.ResetDirtyTracked:
		return "dirtyTracked"
	case pgelasticv1alpha1.ResetSmartDiscard:
		return "smartDiscard"
	case pgelasticv1alpha1.ResetVerified:
		return "verified"
	default:
		return "discardAll"
	}
}

func preparedStatementsLimit(pooling *pgelasticv1alpha1.PoolingConfig) int32 {
	if pooling != nil && pooling.PreparedStatementsLimit != nil {
		return *pooling.PreparedStatementsLimit
	}
	return 1000
}

func serverLifetimeSeconds(pooling *pgelasticv1alpha1.PoolingConfig) int64 {
	if pooling != nil {
		return seconds(pooling.ServerLifetime, 3600)
	}
	return 3600
}

func maxClientConnections(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if pool.Spec.Capacity.MaxClientConnections != nil {
		return *pool.Spec.Capacity.MaxClientConnections
	}
	return 10_000
}

func headroomPercent(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if pool.Spec.Capacity.HeadroomPercent != nil {
		return *pool.Spec.Capacity.HeadroomPercent
	}
	return 0
}

func queueDepthPerTenant(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if admission := pool.Spec.Admission; admission != nil && admission.QueueDepthPerTenant != nil {
		return *admission.QueueDepthPerTenant
	}
	return 64
}

func queryWaitNotifySeconds(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if admission := pool.Spec.Admission; admission != nil &&
		admission.QueryWaitNotifySeconds != nil {
		return *admission.QueryWaitNotifySeconds
	}
	return 5
}

func scramIterations(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if auth := pool.Spec.Auth; auth != nil && auth.ScramIterations != nil {
		return *auth.ScramIterations
	}
	return 4096
}

func metricsPort(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if proxy := pool.Spec.Proxy; proxy != nil && proxy.Metrics != nil && proxy.Metrics.Port != nil {
		return *proxy.Metrics.Port
	}
	return DefaultMetricsPort
}

// tenantDiscriminators is what the pool asked for, unaltered.
//
// A list that resolves to nothing for a given client leaves that client attributed to its
// login role, which is the single-tenant shape. That is a configuration mistake and is
// reported as one rather than silently corrected here: appending DatabaseName to a list the
// operator did not ask for would route connections by a rule nobody wrote down.
func tenantDiscriminators(
	pool *pgelasticv1alpha1.PgElasticPool,
) []pgelasticv1alpha1.TenantDiscriminator {
	if proxy := pool.Spec.Proxy; proxy != nil && proxy.Routing != nil &&
		len(proxy.Routing.TenantDiscriminators) > 0 {
		return proxy.Routing.TenantDiscriminators
	}
	return []pgelasticv1alpha1.TenantDiscriminator{
		pgelasticv1alpha1.DiscriminatorSNI,
		pgelasticv1alpha1.DiscriminatorStartupOptions,
		pgelasticv1alpha1.DiscriminatorDatabaseName,
	}
}

func startupOptionKey(pool *pgelasticv1alpha1.PgElasticPool) string {
	if proxy := pool.Spec.Proxy; proxy != nil && proxy.Routing != nil &&
		proxy.Routing.StartupOptionKey != "" {
		return proxy.Routing.StartupOptionKey
	}
	return "pgelastic.tenant"
}

// requireTLS refuses a client that reaches the startup packet without having negotiated
// TLS. It follows the declared client-facing mode: anything short of Require is a posture
// that permits plaintext, and turning it into a refusal would reject clients the pool said
// it would serve.
func requireTLS(pool *pgelasticv1alpha1.PgElasticPool) bool {
	proxy := pool.Spec.Proxy
	if proxy == nil || proxy.TLS == nil {
		return false
	}
	switch proxy.TLS.Mode {
	case pgelasticv1alpha1.TLSRequire, pgelasticv1alpha1.TLSVerifyCA,
		pgelasticv1alpha1.TLSVerifyFull:
		return true
	default:
		return false
	}
}

func backendTLSMode(pool *pgelasticv1alpha1.PgElasticPool, hasCA bool) string {
	proxy := pool.Spec.Proxy
	if proxy == nil || proxy.TLS == nil || proxy.TLS.BackendTLS == nil {
		return "Disable"
	}
	switch proxy.TLS.BackendTLS.Mode {
	case pgelasticv1alpha1.TLSVerifyFull, pgelasticv1alpha1.TLSVerifyCA:
		if hasCA {
			return "VerifyFull"
		}
		// VerifyFull without a CA is refused by the proxy at start-up, so rendering it
		// would produce a fleet that cannot boot. Require still encrypts.
		return "Require"
	case pgelasticv1alpha1.TLSRequire:
		return "Require"
	default:
		return "Disable"
	}
}
