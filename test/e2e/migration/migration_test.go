//go:build e2e

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
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
)

const (
	e2eNamespace = "pgelastic-e2e-migration"
	// The two instances are short-named so every generated slot and member name stays well
	// inside the identifier limits it ends up in.
	instanceA = "mg-a"
	instanceB = "mg-b"
	poolName  = "mg-pool"
	// sizingClass is the development tier: six postmasters have to fit on one node.
	sizingClass = "dev-1"
	// tenantDatabase is both the PgTenant name and its database.
	tenantDatabase = "acme"
	// neighbourDatabase is a second tenant, pinned to the other instance and never moved.
	// It is what makes "the tenant that was migrated was paused" a claim with a control:
	// without it, a latency spike everybody saw would be indistinguishable from one only the
	// migrated tenant saw.
	neighbourDatabase = "neighbour"
	className         = "mg-class"
	workloadClassName = "mg-standard"
	// proxyReplicas is two because the gate is per-replica in-memory state and kube-proxy
	// pins a connection to one endpoint for its life: a cutover that quiesced only the
	// replica the operator happened to reach first would still be green with one.
	proxyReplicas = 2
	// seedRows is enough data for an initial table sync and a content checksum to mean
	// something, and few enough that the whole suite stays inside its timeout.
	seedRows = 20000
)

// probeDatabase is where the schema-copy specs apply their copy, so that driving the copy
// directly cannot disturb the database a real migration is moving.
const probeDatabase = "acme_copy_probe"

const (
	// probeInterval paces the client held across the cutover. Short enough that a
	// sub-second pause is still sampled several times on either side of itself.
	probeInterval = 20 * time.Millisecond
	// baselineWindow is how long the probes run before the migration is created, which is
	// what "during the cutover" is compared against.
	baselineWindow = 5 * time.Second
	// neighbourDisturbanceCeiling is the worst statement the untouched tenant may see. It is
	// far above any normal statement and far below the pause the migrated tenant is expected
	// to show, so it separates "somebody else's move" from "a fleet-wide stall" without
	// being a threshold anybody has to tune.
	neighbourDisturbanceCeiling = 500 * time.Millisecond
)

// lastNeighbourReport carries the control's measurements from the cutover spec to the spec
// that asserts on them. They cannot be gathered twice: the cutover happens once.
var lastNeighbourReport probeReport

func endpoint(instance, database string) migration.Endpoint {
	return migration.Endpoint{Namespace: e2eNamespace, Instance: instance, Database: database}
}

func query(instance, database, statement string) string {
	GinkgoHelper()
	rows, err := sql.Query(suiteCtx, endpoint(instance, database), statement)
	Expect(err).NotTo(HaveOccurred(), "query on %s/%s: %s", instance, database, statement)
	if len(rows) == 0 || len(rows[0]) == 0 {
		return ""
	}
	return strings.TrimSpace(rows[0][0])
}

func exec(instance, database, statement string) {
	GinkgoHelper()
	Expect(sql.Exec(suiteCtx, endpoint(instance, database), statement)).
		To(Succeed(), "statement on %s/%s: %s", instance, database, statement)
}

func makeInstance(name string, walSize string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: poolName},
			Class:   sizingClass,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      resource.MustParse("2Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse(walSize)},
			},
		},
	}
}

func awaitReady(name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		instance := &pgelasticv1alpha1.PgInstance{}
		g.Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance)).To(Succeed())
		g.Expect(instance.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
		g.Expect(instance.Status.CurrentPrimary).NotTo(BeEmpty())
		// The failover-slot preflight needs the standbys counted towards the quorum, which
		// is a state the instance reaches a reconcile after it first reports Ready.
		g.Expect(instance.Status.QuorumEvidence).NotTo(BeNil())
		g.Expect(instance.Status.QuorumEvidence.VotingMembers).To(HaveLen(2))
	}).Should(Succeed(), "%s never became ready", name)
}

// publishStorage records the volume usage the storage-headroom preflight reads. The
// instance controller owns this field in a deployed cluster; here the suite supplies it so
// the gate is exercised with a real number rather than skipped.
//
// Every status write in this suite retries on conflict. A controller is reconciling these
// objects while the spec writes them, so a conflict is the normal case rather than an error
// worth failing a whole run over.
func publishStorage(name string) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		instance := &pgelasticv1alpha1.PgInstance{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance); err != nil {
			return err
		}
		instance.Status.Storage = &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: ptr.To(resource.MustParse("2Gi")),
			Used:      ptr.To(resource.MustParse("256Mi")),
		}
		return k8sClient.Status().Update(suiteCtx, instance)
	})).To(Succeed())
}

// seedTenant creates the tenant's database on one instance and fills it with a schema the
// online path can actually carry: every table has a primary key, and there is a sequence,
// because a sequence is the thing PostgreSQL 18 logical replication does not move.
func seedTenant(instance string) {
	GinkgoHelper()
	exec(instance, "postgres", fmt.Sprintf(
		`CREATE ROLE %s LOGIN; CREATE DATABASE %s OWNER %s TEMPLATE template0`,
		tenantDatabase, tenantDatabase, tenantDatabase))

	exec(instance, tenantDatabase, `
CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  customer text NOT NULL,
  amount numeric(12,2) NOT NULL,
  placed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE line_items (
  id bigserial PRIMARY KEY,
  order_id bigint NOT NULL REFERENCES orders(id),
  sku text NOT NULL,
  quantity int NOT NULL
);
CREATE INDEX line_items_order_id_idx ON line_items (order_id)`)

	exec(instance, tenantDatabase, fmt.Sprintf(`
INSERT INTO orders (customer, amount, placed_at)
SELECT 'customer-' || g, (g %% 997)::numeric / 7, timestamptz '2026-01-01 00:00:00+00' + (g || ' seconds')::interval
FROM generate_series(1, %d) g;
INSERT INTO line_items (order_id, sku, quantity)
SELECT id, 'sku-' || (id %% 53), (id %% 7) + 1 FROM orders`, seedRows))
}

func makeTenant() *pgelasticv1alpha1.PgTenant { return makeNamedTenant(tenantDatabase) }

func makeNamedTenant(name string) *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: poolName},
			DatabaseName: name,
		},
	}
}

func bindTenant(instance string) { bindNamedTenant(tenantDatabase, instance) }

// bindNamedTenant publishes the binding by hand. The tenant controller is deliberately not
// running in this suite - what it would do is place tenants, and a spec that asserts "acme
// starts on mg-a" has to be the thing that decided it.
func bindNamedTenant(name, instance string) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tenant := &pgelasticv1alpha1.PgTenant{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, tenant); err != nil {
			return err
		}
		tenant.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: instance},
		}
		tenant.Status.Utilization = &pgelasticv1alpha1.PgTenantUtilization{IsCold: ptr.To(true)}
		return k8sClient.Status().Update(suiteCtx, tenant)
	})).To(Succeed())
}

// seedNeighbour puts a second tenant on the other instance. It is never migrated: it is the
// control that tells a tenant move apart from a fleet-wide stall.
func seedNeighbour(instance string) {
	GinkgoHelper()
	exec(instance, "postgres", fmt.Sprintf(
		`CREATE DATABASE %s TEMPLATE template0`, neighbourDatabase))
}

// poolObjects are the pool and the two classes it resolves through. The pool declares a
// proxy fleet, which is the only reason any of this is here: a cutover cannot queue clients
// at a fleet that does not exist.
func poolObjects() []client.Object {
	return []client.Object{
		&pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
		},
		&pgelasticv1alpha1.PgWorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: workloadClassName},
			Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
				Priority: 1000,
				Capacity: pgelasticv1alpha1.WorkloadCapacity{
					Guaranteed: ptr.To(int32(1)),
					Burstable:  8,
				},
			},
		},
		&pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: e2eNamespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     "PgElasticClass",
					Name:     className,
				},
				Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 100},
				// The template is required by the schema and is never acted on here: the pool
				// controller creates no PgInstance, and the two members this suite drives are
				// the ones it creates itself.
				Instances: pgelasticv1alpha1.PoolInstances{
					Replicas: ptr.To(int32(2)),
					Template: pgelasticv1alpha1.PgInstanceTemplate{
						Class: sizingClass,
						Storage: pgelasticv1alpha1.InstanceStorage{
							Size:      resource.MustParse("2Gi"),
							WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("512Mi")},
						},
					},
				},
				Admission: &pgelasticv1alpha1.PoolAdmission{
					DefaultWorkloadClassName: workloadClassName,
				},
				Pooling: &pgelasticv1alpha1.PoolingConfig{
					Mode: pgelasticv1alpha1.PoolModeTransaction,
				},
				Proxy: &pgelasticv1alpha1.ProxySpec{
					Replicas: ptr.To(int32(proxyReplicas)),
					Workers:  ptr.To(int32(2)),
					Routing: &pgelasticv1alpha1.ProxyRouting{
						// The database name is the only discriminator every PostgreSQL client
						// already sends, and it is what these tenants are keyed on.
						TenantDiscriminators: []pgelasticv1alpha1.TenantDiscriminator{
							pgelasticv1alpha1.DiscriminatorDatabaseName,
						},
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
				},
			},
		},
	}
}

// awaitFleet waits for the Deployment to have finished rolling rather than merely to have
// enough ready replicas: while the operator is still replacing replicas because an instance
// has just published its address, anything that attaches to one of those Pods is about to
// lose it.
func awaitFleet(replicas int32) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
		}, deployment)).To(Succeed())
		g.Expect(deployment.Status.ObservedGeneration).To(Equal(deployment.Generation))
		g.Expect(deployment.Status.Replicas).To(Equal(replicas))
		g.Expect(deployment.Status.UpdatedReplicas).To(Equal(replicas))
		g.Expect(deployment.Status.ReadyReplicas).To(Equal(replicas),
			"the fleet has %d/%d replicas ready", deployment.Status.ReadyReplicas, replicas)
	}, "10m", "5s").Should(Succeed())

	// The operator's client certificate is what every control call is made under, so a fleet
	// that is up but unreachable has to fail here rather than inside a cutover.
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: e2eNamespace, Name: proxyobjects.ControlClientSecretName(poolName),
		}, &corev1.Secret{})
	}, "5m", "5s").Should(Succeed(),
		"cert-manager never issued the operator's control certificate, so no cutover could "+
			"reach the fleet's gate")
}

// currentDatabaseThrough opens one connection through the pool's Service and asks
// PostgreSQL which database answered.
func currentDatabaseThrough(database string) string {
	GinkgoHelper()
	forward, err := forwardPod(serviceEndpointPod(), proxyobjects.DefaultClientPort)
	Expect(err).NotTo(HaveOccurred())
	defer forward.close()

	var name string
	Eventually(func(g Gomega) {
		connection, err := pgx.Connect(suiteCtx, forward.dsn(provision.OpsRole, database))
		g.Expect(err).NotTo(HaveOccurred())
		defer func() { _ = connection.Close(suiteCtx) }()
		g.Expect(connection.QueryRow(suiteCtx, "SELECT current_database()").Scan(&name)).To(Succeed())
	}, "2m", "2s").Should(Succeed(),
		"nothing answered for %s through the pool Service; the forward said:\n%s",
		database, forward.log())
	return name
}

// annotate is how an operator asks a running migration to stop or to roll back. It retries
// on conflict because the controller is writing the same object's status throughout.
func annotate(name, key, value string) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object := &pgelasticv1alpha1.PgTenantMigration{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, object); err != nil {
			return err
		}
		if object.Annotations == nil {
			object.Annotations = map[string]string{}
		}
		object.Annotations[key] = value
		return k8sClient.Update(suiteCtx, object)
	})).To(Succeed())
}

func routedInstance() string {
	GinkgoHelper()
	tenant := &pgelasticv1alpha1.PgTenant{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: tenantDatabase}, tenant)).To(Succeed())
	if tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
		return ""
	}
	return tenant.Status.Binding.InstanceRef.Name
}

func makeMigration(name, target string, strategy pgelasticv1alpha1.TenantMigrationStrategy,
) *pgelasticv1alpha1.PgTenantMigration {
	return &pgelasticv1alpha1.PgTenantMigration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
			TenantRef:         corev1.LocalObjectReference{Name: tenantDatabase},
			TargetInstanceRef: corev1.LocalObjectReference{Name: target},
			Strategy:          strategy,
			// The rollback window is zero so this suite can watch the source be dropped
			// rather than waiting an hour for it.
			RollbackWindow: &metav1.Duration{Duration: 0},
			// The offline path's whole dump and restore happens inside the pause, so the
			// budget has to admit it. The pause is still measured and reported; what is
			// relaxed here is the abort threshold, not the measurement.
			CutoverTimeout: &metav1.Duration{Duration: 10 * time.Minute},
			// Long enough that a deliberately held transaction parks the machine in Quiescing
			// instead of timing it out, which is what makes the abort specs deterministic.
			DrainTimeout: &metav1.Duration{Duration: 10 * time.Minute},
		},
	}
}

// holdTransactionOpen starts a backend on the source that sits inside a transaction, which
// is exactly what Quiescing waits to drain. Without it a tenant with no traffic drains
// instantly and the machine runs past the phase an abort spec meant to stop it in.
func holdTransactionOpen() {
	GinkgoHelper()
	_, err := sql.Run(suiteCtx, endpoint(instanceA, tenantDatabase), []string{"sh", "-c",
		fmt.Sprintf(`nohup psql --host=%s --username=postgres --dbname=%s `+
			`--command='BEGIN; SELECT pg_sleep(1800); COMMIT' >/dev/null 2>&1 & echo started`,
			socketDir, tenantDatabase)})
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() string {
		return query(instanceA, tenantDatabase, inFlightCountQuery)
	}, "1m", "1s").ShouldNot(Equal("0"), "no in-flight transaction was ever established")
}

func releaseHeldTransactions() {
	GinkgoHelper()
	exec(instanceA, tenantDatabase, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend'
  AND pid <> pg_backend_pid() AND query LIKE '%pg_sleep%'`)
}

const inFlightCountQuery = `SELECT count(*)::text FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend'
  AND pid <> pg_backend_pid() AND state <> 'idle'`

// socketDir is where the superuser is reachable inside every member's container.
const socketDir = "/var/run/postgresql"

func fetchMigration(name string) *pgelasticv1alpha1.PgTenantMigration {
	GinkgoHelper()
	object := &pgelasticv1alpha1.PgTenantMigration{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: name}, object)).To(Succeed())
	return object
}

func awaitPhase(name string, phase pgelasticv1alpha1.TenantMigrationPhase) *pgelasticv1alpha1.PgTenantMigration {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		object := fetchMigration(name)
		g.Expect(object.Status.Phase).To(Equal(phase),
			"migration %s is in %s: %s", name, object.Status.Phase, conditionSummary(object))
	}, "6m", "1s").Should(Succeed())
	return fetchMigration(name)
}

// probePlan aims one schema copy at a scratch database on the target, so the copy can be
// driven directly instead of through a whole migration. Its source is the tenant's real
// database, which is what makes the dump a real one.
func probePlan(database string) migration.Plan {
	GinkgoHelper()
	secret := &corev1.Secret{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: e2eNamespace, Name: provision.CredentialsSecretName(instanceA)}, secret)).To(Succeed())
	password := string(secret.Data[provision.SecretKeyReplicationPassword])
	Expect(password).NotTo(BeEmpty())

	return migration.Plan{
		Source:      endpoint(instanceA, tenantDatabase),
		Target:      endpoint(instanceB, database),
		SchemaStamp: migration.SchemaStamp(e2eNamespace, "copy-probe"),
		DumpDir:     migration.DumpDir(e2eNamespace, "copy-probe"),
		SourceConnInfo: fmt.Sprintf("host=%s.%s.svc port=%d user=%s password=%s dbname=%s",
			provision.PrimaryServiceName(instanceA), e2eNamespace, provision.PostgresPort,
			provision.ReplicationRole, password, tenantDatabase),
	}
}

func relationCount(relation string) string {
	GinkgoHelper()
	return query(instanceB, probeDatabase, fmt.Sprintf(
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relname = '%s'`, relation))
}

// schemaStampOf reads the mark a committed schema copy leaves. It is asked of the postgres
// database because pg_shdescription is shared, which is also how the operator asks it.
func schemaStampOf(database string) string {
	GinkgoHelper()
	return query(instanceB, "postgres", fmt.Sprintf(
		`SELECT coalesce(max(shobj_description(oid, 'pg_database')), '')
		 FROM pg_database WHERE datname = '%s'`, database))
}

func conditionNamed(object *pgelasticv1alpha1.PgTenantMigration, name string) *metav1.Condition {
	for index := range object.Status.Conditions {
		if object.Status.Conditions[index].Type == name {
			return &object.Status.Conditions[index]
		}
	}
	return nil
}

func conditionSummary(object *pgelasticv1alpha1.PgTenantMigration) string {
	parts := make([]string, 0, len(object.Status.Conditions))
	for _, condition := range object.Status.Conditions {
		parts = append(parts, fmt.Sprintf("%s=%s(%s: %s)",
			condition.Type, condition.Status, condition.Reason, condition.Message))
	}
	return strings.Join(parts, " | ")
}

// reportPause is the product commitment this whole suite exists to check, so it is printed
// rather than only asserted: a number nobody reads is a number nobody notices regressing.
func reportPause(object *pgelasticv1alpha1.PgTenantMigration) {
	GinkgoHelper()
	Expect(object.Status.PauseDurationMillis).NotTo(BeNil(), "no pause was measured at all")
	AddReportEntry(fmt.Sprintf("pauseDurationMillis (%s)", object.Spec.Strategy),
		*object.Status.PauseDurationMillis)

	// The two numbers are different things and both are published. pauseDurationMillis is
	// the controller's own wall clock across the quiesced phases; clientPauseMillis is how
	// long the gate was actually shut, which is the number the product's claim is about. A
	// pool with a fleet has to report it: an empty one would mean nothing was ever held.
	Expect(object.Status.ClientPauseMillis).NotTo(BeNil(),
		"no client pause was reported, so nothing held this tenant's clients at all")
	AddReportEntry(fmt.Sprintf("clientPauseMillis (%s)", object.Spec.Strategy),
		*object.Status.ClientPauseMillis)
	GinkgoWriter.Printf("\n=== %s migration %s: pauseDurationMillis = %d, clientPauseMillis = %d\n",
		object.Spec.Strategy, object.Name,
		*object.Status.PauseDurationMillis, *object.Status.ClientPauseMillis)
}

var _ = Describe("Moving a tenant between two PostgreSQL 18 instances", Ordered, func() {
	var namespace *corev1.Namespace

	BeforeAll(func() {
		namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, namespace))).To(Succeed())

		for _, object := range poolObjects() {
			Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, object))).To(Succeed())
		}

		// The source's WAL volume is deliberately small, so max_slot_wal_keep_size - two
		// fifths of it - is a bound this suite can actually push a slot past.
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeInstance(instanceA, "512Mi")))).To(Succeed())
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeInstance(instanceB, "512Mi")))).To(Succeed())

		awaitReady(instanceA)
		awaitReady(instanceB)
		publishStorage(instanceA)
		publishStorage(instanceB)

		seedTenant(instanceA)
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, makeTenant()))).To(Succeed())
		bindTenant(instanceA)

		seedNeighbour(instanceB)
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeNamedTenant(neighbourDatabase)))).To(Succeed())
		bindNamedTenant(neighbourDatabase, instanceB)
	})

	// The fleet has to exist before anything can be queued at it, and the routing has to be
	// answered by PostgreSQL rather than by the operator: a client that reached the wrong
	// instance would still report a pause, and would be reporting it about the wrong thing.
	It("brings up the pool's proxy fleet and routes each tenant to its own instance", func() {
		awaitFleet(proxyReplicas)
		// Each database exists on exactly one instance, so reaching it at all is the routing
		// claim: a connection sent to the other member would be refused for a database that
		// is not there rather than answering with the wrong data.
		Expect(currentDatabaseThrough(tenantDatabase)).To(Equal(tenantDatabase))
		Expect(currentDatabaseThrough(neighbourDatabase)).To(Equal(neighbourDatabase))
	})

	It("runs the source with a finite bound on what one abandoned slot can retain", func() {
		bound := query(instanceA, "postgres", "SHOW max_slot_wal_keep_size")
		Expect(bound).NotTo(Equal("-1"),
			"an unbounded max_slot_wal_keep_size lets one abandoned migration slot fill the primary's disk")
		megabytes, err := strconv.Atoi(strings.TrimSuffix(bound, "MB"))
		Expect(err).NotTo(HaveOccurred(), "unexpected max_slot_wal_keep_size %q", bound)
		Expect(megabytes).To(BeNumerically("<", 512),
			"the retention bound has to be smaller than the WAL volume it lives on")
	})

	Context("by logical replication", func() {
		const migrationName = "online-move"

		// The load-bearing spec, and the only one that answers the product's actual claim.
		//
		// A client holding an open connection through the pool's Service across the cutover
		// sees a latency spike and no error. Both halves matter and neither on its own is
		// evidence: no error alone would also be true of a client that never noticed anything
		// because nothing held it, and a spike alone would also be true of a client that was
		// dropped and reconnected. The neighbour on the other instance is the control.
		//
		// Nothing here reads the operator's own account of its pause. That number is measured
		// by the thing under suspicion; this one is measured by the client.
		It("carries the tenant onto the target with its clients queued and never dropped", func() {
			held := startProbe("the migrated tenant", provision.OpsRole,
				tenantDatabase, probeInterval)
			defer held.stop()
			neighbour := startProbe("the neighbour on the other instance", provision.OpsRole,
				neighbourDatabase, probeInterval)
			defer neighbour.stop()

			// A baseline first. "During the cutover" is only a meaningful window if there is
			// something before it to compare against.
			time.Sleep(baselineWindow)
			held.mark()
			neighbour.mark()

			Expect(k8sClient.Create(suiteCtx,
				makeMigration(migrationName, instanceB, pgelasticv1alpha1.TenantMigrationOnline))).To(Succeed())

			object := awaitPhase(migrationName, pgelasticv1alpha1.TenantMigrationPhaseCompleted)
			Expect(routedInstance()).To(Equal(instanceB))
			reportPause(object)

			held.stop()
			neighbour.stop()
			heldReport := held.report()
			lastNeighbourReport = neighbour.report()
			neighbourReport := lastNeighbourReport
			AddReportEntry("client through the proxy", heldReport.String())
			AddReportEntry("neighbour through the proxy", neighbourReport.String())
			GinkgoWriter.Printf("\n=== %s\n=== %s\n", heldReport, neighbourReport)

			Expect(heldReport.failures).To(BeEmpty(),
				"the client was dropped rather than queued: %v", heldReport.failures)
			Expect(heldReport.duringCount).To(BeNumerically(">", 0),
				"no statement was issued during the cutover, so nothing was measured")
			Expect(heldReport.duringMax).To(BeNumerically(">", heldReport.beforeP99*10),
				"the client saw no pause at all across the cutover (max %s against a "+
					"baseline p99 of %s), so nothing ever held it and the move was not a "+
					"queued one", heldReport.duringMax, heldReport.beforeP99)

			// The same socket, before and after. It was answered by one backend address and
			// then by another, which is a tenant that moved underneath a connection that never
			// closed - and is exactly what a client that had been dropped and had silently
			// reconnected could not produce, because that one would have shown an error first.
			Expect(heldReport.servers).To(HaveLen(2),
				"the held connection was served by %v across a move between two instances",
				heldReport.servers)
		})

		// The control. A pause every tenant in the pool sees is a fleet-wide stall and not a
		// tenant move, and the two are indistinguishable without measuring both.
		It("left the tenant on the other instance undisturbed", func() {
			report := lastNeighbourReport
			Expect(report.duringCount).To(BeNumerically(">", 0))
			Expect(report.failures).To(BeEmpty(),
				"the neighbour was disturbed by somebody else's migration: %v", report.failures)
			Expect(report.servers).To(HaveLen(1),
				"the neighbour was moved by somebody else's migration; it was served by %v",
				report.servers)
			Expect(report.duringMax).To(BeNumerically("<", neighbourDisturbanceCeiling),
				"the neighbour's worst statement during the cutover took %s, which is a stall "+
					"rather than a move of somebody else's tenant", report.duringMax)
		})

		It("opened a failover-enabled slot rather than one a failover would destroy", func() {
			// The slot is dropped by the cleanup ladder on success, so what is checked here is
			// the durable evidence: the migration recorded the slot it owned, and the source
			// no longer holds it.
			object := fetchMigration(migrationName)
			Expect(object.Status.ReplicationSlotName).NotTo(BeEmpty())
			Expect(query(instanceA, "postgres", fmt.Sprintf(
				"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '%s'",
				object.Status.ReplicationSlotName))).To(Equal("0"),
				"the migration slot was left pinning the source primary's WAL")
		})

		It("moved every row, and the verifier says so", func() {
			object := fetchMigration(migrationName)
			Expect(object.Status.Verification).NotTo(BeNil())
			Expect(object.Status.Verification.SchemaFingerprintMatch).To(HaveValue(BeTrue()))
			Expect(object.Status.Verification.RowCountsMatch).To(HaveValue(BeTrue()))

			Expect(query(instanceB, tenantDatabase, "SELECT count(*) FROM orders")).
				To(Equal(strconv.Itoa(seedRows)))
			Expect(query(instanceB, tenantDatabase, "SELECT count(*) FROM line_items")).
				To(Equal(strconv.Itoa(seedRows)))
		})

		It("publishes what the verifier cannot prove alongside its verdict", func() {
			verified := conditionNamed(fetchMigration(migrationName), migration.ConditionVerified)
			Expect(verified).NotTo(BeNil())
			Expect(verified.Status).To(Equal(metav1.ConditionTrue))
			Expect(verified.Message).To(ContainSubstring("equivalence is not correctness"))
		})

		It("hands the tenant a database carrying no mark of the machinery that built it", func() {
			Expect(schemaStampOf(tenantDatabase)).To(BeEmpty())
		})

		// Logical replication carries no sequence state through PostgreSQL 18. Without the
		// setval the target's sequence would still be at 1, and the first insert after the
		// move would collide with a row that was copied in.
		It("advanced the target's sequence past the source's with a safety gap", func() {
			nextValue := query(instanceB, tenantDatabase,
				"SELECT nextval('orders_id_seq')")
			next, err := strconv.Atoi(nextValue)
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(BeNumerically(">", seedRows+int(migration.DefaultSafetyGap)-2),
				"the target's sequence was not carried across, so the next insert would duplicate a key")

			Expect(sql.Exec(suiteCtx, endpoint(instanceB, tenantDatabase),
				"INSERT INTO orders (customer, amount) VALUES ('post-move', 1.00)")).To(Succeed())
		})

		It("left the source fenced rather than merely unrouted", func() {
			// rollbackWindow is zero in this suite, so the source is dropped as soon as the
			// migration is looked at again. Either state proves the source is not serving.
			Eventually(func() string {
				return query(instanceA, "postgres", fmt.Sprintf(
					"SELECT coalesce(bool_or(datallowconn)::text, 'dropped') FROM pg_database WHERE datname = '%s'",
					tenantDatabase))
			}, "2m", "2s").Should(Or(Equal("false"), Equal("dropped")))
		})
	})

	Context("by dump and restore", func() {
		const migrationName = "offline-move"

		BeforeAll(func() {
			// The tenant now lives on B. Moving it back exercises the offline path on a
			// database that was itself produced by the online one.
			Expect(k8sClient.Create(suiteCtx,
				makeMigration(migrationName, instanceA, pgelasticv1alpha1.TenantMigrationOffline))).To(Succeed())
		})

		It("carries the tenant back with pg_dump and pg_restore", func() {
			object := awaitPhase(migrationName, pgelasticv1alpha1.TenantMigrationPhaseCompleted)
			Expect(routedInstance()).To(Equal(instanceA))
			reportPause(object)

			Expect(object.Status.Verification).NotTo(BeNil())
			Expect(object.Status.Verification.RowCountsMatch).To(HaveValue(BeTrue()))
			Expect(query(instanceA, tenantDatabase, "SELECT count(*) FROM orders")).
				To(Equal(strconv.Itoa(seedRows + 1)))
		})

		It("left no staged dump behind on the target's data volume", func() {
			output, err := sql.Run(suiteCtx, endpoint(instanceA, "postgres"),
				[]string{"sh", "-c", "ls " + migration.ScratchDir + " 2>/dev/null | wc -l"})
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(string(output))).To(Equal("0"),
				"a directory-format dump is the size of the tenant; leaving one behind fails the "+
					"next migration's storage headroom check for reasons nobody can find")
		})
	})

	Context("when preflight refuses", func() {
		It("names the relations without a replica identity rather than degrading quietly", func() {
			exec(instanceA, tenantDatabase,
				"CREATE TABLE audit_trail (message text NOT NULL, at timestamptz DEFAULT now())")
			DeferCleanup(func() {
				exec(instanceA, tenantDatabase, "DROP TABLE IF EXISTS audit_trail")
			})

			object := makeMigration("refused-move", instanceB, pgelasticv1alpha1.TenantMigrationOnline)
			Expect(k8sClient.Create(suiteCtx, object)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, object))).To(Succeed())
			})

			var refusal metav1.Condition
			Eventually(func(g Gomega) {
				current := fetchMigration(object.Name)
				g.Expect(current.Status.Phase).
					To(Equal(pgelasticv1alpha1.TenantMigrationPhasePreflight))
				for _, condition := range current.Status.Conditions {
					if condition.Type == migration.ConditionPreflightPassed {
						refusal = condition
					}
				}
				g.Expect(refusal.Status).To(Equal(metav1.ConditionFalse))
			}, "2m", "2s").Should(Succeed())

			Expect(refusal.Reason).To(Equal(migration.ReasonPreflightRefused))
			Expect(refusal.Message).To(ContainSubstring("public.audit_trail"))
			Expect(refusal.Message).To(ContainSubstring("REPLICA IDENTITY FULL"))
			Expect(routedInstance()).To(Equal(instanceA),
				"a refused migration must leave the tenant exactly where it was")
		})
	})

	// Provisioning is retried, so it has to be re-enterable. These specs drive the schema copy
	// directly against the two real instances, because what is being checked is what
	// PostgreSQL does with a transaction that does not commit - which no fake can answer.
	Context("when the schema copy is restarted", func() {
		var probe migration.Plan

		BeforeEach(func() {
			probe = probePlan(probeDatabase)
			// pg_dump takes an ACCESS SHARE lock on every relation it reads, even for a
			// schema-only dump, so the replication role needs the same reads a real migration
			// opens for it.
			Expect(migration.GrantSourceReads(
				suiteCtx, sql, probe.Source, provision.ReplicationRole)).To(Succeed())
			DeferCleanup(func() {
				exec(instanceB, "postgres",
					fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, probeDatabase))
			})
		})

		It("treats a target that already carries the schema as a satisfied precondition", func() {
			Expect(migration.ProvisionTarget(suiteCtx, sql, sql, probe, tenantDatabase, true)).To(Succeed())
			Expect(migration.ProvisionTarget(suiteCtx, sql, sql, probe, tenantDatabase, true)).
				To(Succeed(), "a second provisioning pass failed on the schema it had itself copied")

			Expect(relationCount("line_items")).To(Equal("1"))
			Expect(relationCount("orders")).To(Equal("1"))
		})

		It("leaves nothing behind when the copy does not commit, and converges on the retry", func() {
			// One relation the dump also creates makes the apply fail exactly as the nightly
			// did, part-way through a schema it has already begun creating.
			exec(instanceB, "postgres", fmt.Sprintf(`CREATE DATABASE %q TEMPLATE template0`, probeDatabase))
			exec(instanceB, probeDatabase, `CREATE TABLE orders (id bigint)`)

			err := migration.ProvisionTarget(suiteCtx, sql, sql, probe, tenantDatabase, true)
			Expect(err).To(HaveOccurred(), "a copy onto a relation that already exists reported success")
			Expect(err.Error()).To(ContainSubstring("already exists"))

			Expect(relationCount("line_items")).To(Equal("0"),
				"the failed apply left objects behind, so the retry can only fail on them")
			Expect(schemaStampOf(probeDatabase)).To(BeEmpty(),
				"a copy that did not commit stamped the database as though it had")

			exec(instanceB, probeDatabase, `DROP TABLE orders`)
			Expect(migration.ProvisionTarget(suiteCtx, sql, sql, probe, tenantDatabase, true)).
				To(Succeed(), "the retry did not converge")
			Expect(relationCount("line_items")).To(Equal("1"))
			Expect(schemaStampOf(probeDatabase)).To(Equal(probe.SchemaStamp))
		})
	})

	// The retry budget exists for transient failures, and nothing else in this suite ever
	// spends any of it. This spec obstructs Provisioning, watches the machine report itself
	// retrying, clears the obstruction and requires it to recover on its own.
	Context("when a phase faults and then recovers", func() {
		It("retries the schema copy until it converges rather than until the budget runs out", func() {
			holdTransactionOpen()
			DeferCleanup(releaseHeldTransactions)

			exec(instanceB, "postgres",
				fmt.Sprintf(`CREATE DATABASE %q TEMPLATE template0`, tenantDatabase))
			exec(instanceB, tenantDatabase, `CREATE TABLE orders (id bigint)`)
			DeferCleanup(func() {
				exec(instanceB, "postgres",
					fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, tenantDatabase))
			})

			name := "retried-move"
			object := makeMigration(name, instanceB, pgelasticv1alpha1.TenantMigrationOnline)
			Expect(k8sClient.Create(suiteCtx, object)).To(Succeed())

			Eventually(func(g Gomega) {
				current := fetchMigration(name)
				retrying := conditionNamed(current, migration.ConditionRetrying)
				g.Expect(retrying).NotTo(BeNil())
				g.Expect(retrying.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(retrying.Message).To(ContainSubstring("already exists"))
				g.Expect(current.Status.Phase).
					To(Equal(pgelasticv1alpha1.TenantMigrationPhaseProvisioning))
			}, "3m", "1s").Should(Succeed())
			AddReportEntry("faulted in phase", string(fetchMigration(name).Status.Phase))

			exec(instanceB, tenantDatabase, `DROP TABLE orders`)

			Eventually(func(g Gomega) {
				current := fetchMigration(name)
				g.Expect(reached(current.Status.Phase, pgelasticv1alpha1.TenantMigrationPhaseCopying)).
					To(BeTrue(), "still in %s: %s", current.Status.Phase, conditionSummary(current))
			}, "5m", "1s").Should(Succeed())

			annotate(name, migrationAbortAnnotation, "stopped by the e2e suite after the retry recovered")
			aborted := awaitPhase(name, pgelasticv1alpha1.TenantMigrationPhaseAborted)
			Expect(routedInstance()).To(Equal(instanceA))
			Expect(query(instanceB, "postgres",
				"SELECT count(*) FROM pg_database WHERE datname = '"+tenantDatabase+"'")).To(Equal("0"))
			Expect(k8sClient.Delete(suiteCtx, aborted)).To(Succeed())
		})
	})

	Context("when a migration is aborted", func() {
		// One held transaction parks every migration in this context at Quiescing, so a spec
		// aiming at an earlier phase can never overshoot past the pause and into a cutover it
		// did not mean to test.
		BeforeAll(func() {
			holdTransactionOpen()
			DeferCleanup(releaseHeldTransactions)
		})

		for _, phase := range []pgelasticv1alpha1.TenantMigrationPhase{
			pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
			pgelasticv1alpha1.TenantMigrationPhasePreWarm,
			pgelasticv1alpha1.TenantMigrationPhaseCopying,
			pgelasticv1alpha1.TenantMigrationPhaseCatchup,
			pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
		} {
			It("leaves the tenant serving from the source when stopped at or after "+string(phase), func() {
				name := "abort-" + strings.ToLower(string(phase))
				object := makeMigration(name, instanceB, pgelasticv1alpha1.TenantMigrationOnline)
				Expect(k8sClient.Create(suiteCtx, object)).To(Succeed())

				Eventually(func(g Gomega) {
					current := fetchMigration(name)
					g.Expect(reached(current.Status.Phase, phase)).To(BeTrue(),
						"still in %s: %s", current.Status.Phase, conditionSummary(current))
				}, "5m", "100ms").Should(Succeed())

				// The phase actually reached is recorded rather than assumed: a reconcile loop
				// can pass through a phase between two polls, and a spec that claimed to have
				// aborted in one phase while really aborting in a later one would be reporting
				// coverage it does not have.
				AddReportEntry("aborted from phase", string(fetchMigration(name).Status.Phase))
				annotate(name, migrationAbortAnnotation, "stopped by the e2e suite in "+string(phase))

				aborted := awaitPhase(name, pgelasticv1alpha1.TenantMigrationPhaseAborted)
				Expect(routedInstance()).To(Equal(instanceA),
					"aborting in %s left the tenant somewhere other than the source", phase)

				// Everything the abort was supposed to reap is gone, and the tenant's clients
				// are no longer held.
				Expect(query(instanceA, "postgres", fmt.Sprintf(
					"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '%s'",
					aborted.Status.ReplicationSlotName))).To(Equal("0"))
				Expect(query(instanceA, tenantDatabase, fmt.Sprintf(
					"SELECT count(*) FROM pg_publication WHERE pubname = '%s'",
					aborted.Status.PublicationName))).To(Equal("0"))
				Expect(query(instanceA, "postgres",
					"SELECT datallowconn::text FROM pg_database WHERE datname = '"+tenantDatabase+"'")).
					To(Equal("true"), "an abort left the source refusing connections")
				Expect(query(instanceB, "postgres",
					"SELECT count(*) FROM pg_database WHERE datname = '"+tenantDatabase+"'")).
					To(Equal("0"), "an abort left a half-built copy of the tenant on the target, which "+
						"stopped receiving changes at an arbitrary instant and looks exactly like a "+
						"complete one")

				Expect(k8sClient.Delete(suiteCtx, aborted)).To(Succeed())
			})
		}
	})

	Context("when a migration is abandoned rather than aborted", func() {
		It("sweeps the objects it left behind and never lets the slot outgrow its bound", func() {
			// The migration has to still be in flight when it is taken away, and a tenant with
			// no traffic finishes in seconds. One held transaction parks it at the quiesce,
			// which is what makes "abandoned" a state this spec can actually produce.
			holdTransactionOpen()
			DeferCleanup(releaseHeldTransactions)

			name := "abandoned-move"
			object := makeMigration(name, instanceB, pgelasticv1alpha1.TenantMigrationOnline)
			Expect(k8sClient.Create(suiteCtx, object)).To(Succeed())

			var slot string
			subscription := migration.SubscriptionName(e2eNamespace, name)
			// The subscription is created last, so waiting for it is what guarantees the whole
			// set of physical objects exists before the migration is taken away.
			Eventually(func(g Gomega) {
				current := fetchMigration(name)
				slot = current.Status.ReplicationSlotName
				g.Expect(slot).NotTo(BeEmpty())
				g.Expect(query(instanceA, "postgres", fmt.Sprintf(
					"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '%s'", slot))).
					To(Equal("1"))
				g.Expect(query(instanceB, "postgres", fmt.Sprintf(
					"SELECT count(*) FROM pg_subscription WHERE subname = '%s'", subscription))).
					To(Equal("1"))
			}, "5m", "1s").Should(Succeed())

			// Deleting the object is exactly the case the cleanup ladder cannot cover: there is
			// no longer anything to run it.
			Expect(k8sClient.Delete(suiteCtx, object)).To(Succeed())
			Eventually(func() bool {
				return apiAbsent(name)
			}, "2m", "1s").Should(BeTrue())

			// A subscriber that keeps consuming keeps the slot moving, so the dangerous state
			// is the other one: a slot nobody is reading. Disabling the subscription is what a
			// target that went away looks like from the source.
			exec(instanceB, tenantDatabase, fmt.Sprintf(`ALTER SUBSCRIPTION %q DISABLE`, subscription))

			bound := walKeepBoundBytes()
			for range 5 {
				// Logical messages generate WAL without writing heap, which pushes the
				// abandoned slot past its retention bound without also filling the data volume.
				exec(instanceA, tenantDatabase,
					`SELECT pg_logical_emit_message(false, 'filler', repeat('x', 6000))
					 FROM generate_series(1, 20000)`)
				exec(instanceA, "postgres", "SELECT pg_switch_wal()")
				exec(instanceA, "postgres", "CHECKPOINT")

				used := walBytesInUse()
				Expect(used).To(BeNumerically("<", bound+walSlackBytes),
					"pg_wal reached %d bytes with an abandoned slot against a max_slot_wal_keep_size "+
						"of %d; the bound is what stops one abandoned migration filling the primary's disk",
					used, bound)
			}
			AddReportEntry("pg_wal bytes with an abandoned slot", walBytesInUse())

			Expect(sweeper.Sweep(suiteCtx)).To(Succeed())
			Expect(query(instanceA, "postgres", fmt.Sprintf(
				"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '%s'", slot))).
				To(Equal("0"), "the sweeper left an abandoned slot behind")
			Expect(query(instanceB, "postgres", fmt.Sprintf(
				"SELECT count(*) FROM pg_subscription WHERE subname = '%s'", subscription))).To(Equal("0"))
		})
	})
})

// migrationAbortAnnotation is spelled here rather than imported so the e2e asserts on the
// annotation an operator would actually set.
const migrationAbortAnnotation = "pgelastic.io/abort"

// phaseOrder is the online strategy's order, used to decide whether a migration has reached
// or passed a phase. A reconcile loop can move through a phase between two polls, so a spec
// that waited for exact equality would hang on a migration that had already gone further.
var phaseOrder = []pgelasticv1alpha1.TenantMigrationPhase{
	pgelasticv1alpha1.TenantMigrationPhasePreflight,
	pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
	pgelasticv1alpha1.TenantMigrationPhasePreWarm,
	pgelasticv1alpha1.TenantMigrationPhaseCopying,
	pgelasticv1alpha1.TenantMigrationPhaseCatchup,
	pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
	pgelasticv1alpha1.TenantMigrationPhaseCutover,
	pgelasticv1alpha1.TenantMigrationPhaseCompleted,
}

func reached(current, wanted pgelasticv1alpha1.TenantMigrationPhase) bool {
	currentIndex, wantedIndex := -1, -1
	for index, phase := range phaseOrder {
		if phase == current {
			currentIndex = index
		}
		if phase == wanted {
			wantedIndex = index
		}
	}
	return currentIndex >= 0 && currentIndex >= wantedIndex
}

func apiAbsent(name string) bool {
	GinkgoHelper()
	object := &pgelasticv1alpha1.PgTenantMigration{}
	err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: e2eNamespace, Name: name}, object)
	return err != nil
}

// walSlackBytes covers the segments PostgreSQL is writing right now plus wal_keep_size,
// neither of which max_slot_wal_keep_size governs.
const walSlackBytes = 128 << 20

func walKeepBoundBytes() int64 {
	GinkgoHelper()
	// pg_settings reports this one in megabytes, so the conversion is two factors of 1024
	// rather than one. Getting it wrong turns the assertion into one about a number a
	// thousand times too small, which passes for reasons that have nothing to do with WAL.
	value := query(instanceA, "postgres",
		"SELECT setting::bigint * 1024 * 1024 FROM pg_settings WHERE name = 'max_slot_wal_keep_size'")
	bound, err := strconv.ParseInt(value, 10, 64)
	Expect(err).NotTo(HaveOccurred(), "unreadable max_slot_wal_keep_size %q", value)
	Expect(bound).To(BeNumerically(">", 0))
	return bound
}

func walBytesInUse() int64 {
	GinkgoHelper()
	value := query(instanceA, "postgres",
		"SELECT sum(size)::bigint FROM pg_ls_waldir()")
	used, err := strconv.ParseInt(value, 10, 64)
	Expect(err).NotTo(HaveOccurred(), "unreadable pg_wal size %q", value)
	return used
}
