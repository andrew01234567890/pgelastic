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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
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
	// seedRows is enough data for an initial table sync and a content checksum to mean
	// something, and few enough that the whole suite stays inside its timeout.
	seedRows = 20000
)

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

func makeTenant() *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: tenantDatabase, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: poolName},
			DatabaseName: tenantDatabase,
		},
	}
}

func bindTenant(instance string) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tenant := &pgelasticv1alpha1.PgTenant{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: tenantDatabase}, tenant); err != nil {
			return err
		}
		tenant.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: instance},
		}
		tenant.Status.Utilization = &pgelasticv1alpha1.PgTenantUtilization{IsCold: ptr.To(true)}
		return k8sClient.Status().Update(suiteCtx, tenant)
	})).To(Succeed())
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
	GinkgoWriter.Printf("\n=== %s migration %s: pauseDurationMillis = %d\n",
		object.Spec.Strategy, object.Name, *object.Status.PauseDurationMillis)
}

var _ = Describe("Moving a tenant between two PostgreSQL 18 instances", Ordered, func() {
	var namespace *corev1.Namespace

	BeforeAll(func() {
		namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, namespace))).To(Succeed())

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

		It("carries the tenant onto the target and flips routing exactly once", func() {
			Expect(k8sClient.Create(suiteCtx,
				makeMigration(migrationName, instanceB, pgelasticv1alpha1.TenantMigrationOnline))).To(Succeed())

			object := awaitPhase(migrationName, pgelasticv1alpha1.TenantMigrationPhaseCompleted)
			Expect(routedInstance()).To(Equal(instanceB))
			reportPause(object)
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
			object := fetchMigration(migrationName)
			var verified *metav1.Condition
			for i := range object.Status.Conditions {
				if object.Status.Conditions[i].Type == migration.ConditionVerified {
					verified = &object.Status.Conditions[i]
				}
			}
			Expect(verified).NotTo(BeNil())
			Expect(verified.Status).To(Equal(metav1.ConditionTrue))
			Expect(verified.Message).To(ContainSubstring("equivalence is not correctness"))
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
