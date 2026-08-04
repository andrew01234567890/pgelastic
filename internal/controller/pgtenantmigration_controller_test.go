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

package controller

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

const (
	migrationNamespace = "migration-test"
	sourceInstance     = "pg-src"
	targetInstance     = "pg-dst"
	migrationTenant    = "acme"
	migrationDatabase  = "acme"
)

// scriptedSQL answers by longest matching fragment and records every statement, which is
// enough to drive the whole phase machine without a database.
type scriptedSQL struct {
	mutex      sync.Mutex
	answers    map[string][]migration.Row
	statements []string
}

func newScriptedSQL() *scriptedSQL {
	sql := &scriptedSQL{answers: map[string][]migration.Row{}}
	maps.Copy(sql.answers, map[string][]migration.Row{
		// Preflight answers for a source that may be moved.
		"AND c.relreplident = 'd'": {},
		// No matview and no unlogged table: both are unpublishable, so an online move of
		// one is refused before anything is provisioned.
		"c.relpersistence = 'u'":                    {},
		"FROM pg_prepared_xacts":                    {},
		"FROM pg_largeobject_metadata":              {{"0"}},
		"SELECT extname FROM pg_extension":          {{"plpgsql"}},
		"FROM pg_database d WHERE d.datname":        {{"UTF8|C|C|b|C.UTF-8|"}},
		"pg_database_size":                          {{"1048576"}},
		"FROM pg_stat_activity WHERE backend_type":  {{"5"}},
		"SHOW wal_level":                            {{"logical"}},
		"SHOW synchronized_standby_slots":           {{"pgelastic_pg_src_2, pgelastic_pg_src_3"}},
		"WHERE slot_type = 'physical'":              {{"pgelastic_pg_src_2"}, {"pgelastic_pg_src_3"}},
		"FROM pg_stat_replication WHERE sync_state": {{"pg-src-2"}, {"pg-src-3"}},
		"SHOW hot_standby_feedback":                 {{"on"}},
		"SHOW sync_replication_slots":               {{"on"}},
		"SHOW primary_slot_name":                    {{"pgelastic_pg_src_1"}},
		"SHOW primary_conninfo":                     {{"host=pg-src-1 dbname=postgres"}},
		// The roles a tenant's database depends on, the attributes they hold, and whether
		// PUBLIC can still connect to it. A source with no roles of its own and no PUBLIC
		// CONNECT is the ordinary case and the one that may be moved.
		"FROM pg_roles r JOIN closure c ON c.oid = r.oid": {},
		"CASE WHEN r.rolsuper THEN 'SUPERUSER' END":       {},
		"e.grantee = 0 AND e.privilege_type = 'CONNECT'":  {{"0"}},
		"FROM pg_auth_members a":                          {},
		// Provisioning, copying, catchup and cutover.
		"FROM pg_namespace n WHERE":                    {{"public"}},
		"FROM pg_database WHERE datname":               {{"0"}},
		"FROM pg_roles WHERE rolname":                  {{"0"}},
		"bool_or(datallowconn)":                        {{"true"}},
		"FROM pg_publication WHERE pubname":            {{"0"}},
		"count(*)::text FROM pg_replication_slots":     {{"0"}},
		"FROM pg_subscription WHERE subname":           {{"0"}},
		"FROM pg_subscription_rel":                     {{"3 3"}},
		"pg_wal_lsn_diff(pg_current_wal_lsn()":         {{"0"}},
		"SELECT pg_current_wal_lsn()":                  {{"0/5000000"}},
		"coalesce(confirmed_flush_lsn, '0/0'::pg_lsn)": {{"1"}},
		"FROM pg_stat_activity\nWHERE datname":         {{"0"}},
		"FROM pg_sequences ORDER BY schemaname":        {},
		"string_agg(entry":                             {{"fingerprint"}},
		"c.relkind IN ('r', 'p') AND c.relispartition": {},
	})
	return sql
}

func (s *scriptedSQL) answer(fragment string, rows ...migration.Row) *scriptedSQL {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.answers[fragment] = rows
	return s
}

func (s *scriptedSQL) Exec(_ context.Context, _ migration.Endpoint, statement string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.statements = append(s.statements, statement)
	return nil
}

func (s *scriptedSQL) Query(_ context.Context, _ migration.Endpoint, statement string) ([]migration.Row, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.statements = append(s.statements, statement)
	best, found := "", false
	for fragment := range s.answers {
		if strings.Contains(statement, fragment) && len(fragment) > len(best) {
			best, found = fragment, true
		}
	}
	if !found {
		return nil, fmt.Errorf("no scripted answer for %q", statement)
	}
	return s.answers[best], nil
}

func (s *scriptedSQL) ran(fragment string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, statement := range s.statements {
		if strings.Contains(statement, fragment) {
			return true
		}
	}
	return false
}

type scriptedShell struct{}

func (scriptedShell) Run(_ context.Context, _ migration.Endpoint, _ []string) ([]byte, error) {
	return nil, nil
}

func makeMigrationInstance(name string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: migrationNamespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: "migration-pool"},
			Class:   instanceClassName,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      *quantity("100Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: *quantity("20Gi")},
			},
		},
	}
}

func migrationInstanceStatus(name string) pgelasticv1alpha1.PgInstanceStatus {
	return pgelasticv1alpha1.PgInstanceStatus{
		CurrentPrimary: name + "-1",
		Instances: []pgelasticv1alpha1.InstanceMemberStatus{
			{Name: name + "-1", Role: pgelasticv1alpha1.InstanceRolePrimary},
			{Name: name + "-2", Role: pgelasticv1alpha1.InstanceRoleReplica},
			{Name: name + "-3", Role: pgelasticv1alpha1.InstanceRoleReplica},
		},
		CollationContract: &pgelasticv1alpha1.CollationContract{
			Encoding: "UTF8", LocaleProvider: "builtin", Locale: "C.UTF-8",
			WALSegmentSize: 16 << 20, DataChecksums: true,
		},
		Storage: &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: quantity("100Gi"), Used: quantity("1Gi"),
		},
	}
}

var _ = Describe("PgTenantMigration controller", func() {
	var (
		sql        *scriptedSQL
		router     migration.Router
		reconciler *PgTenantMigrationReconciler
		source     *pgelasticv1alpha1.PgInstance
		target     *pgelasticv1alpha1.PgInstance
		tenant     *pgelasticv1alpha1.PgTenant
		secret     *corev1.Secret
		object     *pgelasticv1alpha1.PgTenantMigration
		clock      time.Time
	)

	BeforeEach(func() {
		ensureNamespace(migrationNamespace)
		claimPool(migrationNamespace, "migration-class", "migration-pool")
		clock = time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

		source = makeMigrationInstance(sourceInstance)
		target = makeMigrationInstance(targetInstance)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		Expect(k8sClient.Create(ctx, target)).To(Succeed())
		source.Status = migrationInstanceStatus(sourceInstance)
		target.Status = migrationInstanceStatus(targetInstance)
		Expect(k8sClient.Status().Update(ctx, source)).To(Succeed())
		Expect(k8sClient.Status().Update(ctx, target)).To(Succeed())

		tenant = makeTenant(migrationNamespace, migrationTenant, "migration-pool", migrationDatabase)
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		tenant.Status = pgelasticv1alpha1.PgTenantStatus{
			Binding: &pgelasticv1alpha1.PgTenantBinding{
				InstanceRef: &corev1.LocalObjectReference{Name: sourceInstance},
			},
			Utilization: &pgelasticv1alpha1.PgTenantUtilization{IsCold: ptr.To(true)},
		}
		Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())

		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      provision.CredentialsSecretName(sourceInstance),
				Namespace: migrationNamespace,
			},
			Data: map[string][]byte{provision.SecretKeyReplicationPassword: []byte("placeholder")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		object = &pgelasticv1alpha1.PgTenantMigration{
			ObjectMeta: metav1.ObjectMeta{Name: "move-acme", Namespace: migrationNamespace},
			Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
				TenantRef:         corev1.LocalObjectReference{Name: migrationTenant},
				TargetInstanceRef: corev1.LocalObjectReference{Name: targetInstance},
				Strategy:          pgelasticv1alpha1.TenantMigrationOnline,
			},
		}
		Expect(k8sClient.Create(ctx, object)).To(Succeed())

		sql = newScriptedSQL()
		router = migration.BindingRouter{Client: k8sClient}
		reconciler = &PgTenantMigrationReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			SQL: sql, Shell: scriptedShell{}, Router: router,
			Now: func() time.Time { return clock },
		}
	})

	AfterEach(func() {
		deleteAndAwait(object, tenant, secret, source, target)
	})

	// advance drives reconciles until the phase stops changing, so a spec asserts on where
	// the machine settled rather than on how many reconciles it took to get there.
	advance := func(limit int) *pgelasticv1alpha1.PgTenantMigration {
		GinkgoHelper()
		var current *pgelasticv1alpha1.PgTenantMigration
		previous := pgelasticv1alpha1.TenantMigrationPhase("")
		for range limit {
			_, err := reconciler.Reconcile(ctx, requestFor(object))
			Expect(err).NotTo(HaveOccurred())
			current = refetch(object)
			if current.Status.Phase == previous {
				break
			}
			previous = current.Status.Phase
			clock = clock.Add(100 * time.Millisecond)
		}
		return current
	}

	routedInstance := func() string {
		GinkgoHelper()
		fetched := refetch(tenant)
		if fetched.Status.Binding == nil || fetched.Status.Binding.InstanceRef == nil {
			return ""
		}
		return fetched.Status.Binding.InstanceRef.Name
	}

	It("carries an online migration through every phase and flips routing exactly once", func() {
		final := advance(20)
		Expect(final.Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhaseCompleted))
		Expect(routedInstance()).To(Equal(targetInstance))
		Expect(final.Status.PauseDurationMillis).NotTo(BeNil())
		Expect(*final.Status.PauseDurationMillis).To(BeNumerically(">=", 0))
		Expect(final.Status.RollbackDeadline).NotTo(BeNil())
		Expect(conditionOf(final.Status.Conditions, migration.ConditionSucceeded).Status).
			To(Equal(metav1.ConditionTrue))
	})

	It("records the physical objects it owns so the sweeper can reap them by name", func() {
		final := advance(20)
		Expect(final.Status.ReplicationSlotName).
			To(Equal(migration.SlotName(migrationNamespace, object.Name)))
		Expect(final.Status.PublicationName).
			To(Equal(migration.PublicationName(migrationNamespace, object.Name)))
		Expect(final.Status.SubscriptionName).
			To(Equal(migration.SubscriptionName(migrationNamespace, object.Name)))
	})

	It("resolves the source once so a rewritten binding cannot move it", func() {
		_, err := reconciler.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		Expect(refetch(object).Status.SourceInstanceRef.Name).To(Equal(sourceInstance))

		rewritten := refetch(tenant)
		rewritten.Status.Binding.InstanceRef = &corev1.LocalObjectReference{Name: targetInstance}
		Expect(k8sClient.Status().Update(ctx, rewritten)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		Expect(refetch(object).Status.SourceInstanceRef.Name).To(Equal(sourceInstance))
	})

	// The condition has to name the offending relations and the remedy. A refusal an
	// operator cannot act on is indistinguishable from a silent degrade.
	It("refuses a tenant lacking replica identity with a self-explaining condition", func() {
		sql.answer("AND c.relreplident = 'd'", migration.Row{"public.events"})
		final := advance(6)
		Expect(final.Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhasePreflight))
		Expect(routedInstance()).To(Equal(sourceInstance))

		condition := conditionOf(final.Status.Conditions, migration.ConditionPreflightPassed)
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(migration.ReasonPreflightRefused))
		Expect(condition.Message).To(ContainSubstring("public.events"))
		Expect(condition.Message).To(ContainSubstring("REPLICA IDENTITY FULL"))
		Expect(condition.Message).To(ContainSubstring("silently drop"))
	})

	It("leaves the tenant on the source when aborted at any phase boundary", func() {
		for _, stopAt := range []pgelasticv1alpha1.TenantMigrationPhase{
			pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
			pgelasticv1alpha1.TenantMigrationPhasePreWarm,
			pgelasticv1alpha1.TenantMigrationPhaseCopying,
			pgelasticv1alpha1.TenantMigrationPhaseCatchup,
			pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
			pgelasticv1alpha1.TenantMigrationPhaseCutover,
		} {
			By("aborting in " + string(stopAt))
			fresh := &pgelasticv1alpha1.PgTenantMigration{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "abort-in-" + strings.ToLower(string(stopAt)),
					Namespace: migrationNamespace,
				},
				Spec: object.Spec,
			}
			Expect(k8sClient.Create(ctx, fresh)).To(Succeed())

			for range 12 {
				_, err := reconciler.Reconcile(ctx, requestFor(fresh))
				Expect(err).NotTo(HaveOccurred())
				if refetch(fresh).Status.Phase == stopAt {
					break
				}
			}
			Expect(refetch(fresh).Status.Phase).To(Equal(stopAt))

			current := refetch(fresh)
			current.Annotations = map[string]string{AnnotationAbort: "operator stopped it"}
			Expect(k8sClient.Update(ctx, current)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, requestFor(fresh))
			Expect(err).NotTo(HaveOccurred())

			aborted := refetch(fresh)
			Expect(aborted.Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhaseAborted))
			Expect(routedInstance()).To(Equal(sourceInstance),
				"aborting in %s left the tenant somewhere other than the source", stopAt)
			Expect(refetch(tenant).Annotations).NotTo(HaveKey(migration.AnnotationQuiescedBy),
				"aborting in %s left the tenant's clients queued", stopAt)

			deleteAndAwait(fresh)
		}
	})

	It("rolls a completed migration back to the source inside its window", func() {
		Expect(advance(20).Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhaseCompleted))
		Expect(routedInstance()).To(Equal(targetInstance))

		current := refetch(object)
		current.Annotations = map[string]string{AnnotationRollback: "regression on the target"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		Expect(refetch(object).Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhaseRolledBack))
		Expect(routedInstance()).To(Equal(sourceInstance))
		Expect(sql.ran("ALLOW_CONNECTIONS true")).To(BeTrue(),
			"a rollback that leaves the source refusing connections is not a rollback")
	})

	It("drops the source only once the rollback window has closed", func() {
		Expect(advance(20).Status.Phase).To(Equal(pgelasticv1alpha1.TenantMigrationPhaseCompleted))
		Expect(sql.ran("DROP DATABASE")).To(BeFalse())

		clock = refetch(object).Status.RollbackDeadline.Add(time.Second)
		_, err := reconciler.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		Expect(sql.ran("DROP DATABASE")).To(BeTrue())
	})

	It("reports what the verifier cannot prove alongside its verdict", func() {
		final := advance(20)
		condition := conditionOf(final.Status.Conditions, migration.ConditionVerified)
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		Expect(condition.Message).To(ContainSubstring("equivalence is not correctness"))
		Expect(final.Status.Verification).NotTo(BeNil())
		Expect(final.Status.Verification.VerifiedAt).NotTo(BeNil())
	})

	// A deployed operator reads through the manager's informer cache, which lags its own last
	// status write by however long the watch event takes to arrive. Driving reconciles with
	// no cache sync between them is that lag made deterministic: the second reconcile computes
	// a status against a revision the first one has already replaced. Losing that race is not
	// a harmless retry - the effect runs before the status is published, so every lost race
	// re-runs the whole phase.
	It("does not lose a status race with its own previous write", func() {
		awaitCached(object, tenant, source, target, secret)
		cached := &PgTenantMigrationReconciler{
			Client: cachedClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			SQL: sql, Shell: scriptedShell{}, Router: router,
			Now: func() time.Time { return clock },
		}

		for range 4 {
			_, err := cached.Reconcile(ctx, requestFor(object))
			Expect(err).NotTo(HaveOccurred())
			clock = clock.Add(100 * time.Millisecond)
		}
		Expect(refetch(object).Status.Phase).
			To(Equal(pgelasticv1alpha1.TenantMigrationPhaseCatchup))
	})

	It("refuses to run without the ports a migration acts through", func() {
		portless := &PgTenantMigrationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := portless.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		condition := conditionOf(refetch(object).Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(migration.ReasonUnresolved))
	})
})
