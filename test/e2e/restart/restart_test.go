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

package restart

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
	"github.com/andrew01234567890/pgelastic/test/e2e/certcheck"
)

const (
	e2eNamespace = "pgelastic-e2e-restart"
	// The instance being rolled, and the neighbour that is never touched. Short-named so
	// every generated member name stays well inside the identifier limits.
	instanceA = "rs-a"
	instanceB = "rs-b"
	poolName  = "rs-pool"
	// sizingClass is the development tier: six postmasters have to fit on one node.
	sizingClass = "dev-1"
	// tenantDatabase lives on the instance that is rolled; neighbourDatabase lives on the
	// other one and is the control. Without it, a stall everybody saw would be
	// indistinguishable from one only the rolled instance's clients saw.
	tenantDatabase    = "acme"
	neighbourDatabase = "neighbour"
	className         = "rs-class"
	workloadClassName = "rs-standard"
	// The agent's reserved band is base + concurrent dumps, capped: raising the dumps from
	// one to six moves max_connections by five, which is a PGC_POSTMASTER change made
	// through a field that is not immutable and is the number the capacity model rests on.
	concurrentDumpsBefore int32 = 1
	concurrentDumpsAfter  int32 = 6
	// maxConnectionsIncrease is what those two produce, and is asserted against SHOW
	// max_connections on each member rather than against the operator's own derivation.
	maxConnectionsIncrease = 5
	// proxyReplicas is two because the gate is per-replica in-memory state and kube-proxy
	// pins a connection to one endpoint for its life: a roll that held only the replica the
	// operator happened to reach first would still be green with one.
	proxyReplicas = 2
)

const (
	// probeInterval paces the clients held across the roll. Short enough that a sub-second
	// pause is still sampled several times on either side of itself.
	probeInterval = 20 * time.Millisecond
	// baselineWindow is how long the probes run before the roll is triggered, which is what
	// "during the roll" is compared against.
	baselineWindow = 5 * time.Second
	// sampleInterval is how often every member is asked what it is. It is short relative to
	// a member restart and long relative to one exec, so the ordering it records is real
	// without the sampling itself becoming the load.
	sampleInterval = 2 * time.Second
	// fleetSample is how often the proxy fleet is looked at during a roll. A replacement
	// under maxSurge zero takes seconds, so this is several looks at every one of them - and
	// a rewrite of the fleet's configuration is caught even if the fleet settles back onto
	// the shape it started with.
	fleetSample = 500 * time.Millisecond
	// neighbourDisturbanceCeiling is the worst statement the untouched tenant may see: far
	// above any normal statement, far below the pause the rolled instance's clients show.
	neighbourDisturbanceCeiling = 500 * time.Millisecond
)

func makeInstance(name string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: poolName},
			Class:   sizingClass,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      resource.MustParse("2Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("512Mi")},
			},
			// The instance starts with room to raise max_connections. Concurrent dumps are
			// charged to the agent's reserved band, and that band is capped, so an instance
			// created at the default has almost none of it left to move.
			PerTenantLogicalBackup: &pgelasticv1alpha1.PerTenantLogicalBackup{
				MaxConcurrentDumps: ptr.To(concurrentDumpsBefore),
			},
		},
	}
}

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

func endpoint(instance, database string) migration.Endpoint {
	return migration.Endpoint{Namespace: e2eNamespace, Instance: instance, Database: database}
}

func exec(instance, database, statement string) {
	GinkgoHelper()
	Expect(sql.Exec(suiteCtx, endpoint(instance, database), statement)).
		To(Succeed(), "statement on %s/%s: %s", instance, database, statement)
}

// ask runs one query inside one named member's own container, so the answer is the
// postmaster's rather than the operator's account of it.
func ask(member, statement string) string {
	GinkgoHelper()
	rows, err := memberSQL(member).Query(suiteCtx, endpoint("", "postgres"), statement)
	Expect(err).NotTo(HaveOccurred(), "query on %s: %s", member, statement)
	Expect(rows).NotTo(BeEmpty(), "%s answered nothing to %s", member, statement)
	return strings.TrimSpace(rows[0][0])
}

func maxConnectionsOn(member string) int {
	GinkgoHelper()
	value, err := strconv.Atoi(ask(member, "SHOW max_connections"))
	Expect(err).NotTo(HaveOccurred())
	return value
}

func instanceOf(name string) *pgelasticv1alpha1.PgInstance {
	GinkgoHelper()
	instance := &pgelasticv1alpha1.PgInstance{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance)).To(Succeed())
	return instance
}

func membersOf(name string) []string {
	return []string{
		provision.MemberName(name, 1),
		provision.MemberName(name, 2),
		provision.MemberName(name, 3),
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
		g.Expect(instance.Status.QuorumEvidence).NotTo(BeNil())
		g.Expect(instance.Status.QuorumEvidence.VotingMembers).To(HaveLen(2))
	}).Should(Succeed(), "%s never became ready", name)
}

// awaitRollComplete waits for the operator to say it has finished, and for the instance to
// be back at full redundancy with a quorum. It is a precondition of the assertions rather
// than one of them: everything the specs actually claim is read out of PostgreSQL.
func awaitRollComplete(name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		instance := instanceOf(name)
		if roll := instance.Status.Roll; roll != nil {
			g.Expect(roll.Step).NotTo(Equal(pgelasticv1alpha1.RollStepBlocked),
				"the roll stopped on %s: %s", roll.Member, roll.Message)
		}
		g.Expect(instance.Status.Roll).To(BeNil(), "a roll is still in progress")
		g.Expect(instance.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
		g.Expect(instance.Annotations).NotTo(HaveKey(ha.AnnotationMaintenance))
	}).Should(Succeed(), "%s never finished rolling", name)
}

func awaitFleet(replicas int32) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
		}, deployment)).To(Succeed())
		g.Expect(deployment.Status.ObservedGeneration).To(Equal(deployment.Generation))
		g.Expect(deployment.Status.UpdatedReplicas).To(Equal(replicas))
		g.Expect(deployment.Status.ReadyReplicas).To(Equal(replicas))
	}, "10m", "5s").Should(Succeed())

	certcheck.AwaitControlClientSecret(suiteCtx, k8sClient, e2eNamespace, poolName)
}

func makeTenant(name string) *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: poolName},
			DatabaseName: name,
		},
	}
}

// bindTenant publishes the binding by hand. The tenant controller is deliberately not
// running here: what it would do is place tenants, and a suite that asserts "acme is on
// rs-a" has to be the thing that decided it.
func bindTenant(name, instance string) {
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

func seedTenant(instance, database string) {
	GinkgoHelper()
	exec(instance, "postgres", fmt.Sprintf(
		`CREATE DATABASE %s TEMPLATE template0`, database))
}

// raiseMaxConnections changes a parameter PostgreSQL can only adopt at startup. Concurrent
// dumps are charged to the agent's own reserved band, which is a term of max_connections,
// so raising them is a real PGC_POSTMASTER change made through a field that is not
// immutable - and it moves the number the whole capacity model rests on.
func raiseMaxConnections(name string, dumps int32) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		instance := &pgelasticv1alpha1.PgInstance{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance); err != nil {
			return err
		}
		instance.Spec.PerTenantLogicalBackup = &pgelasticv1alpha1.PerTenantLogicalBackup{
			MaxConcurrentDumps: ptr.To(dumps),
		}
		return k8sClient.Update(suiteCtx, instance)
	})).To(Succeed())
}

func requestRestart(name, value string) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		instance := &pgelasticv1alpha1.PgInstance{}
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: name}, instance); err != nil {
			return err
		}
		if instance.Annotations == nil {
			instance.Annotations = map[string]string{}
		}
		instance.Annotations[pgelasticv1alpha1.AnnotationRestartedAt] = value
		return k8sClient.Update(suiteCtx, instance)
	})).To(Succeed())
}

// sample is one instant's answer from one member, as PostgreSQL and the kubelet gave it.
type sample struct {
	at         time.Time
	reachable  bool
	inRecovery bool
	podUID     types.UID
	podReady   bool
}

// watcher records what every member of one instance was, continuously, for the whole of a
// roll.
//
// It is the only way the two ordering claims can be made at all. "The replicas restarted
// before the primary" and "the role moved before the primary's Pod was recreated" are
// statements about instants, and an instance inspected only at the end looks identical
// whichever order it happened in. Every answer is the member's own - pg_is_in_recovery()
// out of its own postmaster - because the operator's account of who is primary is exactly
// what is being checked.
type watcher struct {
	mu      sync.Mutex
	samples map[string][]sample
	stop    chan struct{}
	done    chan struct{}
}

func watch(instance string) *watcher {
	w := &watcher{
		samples: map[string][]sample{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	members := membersOf(instance)
	go func() {
		defer close(w.done)
		for {
			w.round(members)
			select {
			case <-w.stop:
				return
			case <-time.After(sampleInterval):
			}
		}
	}()
	return w
}

func (w *watcher) round(members []string) {
	var wait sync.WaitGroup
	for _, member := range members {
		wait.Go(func() { w.sampleOne(member) })
	}
	wait.Wait()
}

func (w *watcher) sampleOne(member string) {
	taken := sample{at: time.Now()}
	pod := &corev1.Pod{}
	if err := k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: member}, pod); err == nil {
		taken.podUID = pod.UID
		taken.podReady = podReady(pod)
	}
	if report, err := (execProber{runner: runner}).Probe(suiteCtx, member, ""); err == nil {
		taken.reachable = report.Healthy
		taken.inRecovery = report.InRecovery
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples[member] = append(w.samples[member], taken)
}

func (w *watcher) close() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// recreatedAt is when a member was first seen running under a Pod other than the one it
// started under, which is the instant its restart began.
func (w *watcher) recreatedAt(member string) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	taken := w.samples[member]
	if len(taken) == 0 || taken[0].podUID == "" {
		return time.Time{}, false
	}
	original := taken[0].podUID
	for _, one := range taken {
		if one.podUID != "" && one.podUID != original {
			return one.at, true
		}
	}
	return time.Time{}, false
}

// tookTheRoleAt is when a member other than the one named was first seen reporting itself
// out of recovery, which is PostgreSQL's own account of the promotion.
func (w *watcher) tookTheRoleAt(notThisOne string) (string, time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	best := time.Time{}
	winner := ""
	for member, taken := range w.samples {
		if member == notThisOne {
			continue
		}
		for _, one := range taken {
			if one.reachable && !one.inRecovery {
				if best.IsZero() || one.at.Before(best) {
					best, winner = one.at, member
				}
				break
			}
		}
	}
	return winner, best, winner != ""
}

// worstRedundancy is the fewest members ever seen answering at one instant.
//
// Under dataDurability Required with quorum ANY 1, two members answering is a primary and
// one standby, which is a quorum satisfied and commits flowing. One is a primary with
// nothing to wait on, and every commit on the instance stalls. So the whole "no client
// error" claim rests on this never dropping below two.
func (w *watcher) worstRedundancy() (int, time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	byRound := map[int]int{}
	rounds := 0
	when := map[int]time.Time{}
	for _, taken := range w.samples {
		rounds = max(rounds, len(taken))
	}
	for round := range rounds {
		for _, taken := range w.samples {
			if round < len(taken) {
				when[round] = taken[round].at
				if taken[round].reachable {
					byRound[round]++
				}
			}
		}
	}
	worst, at := 3, time.Time{}
	for round := range rounds {
		if byRound[round] < worst {
			worst, at = byRound[round], when[round]
		}
	}
	return worst, at
}

// rollOnce is the whole verification, run twice with two different triggers.
//
// Everything that is asserted is read from PostgreSQL or from the client's own socket. The
// operator's status is consulted for exactly one thing - knowing when to stop waiting - and
// for nothing that is claimed.
func rollOnce(what string, trigger func(), expectedIncrease int) {
	GinkgoHelper()
	members := membersOf(instanceA)
	before := map[string]int{}
	for _, member := range members {
		before[member] = maxConnectionsOn(member)
	}
	primaryBefore := instanceOf(instanceA).Status.CurrentPrimary
	Expect(primaryBefore).NotTo(BeEmpty())
	Expect(ask(primaryBefore, "SELECT pg_is_in_recovery()")).To(Equal("f"),
		"the member the operator calls primary has to be the one PostgreSQL calls primary")

	// On different replicas, deliberately. Both tenants forwarded to the same Pod makes the
	// two probes one observation: that Pod going away ends both at the same moment, and the
	// report then says every tenant in the pool was dropped whether the fleet moved or a
	// single replica was replaced. On two replicas the pair separates those.
	held := startProbe("the rolled instance's tenant", provision.OpsRole,
		tenantDatabase, 0, probeInterval)
	defer held.stop()
	neighbour := startProbe("the neighbour on the other instance", provision.OpsRole,
		neighbourDatabase, 1, probeInterval)
	defer neighbour.stop()

	time.Sleep(baselineWindow)
	held.mark()
	neighbour.mark()

	observer := watch(instanceA)
	defer observer.close()
	// Sampled throughout rather than compared at the ends. A fleet that rolled and settled
	// back onto the same ReplicaSet reads as unchanged from the ends alone, and every client
	// it was carrying is still gone.
	fleet := watchFleet(fleetSample)
	defer fleet.close()

	trigger()
	// Waiting for every member to have been recreated, rather than for the operator to say
	// it has finished. The operator's record of a roll is absent both before it starts and
	// after it ends, so waiting on that alone passes instantly against a roll that has not
	// begun - which is exactly the false green this spec exists to avoid.
	Eventually(func(g Gomega) {
		for _, member := range members {
			_, recreated := observer.recreatedAt(member)
			g.Expect(recreated).To(BeTrue(), "%s has not been rolled yet", member)
		}
	}, "30m", "5s").Should(Succeed())
	awaitRollComplete(instanceA)
	observer.close()

	held.stop()
	neighbour.stop()
	heldReport := held.report()
	neighbourReport := neighbour.report()
	AddReportEntry(what+": client through the proxy", heldReport.String())
	AddReportEntry(what+": neighbour through the proxy", neighbourReport.String())
	GinkgoWriter.Printf("\n=== %s\n=== %s\n=== %s\n", what, heldReport, neighbourReport)

	By("every member came back on the new configuration, read from its own postmaster")
	for _, member := range members {
		Expect(maxConnectionsOn(member)).To(Equal(before[member]+expectedIncrease),
			"%s is still running the configuration it started with", member)
	}

	By("the instance is serving again with a quorum")
	awaitReady(instanceA)
	// Streaming is a later fact than Ready. The member the roll finished on has to reconnect
	// its WAL receiver and be seen in the primary's pg_stat_replication, which is a second or
	// two after the instance is serving again - so this waits rather than samples.
	Eventually(func(g Gomega) {
		evidence := instanceOf(instanceA).Status.QuorumEvidence
		g.Expect(evidence).NotTo(BeNil())
		g.Expect(evidence.StreamingMembers).To(HaveLen(2),
			"only %v are streaming, so the instance is still one member short of the "+
				"redundancy it started with", evidence.StreamingMembers)
	}, "5m", "5s").Should(Succeed())

	// Asserted before the clients are, because a replica that goes away takes every socket
	// held through it with it, and at the client that is indistinguishable from the drop the
	// next assertion is looking for. Rolling an instance is not allowed to touch the fleet
	// in front of it, so naming that first means the report accuses the cause rather than
	// the symptom.
	//
	// The configuration is asserted before the Pods, and separately, because it is the cause
	// and they are the consequence. The proxy Deployment's Pod template carries the
	// structural half of the rendered document as an annotation, so anything an instance roll
	// publishes into that half rewrites the template and the rollout that follows drops every
	// client on the pool - on every tenant of every instance, including the ones the roll
	// never touched. A changed hash names the value that moved; a changed Pod set only says
	// somebody died.
	By("the proxy fleet in front of the instance was never replaced")
	shapes := fleet.close()
	hashes := make([]string, 0, len(shapes))
	for _, shape := range shapes {
		if !slices.Contains(hashes, shape.configHash) {
			hashes = append(hashes, shape.configHash)
		}
	}
	Expect(hashes).To(HaveLen(1),
		"rolling the instance rewrote the proxy fleet's structural configuration %d times, "+
			"and every rewrite replaces every replica and drops every client on the pool. "+
			"The hashes seen were %v, in this sequence:\n%s",
		len(hashes)-1, hashes, journalOf(shapes))
	Expect(shapes).To(HaveLen(1),
		"the replicas serving the pool changed while the instance rolled, without the "+
			"configuration having moved - so this is the fleet being replaced for some other "+
			"reason, or an eviction this suite did not cause:\n%s",
		journalOf(shapes))

	By("no client on the rolled instance saw an error")
	Expect(heldReport.failures).To(BeEmpty(),
		"a client was dropped rather than queued:\n%s\nthe port-forward it held said:\n%s",
		heldReport.failureSummary(), heldReport.forwardLog)
	Expect(heldReport.duringCount).To(BeNumerically(">", 0),
		"no statement was issued during the roll, so nothing was measured")

	By("quorum was never lost")
	worst, at := observer.worstRedundancy()
	Expect(worst).To(BeNumerically(">=", 2),
		"only %d members were answering at %s; under dataDurability Required that is every "+
			"commit on the instance stalling", worst, at)

	By("the replicas restarted before the primary, and the role moved before it did")
	primaryRecreated, ok := observer.recreatedAt(primaryBefore)
	Expect(ok).To(BeTrue(), "%s was never recreated, so it was never rolled", primaryBefore)
	for _, member := range members {
		if member == primaryBefore {
			continue
		}
		recreated, ok := observer.recreatedAt(member)
		Expect(ok).To(BeTrue(), "%s was never recreated, so it was never rolled", member)
		Expect(recreated).To(BeTemporally("<", primaryRecreated),
			"%s was restarted after the primary; the member holding the role has to go last",
			member)
	}

	successor, promoted, ok := observer.tookTheRoleAt(primaryBefore)
	Expect(ok).To(BeTrue(), "no other member ever reported itself out of recovery, so the "+
		"primary was restarted underneath its clients rather than switched away")
	Expect(promoted).To(BeTemporally("<", primaryRecreated),
		"%s took the role at %s, after %s had already been recreated at %s: the primary was "+
			"restarted first and handed over afterwards", successor, promoted, primaryBefore,
		primaryRecreated)
	Expect(ask(primaryBefore, "SELECT pg_is_in_recovery()")).To(Equal("t"),
		"the old primary came back as a standby")

	By("the neighbour instance was untouched")
	Expect(neighbourReport.failures).To(BeEmpty(),
		"a tenant on the other instance saw an error:\n%s\nthe port-forward it held said:\n%s",
		neighbourReport.failureSummary(), neighbourReport.forwardLog)
	Expect(neighbourReport.duringMax).To(BeNumerically("<", neighbourDisturbanceCeiling),
		"the neighbour's worst statement was %s, so this was a fleet-wide stall rather than "+
			"one instance being rolled", neighbourReport.duringMax)
}

var _ = Describe("Rolling a PostgreSQL 18 instance under load", Ordered, Label("restart"), func() {
	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}))).To(Succeed())
		for _, object := range poolObjects() {
			Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, object))).To(Succeed())
		}
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeInstance(instanceA)))).To(Succeed())
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeInstance(instanceB)))).To(Succeed())

		awaitReady(instanceA)
		awaitReady(instanceB)

		seedTenant(instanceA, tenantDatabase)
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeTenant(tenantDatabase)))).To(Succeed())
		bindTenant(tenantDatabase, instanceA)

		seedTenant(instanceB, neighbourDatabase)
		Expect(client.IgnoreAlreadyExists(
			k8sClient.Create(suiteCtx, makeTenant(neighbourDatabase)))).To(Succeed())
		bindTenant(neighbourDatabase, instanceB)

		awaitFleet(proxyReplicas)
	})

	// The coexistence spec. It runs before the rolls because what it measures is the fleet
	// at rest: the failure it exists to catch was permanent, not something a roll provoked.
	//
	// Two operators that both claim this pool render slightly different proxy Deployments -
	// different proxy image, different structural config - and each rewrites the other's,
	// forever. Watched from outside, that is a Deployment whose generation climbs on its own,
	// two ReplicaSets alternating, and Pods replaced every few seconds against a spec asking
	// for two. Ownership resolution is what makes the deployed operator, which carries the
	// default controllerName, resolve every object here to this suite's name and leave it be.
	//
	// The mutation test this spec is worth anything under: make ownership.Resolver.Of return
	// ownership.Mine unconditionally, redeploy the operator, and this fails with the churn.
	It("keeps the fleet still while another operator runs on the same cluster",
		Label("coexistence"), func() {
			requireDeployedOperator()

			at := observeFleet()
			Expect(at.replicaSets).To(HaveLen(1),
				"the fleet is already being served by more than one ReplicaSet: %s", at)

			deadline := time.Now().Add(coexistenceWindow)
			for time.Now().Before(deadline) {
				time.Sleep(coexistenceSample)
				now := observeFleet()
				Expect(now).To(Equal(at),
					"the proxy fleet changed under a cluster running two operators: was %s, now %s", at, now)
			}
		})

	// The load-bearing spec. A parameter that PostgreSQL can only adopt at startup is
	// raised, and every client holding a socket through the pool's Service across the whole
	// roll is queued rather than dropped.
	//
	// The mutation test this spec is worth anything under: take the quiesce out of the
	// handover - drop the Quiescer from the reconciler in the suite setup, or make
	// rollPrimary skip straight to naming the member - and the held probe records
	// `FATAL: 55000 database ... is not currently accepting connections`, or a refused
	// connection, instead of a latency spike. Without that demonstration this spec proves
	// only that a restart happened.
	It("restarts every member for a max_connections change without dropping a client", func() {
		rollOnce("configuration change", func() {
			raiseMaxConnections(instanceA, concurrentDumpsAfter)
		}, maxConnectionsIncrease)
	})

	// The same claim, reached the way an operator reaches it: the idiom kubectl rollout
	// restart teaches. Nothing about the configuration changes, so max_connections is
	// expected to come back exactly as it was - which is itself the assertion that the
	// members really were restarted rather than left alone, because the Pods were recreated
	// and the ordering was observed.
	It("restarts every member for a restartedAt annotation without dropping a client", func() {
		rollOnce("explicit restart request", func() {
			requestRestart(instanceA, time.Now().UTC().Format(time.RFC3339Nano))
		}, 0)
	})

	// The trap the primary PodDisruptionBudget sets, and the reason this work exists: an
	// eviction of the primary is refused by that budget, and until the roll learned to read
	// the node's own unschedulable flag nothing ever started the switchover it was waiting
	// for, so `kubectl drain` blocked until a human intervened.
	//
	// It needs more than one node, and says so rather than passing vacuously: on a
	// single-node cluster every member is on the node being drained, handing the role over
	// moves it to a member with the same problem, and the roll correctly refuses.
	It("hands the primary's role away when its node is drained", Label("multinode"), func() {
		primary := instanceOf(instanceA).Status.CurrentPrimary
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: e2eNamespace, Name: primary}, pod)).To(Succeed())
		node := pod.Spec.NodeName
		Expect(node).NotTo(BeEmpty())

		nodes := &corev1.NodeList{}
		Expect(k8sClient.List(suiteCtx, nodes)).To(Succeed())
		if len(nodes.Items) < 2 {
			Fail(fmt.Sprintf("this cluster has %d node; the drain trap can only be closed "+
				"where the role has somewhere to go. Run this spec on a multi-node cluster, "+
				"or exclude it with -ginkgo.label-filter='!multinode'", len(nodes.Items)))
		}

		cordonNode(node, true)
		defer cordonNode(node, false)

		Eventually(func(g Gomega) {
			g.Expect(instanceOf(instanceA).Status.CurrentPrimary).NotTo(Equal(primary))
		}, "10m", "5s").Should(Succeed(),
			"the primary's node was cordoned and nothing handed the role away, so an "+
				"eviction of %s is still refused by its PodDisruptionBudget", primary)

		Expect(ask(primary, "SELECT pg_is_in_recovery()")).To(Equal("t"))
		awaitReady(instanceA)
	})
})

func cordonNode(name string, unschedulable bool) {
	GinkgoHelper()
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node := &corev1.Node{}
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, node); err != nil {
			return err
		}
		node.Spec.Unschedulable = unschedulable
		return k8sClient.Update(suiteCtx, node)
	})).To(Succeed())
}
