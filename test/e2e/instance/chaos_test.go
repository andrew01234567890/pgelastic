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

package instance

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/verify"
)

const (
	// chaosNamespace and chaosInstance are separate from the provisioning suite's, so a
	// scenario that leaves an instance in a bad state cannot make the other suite lie.
	chaosNamespace = "pgelastic-e2e-chaos"
	chaosInstance  = "pg-chaos"
	// chaosReplicas is the only topology the quorum gate is designed around.
	chaosReplicas = 3
	// oracleRole and oracleDatabase are created for the durability oracle alone. The role
	// exists because pgelastic_ops deliberately cannot create anything, and the oracle has
	// to own the relation it writes to.
	oracleRole     = "pgelastic_e2e_oracle"
	oracleDatabase = "postgres"
	// oracleSecret carries a password generated per run, so nothing resembling a credential
	// is ever written down in this repository.
	oracleSecret = "pgelastic-e2e-oracle"
	// oracleWriters is deliberately small: the point is a steady stream of acknowledged
	// commits to lose, not throughput.
	oracleWriters = 4
)

func chaosMemberNames() []string {
	names := make([]string, 0, chaosReplicas)
	for serial := int32(1); serial <= chaosReplicas; serial++ {
		names = append(names, provision.MemberName(chaosInstance, serial))
	}
	return names
}

// chaosPsql runs a query on one member over its local Unix socket, as the bootstrap
// superuser. It answers what PostgreSQL says, which is the only thing any of these specs
// are allowed to believe.
func chaosPsql(member, query string) (string, error) {
	command := kubectlCommand("exec", "-n", chaosNamespace, member, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-d", oracleDatabase, "-tAqc", query)
	output, err := command.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func mustChaosQuery(member, query string) string {
	GinkgoHelper()
	output, err := chaosPsql(member, query)
	Expect(err).NotTo(HaveOccurred(), "psql on %s failed: %s", member, output)
	return output
}

func chaosCR() *pgelasticv1alpha1.PgInstance {
	GinkgoHelper()
	fetched := &pgelasticv1alpha1.PgInstance{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: chaosNamespace, Name: chaosInstance}, fetched)).To(Succeed())
	return fetched
}

// primariesNow counts the members that answer and say they are out of recovery.
//
// A member that does not answer is counted as unknown rather than as a replica: not knowing
// where a member is, is not evidence that it is behaving.
func primariesNow() (primaries []string, answered int) {
	for _, member := range chaosMemberNames() {
		output, err := chaosPsql(member, "SELECT pg_is_in_recovery()")
		if err != nil {
			continue
		}
		answered++
		if output == "f" {
			primaries = append(primaries, member)
		}
	}
	return primaries, answered
}

// splitBrainWatch samples "how many members simultaneously report themselves out of
// recovery" for the whole of a disruption, not merely after it.
//
// The invariant it enforces is "never more than one". "Exactly one" cannot be required
// while the disruption is running: a correct failover has a window with no primary at all,
// which is the whole point of stopping the old one before starting the new one. Exactly one
// is asserted separately, once the instance has converged.
type splitBrainWatch struct {
	mutex      sync.Mutex
	worst      int
	worstNames []string
	samples    int
	stop       chan struct{}
	done       chan struct{}
}

func watchForSplitBrain() *splitBrainWatch {
	watch := &splitBrainWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(watch.done)
		for {
			select {
			case <-watch.stop:
				return
			default:
			}
			primaries, _ := primariesNow()
			watch.mutex.Lock()
			watch.samples++
			if len(primaries) > watch.worst {
				watch.worst, watch.worstNames = len(primaries), primaries
			}
			watch.mutex.Unlock()
			select {
			case <-watch.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return watch
}

func (w *splitBrainWatch) assertNoSplitBrain() {
	GinkgoHelper()
	close(w.stop)
	<-w.done
	w.mutex.Lock()
	defer w.mutex.Unlock()
	Expect(w.samples).To(BeNumerically(">", 5),
		"the invariant has to be sampled during the disruption, not only after it")
	Expect(w.worst).To(BeNumerically("<=", 1),
		"two members reported pg_is_in_recovery() = false at once: %v", w.worstNames)
}

// readWriteEndpoints counts the ready addresses behind the read-write Service.
//
// It reads EndpointSlices rather than the Service, because a Service exists whether or not
// anything is behind it: an endpoint-less <instance>-rw refuses every connection with the
// same "connection refused" a missing Service would give, and only the slices can tell the
// two apart.
func readWriteEndpoints() (int, error) {
	endpointSlices := &discoveryv1.EndpointSliceList{}
	err := k8sClient.List(suiteCtx, endpointSlices, client.InNamespace(chaosNamespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: provision.PrimaryServiceName(chaosInstance)})
	if err != nil {
		return 0, err
	}
	ready := 0
	for _, slice := range endpointSlices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
				ready += len(endpoint.Addresses)
			}
		}
	}
	return ready, nil
}

// leaseHolder is the member holding the promotion Lease, read straight from the API server.
func leaseHolder() string {
	lease := &coordinationv1.Lease{}
	if err := k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: chaosNamespace, Name: chaosInstance}, lease); err != nil {
		return ""
	}
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// endpointGrace is how long the read-write Service is allowed to be endpoint-less while a
// member is demonstrably serving.
//
// It is not zero because a promotion legitimately has an instant with no endpoint - the old
// primary's label is gone and the new one's has not landed - and a label the operator has
// just written takes a moment to reach the EndpointSlice controller. It is far below the
// half-minute the two-phase sentinel used to freeze the label for, which is the failure
// this exists to catch.
const endpointGrace = 10 * time.Second

// endpointWatch samples the invariant "a member that is out of recovery and holds the
// promotion Lease is a member the read-write Service must be able to reach".
//
// The lease is the independent half. Asserting only on what the member says about itself
// would be asserting the operator's own view back at it; the lease is held by the member's
// agent, so the two together say "this member is serving and is entitled to".
type endpointWatch struct {
	mutex     sync.Mutex
	worst     time.Duration
	worstNote string
	violating time.Time
	samples   int
	stop      chan struct{}
	done      chan struct{}
}

func watchReadWriteEndpoints() *endpointWatch {
	watch := &endpointWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(watch.done)
		for {
			watch.sample()
			select {
			case <-watch.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return watch
}

func (w *endpointWatch) sample() {
	holder := leaseHolder()
	if holder == "" {
		w.clear()
		return
	}
	recovery, err := chaosPsql(holder, "SELECT pg_is_in_recovery()")
	if err != nil || recovery != "f" {
		w.clear()
		return
	}
	ready, err := readWriteEndpoints()
	if err != nil {
		w.clear()
		return
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.samples++
	if ready > 0 {
		w.violating = time.Time{}
		return
	}
	if w.violating.IsZero() {
		w.violating = time.Now()
	}
	if elapsed := time.Since(w.violating); elapsed > w.worst {
		w.worst = elapsed
		w.worstNote = fmt.Sprintf("%s reports pg_is_in_recovery() = false and holds the lease", holder)
	}
}

func (w *endpointWatch) clear() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.violating = time.Time{}
}

func (w *endpointWatch) assertServiceFollowedTheServingMember() {
	GinkgoHelper()
	close(w.stop)
	<-w.done
	w.mutex.Lock()
	defer w.mutex.Unlock()
	Expect(w.samples).To(BeNumerically(">", 5),
		"the invariant has to be sampled while the primary was moving, not only afterwards")
	Expect(w.worst).To(BeNumerically("<", endpointGrace),
		"%s was left with no endpoints for %s: %s",
		provision.PrimaryServiceName(chaosInstance), w.worst.Truncate(time.Second), w.worstNote)
}

// oracleReport is what one chaos scenario proved.
type oracleReport struct {
	Report   verify.Report
	ExitCode int
	Logs     string
}

func randomSecret() string {
	GinkgoHelper()
	buffer := make([]byte, 24)
	_, err := rand.Read(buffer)
	Expect(err).NotTo(HaveOccurred())
	return base64.RawURLEncoding.EncodeToString(buffer)
}

// oraclePod runs the durability oracle inside the cluster, against the read-write Service.
//
// It runs in-cluster deliberately. The Service is re-resolved on every connection, so a
// failover moves the endpoint underneath the oracle exactly as it moves underneath a
// tenant; an oracle driven from outside through a port-forward would be measuring the
// port-forward.
//
// The two phases are one shell script rather than two Pods because the ledger is the
// evidence and it must not move: the writer fleet runs while the instance is being broken,
// and the check then retries until the surviving primary can answer at all. Only the
// operational exit code is retried. A lost commit is reported the first time it is seen.
func oraclePod(scenario string, duration time.Duration) *corev1.Pod {
	table := "chaos_" + scenario
	dsn := fmt.Sprintf("host=%s.%s.svc port=%d user=%s dbname=%s connect_timeout=5",
		provision.PrimaryServiceName(chaosInstance), chaosNamespace,
		provision.PostgresPort, oracleRole, oracleDatabase)

	script := fmt.Sprintf(`
%[1]s run --dsn "$DSN" --duration %[2]ds --writers %[3]d \
  --op-timeout 5s --ledger /tmp/ledger.log --table %[4]s
echo "---WORKLOAD-DONE---"
code=3
attempt=0
while [ $attempt -lt 120 ]; do
  %[1]s check --dsn "$DSN" --ledger /tmp/ledger.log --table %[4]s --json >/tmp/report.json 2>/tmp/error
  code=$?
  if [ $code -ne 3 ]; then break; fi
  attempt=$((attempt+1))
  sleep 5
done
echo "---REPORT---"
cat /tmp/report.json
echo "---END-REPORT---"
cat /tmp/error
exit $code
`, provision.SourceVerifyBinary, int(duration.Seconds()), oracleWriters, table)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oracle-" + scenario,
			Namespace: chaosNamespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "oracle",
				Image:           agentImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"/bin/sh", "-c", script},
				Env: []corev1.EnvVar{
					{Name: "DSN", Value: dsn},
					{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: oracleSecret},
							Key:                  "password",
						},
					}},
				},
			}},
		},
	}
}

// keepDown deletes a member until the operator has named somebody else, then stops.
//
// It stops on the decision rather than after a fixed window because the member has work to
// do the moment the decision lands: it has to come back and rewind onto the new primary,
// and deleting it in the middle of that turns a rewind into a re-clone for no reason.
func keepDown(member string, window time.Duration) {
	GinkgoHelper()
	deadline := time.Now().Add(window)
	first := true
	for time.Now().Before(deadline) {
		output, err := kubectlCommand("delete", "pod", "-n", chaosNamespace,
			member, "--grace-period=0", "--force", "--ignore-not-found").CombinedOutput()
		if first {
			Expect(err).NotTo(HaveOccurred(), string(output))
			first = false
		}
		target := chaosCR().Status.TargetPrimary
		if target != "" && target != pgelasticv1alpha1.TargetPrimaryPending && target != member {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// losePrimary takes the named primary away for longer than the failover delay and waits
// until somebody else holds the role.
func losePrimary(primary string) {
	GinkgoHelper()
	keepDown(primary, 75*time.Second)
	Eventually(func(g Gomega) {
		g.Expect(chaosCR().Status.CurrentPrimary).NotTo(Equal(primary),
			"the instance has to elect somebody else")
	}, "10m", "5s").Should(Succeed())
}

// memberTimeline is the timeline a member holds WAL for, which for a standby is the one its
// WAL receiver is streaming rather than the one its last replayed checkpoint was on.
func memberTimeline(member string) int {
	GinkgoHelper()
	output := mustChaosQuery(member, `SELECT GREATEST(
		(SELECT timeline_id FROM pg_control_checkpoint()),
		COALESCE((SELECT received_tli FROM pg_stat_wal_receiver), 0))`)
	timeline, err := strconv.Atoi(output)
	Expect(err).NotTo(HaveOccurred(), output)
	return timeline
}

// heldDown remembers the nodes to make schedulable again.
type heldDown struct {
	cordoned []string
	once     sync.Once
}

// holdMembersDown takes the named members away and keeps them away until release.
//
// Deleting them is not enough on its own. The operator recreates a deleted member at once,
// and on a single-node cluster with the image cached and the data volume already bound it
// is back and acknowledging commits within a few seconds - so a spec that asserts on their
// absence for any useful window would be measuring Pod startup time rather than the
// property it names. Cordoning first leaves every recreated Pod Pending, which is what a
// node loss looks like from the instance's side and is exactly reversible. Every node is
// cordoned, not merely the one the member was on, because the replacement Pod is free to
// be scheduled anywhere.
func holdMembersDown(members []string) *heldDown {
	GinkgoHelper()
	nodes := &corev1.NodeList{}
	Expect(k8sClient.List(suiteCtx, nodes)).To(Succeed())

	held := &heldDown{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		patched := node.DeepCopy()
		patched.Spec.Unschedulable = true
		Expect(k8sClient.Update(suiteCtx, patched)).To(Succeed())
		held.cordoned = append(held.cordoned, node.Name)
	}
	for _, member := range members {
		output, err := kubectlCommand("delete", "pod", "-n", chaosNamespace, member,
			"--grace-period=0", "--force", "--ignore-not-found").CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(output))
	}
	return held
}

// release makes the nodes schedulable again. It reports rather than asserts, because it
// runs from a defer that a failing spec has already entered.
func (h *heldDown) release() {
	h.once.Do(func() {
		for _, name := range h.cordoned {
			node := &corev1.Node{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, node); err != nil {
				AddReportEntry("could not re-read node "+name, err.Error())
				continue
			}
			node.Spec.Unschedulable = false
			if err := k8sClient.Update(suiteCtx, node); err != nil {
				AddReportEntry("could not uncordon node "+name, err.Error())
			}
		}
	})
}

func podLogs(name string) string {
	output, err := kubectlCommand("logs", "-n", chaosNamespace, name).CombinedOutput()
	if err != nil {
		return string(output)
	}
	return string(output)
}

// runScenario drives one chaos scenario end to end: start the oracle, break something while
// it writes, wait for the instance to converge, and read the oracle's verdict.
func runScenario(scenario string, duration time.Duration, disrupt func()) oracleReport {
	GinkgoHelper()
	pod := oraclePod(scenario, duration)
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, pod))).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

	By("waiting for the oracle to start writing")
	Eventually(func(g Gomega) {
		fetched := &corev1.Pod{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod), fetched)).To(Succeed())
		g.Expect(fetched.Status.Phase).To(BeElementOf(corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed))
	}, "3m", "2s").Should(Succeed())
	// A short head start so the ledger holds acknowledged commits from before the
	// disruption, which are the ones a lost-commit bug would take.
	time.Sleep(10 * time.Second)

	watch := watchForSplitBrain()
	disrupt()

	By("waiting for the oracle to reach a verdict")
	var finished *corev1.Pod
	Eventually(func(g Gomega) {
		fetched := &corev1.Pod{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod), fetched)).To(Succeed())
		g.Expect(fetched.Status.Phase).To(BeElementOf(corev1.PodSucceeded, corev1.PodFailed),
			"the oracle is still %s", fetched.Status.Phase)
		finished = fetched
	}, "20m", "5s").Should(Succeed())

	watch.assertNoSplitBrain()

	logs := podLogs(pod.Name)
	result := oracleReport{Logs: logs, ExitCode: exitCodeOf(finished)}
	if body := between(logs, "---REPORT---", "---END-REPORT---"); body != "" {
		Expect(json.Unmarshal([]byte(body), &result.Report)).To(Succeed(), "report body was %q", body)
	}
	return result
}

func exitCodeOf(pod *corev1.Pod) int {
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil {
			return int(status.State.Terminated.ExitCode)
		}
	}
	return -1
}

func between(text, start, end string) string {
	from := strings.Index(text, start)
	to := strings.Index(text, end)
	if from < 0 || to < from {
		return ""
	}
	return strings.TrimSpace(text[from+len(start) : to])
}

// assertOraclePassed is the assertion every scenario shares: no acknowledged commit was
// lost, and nothing appeared that was never attempted.
func assertOraclePassed(scenario string, result oracleReport) {
	GinkgoHelper()
	Expect(result.Report.DurabilityViolation).To(BeFalse(),
		"%s lost acknowledged commits %v\n%s", scenario, result.Report.LostCommitted, result.Logs)
	Expect(result.Report.UnexpectedWrites).To(BeFalse(),
		"%s produced writes nobody attempted %v\n%s", scenario, result.Report.Unexpected, result.Logs)
	Expect(result.Report.Verdict).To(Equal(verify.VerdictPass), result.Logs)
	Expect(result.ExitCode).To(Equal(verify.ExitPass), result.Logs)
	Expect(result.Report.Counts.Committed).To(BeNumerically(">", 0),
		"a scenario that committed nothing proves nothing\n%s", result.Logs)
	AddReportEntry(scenario+" oracle", result.Report)
}

// quorumMembersQuery names the standbys PostgreSQL itself reports as streaming quorum
// members, which is a stronger statement than counting rows: a member that is merely
// connected is not redundancy. Under "ANY 1" an instance whose second standby is streaming
// but not counted is still one failure away from stalling every commit.
const quorumMembersQuery = `SELECT COALESCE(string_agg(application_name, ',' ORDER BY application_name), '')
	FROM pg_stat_replication
	WHERE state = 'streaming' AND sync_state IN ('sync', 'quorum')`

// awaitConverged waits until the instance is whole again: three members, one primary, both
// standbys streaming as quorum members, and every member on the same timeline.
func awaitConverged() {
	GinkgoHelper()
	By("waiting for the instance to converge")
	Eventually(func(g Gomega) {
		status := chaosCR().Status
		g.Expect(status.CurrentPrimary).NotTo(BeEmpty())
		g.Expect(status.TargetPrimary).To(Equal(status.CurrentPrimary),
			"targetPrimary != currentPrimary means a failover is still in progress")

		primary := status.CurrentPrimary
		quorum, err := chaosPsql(primary, quorumMembersQuery)
		g.Expect(err).NotTo(HaveOccurred(), quorum)
		g.Expect(quorum).To(Equal(strings.Join(standbysOf(primary), ",")),
			"both standbys must be streaming and counted towards the quorum, not merely present")

		inSyncSet := map[string]bool{}
		for _, member := range status.Instances {
			inSyncSet[member.Name] = member.InSyncSet
		}
		for _, standby := range standbysOf(primary) {
			g.Expect(inSyncSet).To(HaveKeyWithValue(standby, true),
				"the instance has to record %s as a voter, or the failover gate cannot count it", standby)
		}
		g.Expect(status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady),
			"a member still rebuilding itself leaves the instance short of redundancy")
	}, "15m", "5s").Should(Succeed())

	primaries, answered := primariesNow()
	Expect(answered).To(Equal(chaosReplicas), "every member must answer once the instance has healed")
	Expect(primaries).To(HaveLen(1), "exactly one member may be out of recovery: %v", primaries)

	// The comparison is made on the timeline each member is actually on, which for a standby
	// is the one its WAL receiver is streaming. A standby writes the timeline switch into its
	// control file only when it turns a replayed checkpoint into a restartpoint, and it skips
	// that when it has restartpointed recently - so the control file alone reports a member
	// that has already followed the new history as though it had been left behind on the old.
	const timelineQuery = `SELECT GREATEST(
		(SELECT timeline_id FROM pg_control_checkpoint()),
		COALESCE((SELECT received_tli FROM pg_stat_wal_receiver), 0))`

	var converged string
	Eventually(func(g Gomega) {
		timelines := map[string]string{}
		for _, member := range chaosMemberNames() {
			output, err := chaosPsql(member, timelineQuery)
			g.Expect(err).NotTo(HaveOccurred(), output)
			timelines[member] = output
		}
		converged = timelines[primaries[0]]
		for member, timeline := range timelines {
			g.Expect(timeline).To(Equal(converged),
				"%s is on timeline %s while the primary %s is on %s",
				member, timeline, primaries[0], converged)
		}
	}, "3m", "5s").Should(Succeed())
	AddReportEntry("converged timeline", converged)
}

// The chaos label is what CI filters on: these specs cordon nodes and force-delete Pods
// and take minutes, so they run nightly rather than on every push.
var _ = Describe("Chaos: a three-node instance under real failures", Ordered, Serial, Label("chaos"), func() {
	var oraclePassword string

	AfterEach(dumpOnFailure)

	BeforeAll(func() {
		probeNamespace.Store(chaosNamespace)
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: chaosNamespace}}
		// A namespace left Terminating by a previous run accepts creations and then garbage
		// collects them, which looks exactly like an instance that never appeared.
		Eventually(func() error {
			existing := &corev1.Namespace{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: chaosNamespace}, existing)
			if apierrors.IsNotFound(err) {
				return k8sClient.Create(suiteCtx, namespace)
			}
			if err != nil {
				return err
			}
			if existing.Status.Phase == corev1.NamespaceTerminating {
				return fmt.Errorf("namespace %s is still terminating", chaosNamespace)
			}
			return nil
		}, "5m", "3s").Should(Succeed())

		claimNamespace(chaosNamespace)

		oraclePassword = randomSecret()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: oracleSecret, Namespace: chaosNamespace},
			StringData: map[string]string{"password": oraclePassword},
		}
		Eventually(func() error {
			return client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, secret))
		}, "5m", "3s").Should(Succeed())

		instance := &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: chaosInstance, Namespace: chaosNamespace},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				PoolRef: corev1.LocalObjectReference{Name: claimPoolName},
				Class:   sizingClass,
				Storage: pgelasticv1alpha1.InstanceStorage{
					Size:      resource.MustParse("2Gi"),
					WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("2Gi")},
				},
			},
		}
		// The namespace may still be finalising a previous run's deletion, in which case the
		// API server accepts nothing into it. Retrying is the only way to tell that apart
		// from a spec that is genuinely broken.
		Eventually(func() error {
			return client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, instance))
		}, "5m", "3s").Should(Succeed())

		DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, instance)
			_ = k8sClient.Delete(suiteCtx, namespace)
		})

		By("waiting for the instance to bootstrap")
		Eventually(func(g Gomega) {
			g.Expect(chaosCR().Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
		}, "15m", "5s").Should(Succeed())
		awaitConverged()

		By("creating the role the durability oracle writes as")
		primary := chaosCR().Status.CurrentPrimary
		mustChaosQuery(primary, fmt.Sprintf(
			"CREATE ROLE %s LOGIN PASSWORD %s", oracleRole, quoteSQLLiteral(oraclePassword)))
		mustChaosQuery(primary, fmt.Sprintf("GRANT CREATE, USAGE ON SCHEMA public TO %s", oracleRole))
		// Declared rather than inherited from PUBLIC. Bootstrap revokes PUBLIC's CONNECT on the
		// maintenance databases, because once tenant roles can authenticate, a role locked out
		// of every tenant database could still read every tenant's name out of postgres. The
		// oracle writes to postgres by design, so it says so.
		mustChaosQuery(primary, fmt.Sprintf(
			"GRANT CONNECT ON DATABASE %s TO %s", oracleDatabase, oracleRole))
	})

	It("survives the primary Pod being deleted with no grace period", func() {
		before := chaosCR().Status

		result := runScenario("deletepod", 120*time.Second, func() {
			By("deleting the primary Pod with --grace-period=0 and keeping it gone")
			// The operator recreates a deleted member within a second or two, and on a
			// single-node kind cluster with cached images and local storage it can be back
			// serving before the failover delay has even elapsed - which is the cheaper
			// correct answer, and not the path this spec exists to exercise. Holding the
			// member down for longer than the delay is what a node loss looks like, and it
			// is what forces the promotion.
			keepDown(before.CurrentPrimary, 75*time.Second)

			Eventually(func(g Gomega) {
				g.Expect(chaosCR().Status.CurrentPrimary).NotTo(Equal(before.CurrentPrimary),
					"the instance has to elect somebody else")
			}, "10m", "5s").Should(Succeed())
		})

		awaitConverged()
		assertOraclePassed("deletepod", result)

		after := chaosCR().Status
		Expect(after.PrimaryEpoch).To(BeNumerically(">", before.PrimaryEpoch),
			"a promotion must bump the fence token")
		Expect(mustChaosQuery(after.CurrentPrimary,
			"SELECT current_setting('"+pgconf.GUCPrimaryEpoch+"')")).
			To(Equal(strconv.FormatInt(after.PrimaryEpoch, 10)),
				"the published epoch and the one bound into the postmaster must agree")
	})

	It("survives the postmaster being SIGKILLed inside the primary", func() {
		before := chaosCR().Status

		result := runScenario("sigkill", 90*time.Second, func() {
			By("sending SIGKILL to the postmaster")
			output, err := kubectlCommand("exec", "-n", chaosNamespace,
				before.CurrentPrimary, "-c", "postgres", "--", "sh", "-c",
				"kill -9 $(head -1 "+provision.DataDir+"/postmaster.pid)").CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(output))

			// Whether this becomes a failover or a local restart is the instance's business:
			// a postmaster that comes back inside the debounce is a cheaper correct answer
			// than a failover. What is not negotiable is that no acknowledged commit is lost
			// either way, which is what the oracle is for.
			Eventually(func(g Gomega) {
				_, answered := primariesNow()
				g.Expect(answered).To(Equal(chaosReplicas))
			}, "10m", "5s").Should(Succeed())
		})

		awaitConverged()
		assertOraclePassed("sigkill", result)
		Expect(chaosCR().Status.PrimaryEpoch).To(BeNumerically(">=", before.PrimaryEpoch),
			"the fence token never goes backwards")
	})

	It("makes a graceful drain of the primary block on its PodDisruptionBudget", func() {
		before := chaosCR().Status

		result := runScenario("drain", 60*time.Second, func() {
			By("evicting the primary, which is what kubectl drain does per Pod")
			err := k8sClient.SubResource("eviction").Create(suiteCtx,
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: before.CurrentPrimary, Namespace: chaosNamespace}},
				&policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{
					Name: before.CurrentPrimary, Namespace: chaosNamespace}})
			Expect(err).To(HaveOccurred(),
				"a drain must block on the primary rather than take it down and find out afterwards")
			Expect(apierrors.IsTooManyRequests(err)).To(BeTrue(),
				"the refusal must come from the PodDisruptionBudget, got %v", err)

			By("evicting a standby, which the replica budget does permit")
			standby := otherMember(before.CurrentPrimary)
			evictErr := k8sClient.SubResource("eviction").Create(suiteCtx,
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: standby, Namespace: chaosNamespace}},
				&policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{
					Name: standby, Namespace: chaosNamespace}})
			Expect(evictErr).NotTo(HaveOccurred())
		})

		awaitConverged()
		assertOraclePassed("drain", result)
		Expect(chaosCR().Status.CurrentPrimary).To(Equal(before.CurrentPrimary),
			"a blocked drain must not have moved the primary")
	})

	It("stalls commits rather than degrading when the quorum is lost", func() {
		primary := chaosCR().Status.CurrentPrimary
		loadedBefore := mustChaosQuery(primary, "SHOW synchronous_standby_names")
		Expect(loadedBefore).To(HavePrefix("ANY 1"))

		mustChaosQuery(primary, "CREATE TABLE IF NOT EXISTS chaos_stall (id int primary key)")

		By("holding both standbys down for the whole of the stall window")
		// A single delete is not enough. The operator recreates the member at once and it
		// can be back and acknowledging commits within a couple of seconds, so a stall
		// asserted against one delete measures Pod startup time rather than durability.
		var standbys []string
		for _, member := range chaosMemberNames() {
			if member != primary {
				standbys = append(standbys, member)
			}
		}
		held := holdMembersDown(standbys)
		defer held.release()

		// A deleted Pod takes a few seconds to stop streaming, and a commit issued inside
		// that window is acknowledged by a standby that is still there.
		By("waiting until no standby is streaming any more")
		Eventually(func(g Gomega) {
			streaming, err := chaosPsql(primary,
				"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
			g.Expect(err).NotTo(HaveOccurred(), streaming)
			g.Expect(streaming).To(Equal("0"))
		}, "3m", "2s").Should(Succeed())

		// Everything the stall depends on, read at the instant the commit is issued. Without
		// it a commit that returns is indistinguishable from a commit that had nothing to
		// wait for, and those are a durability bug and a test bug respectively.
		atIssue := fmt.Sprintf("synchronous_commit=%s synchronous_standby_names=%s streaming=%s",
			mustChaosQuery(primary, "SHOW synchronous_commit"),
			mustChaosQuery(primary, "SHOW synchronous_standby_names"),
			mustChaosQuery(primary,
				"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"))

		By("issuing a commit that cannot reach a quorum")
		blocked := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			output, err := chaosPsql(primary, "INSERT INTO chaos_stall VALUES (1)")
			if err != nil {
				err = fmt.Errorf("%w: %s", err, output)
			}
			blocked <- err
		}()

		// The commit must not return, and the reason it must not return has to still hold
		// at every sample. A commit that stayed blocked while a standby was quietly
		// acknowledging it would prove nothing, so the quorum is re-checked alongside it.
		Consistently(func() string {
			select {
			case err := <-blocked:
				return fmt.Sprintf("the commit returned while the quorum was lost: %v", err)
			default:
			}
			streaming, err := chaosPsql(primary,
				"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")
			if err != nil {
				return fmt.Sprintf("the primary stopped answering: %v: %s", err, streaming)
			}
			if streaming != "0" {
				return "a standby started streaming again, so the quorum was not lost: " + streaming
			}
			return ""
		}, "60s", "3s").Should(BeEmpty(),
			"dataDurability Required means the commit stalls; it must not be acknowledged (%s)",
			atIssue)

		loadedDuring := mustChaosQuery(primary, "SHOW synchronous_standby_names")
		Expect(loadedDuring).To(Equal(loadedBefore),
			"silently unsetting synchronous_standby_names would turn a stall into "+
				"undetectable asynchronous replication")
		Eventually(func(g Gomega) {
			condition := conditionOfChaos(pgelasticv1alpha1.ConditionWriteStalled)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		}, "3m", "5s").Should(Succeed(), "a stalled commit has to be alertable, not inexplicable")

		By("letting the standbys come back")
		held.release()

		var completed error
		Eventually(blocked, "15m", "5s").Should(Receive(&completed),
			"the blocked commit must complete once a standby can acknowledge it")
		Expect(completed).NotTo(HaveOccurred())

		Expect(mustChaosQuery(primary, "SELECT count(*) FROM chaos_stall")).To(Equal("1"))
		awaitConverged()
	})

	It("brings every member back after a double failover forks a third timeline", func() {
		before := chaosCR().Status
		beforeTimeline := memberTimeline(before.CurrentPrimary)

		endpoints := watchReadWriteEndpoints()
		result := runScenario("doublefailover", 240*time.Second, func() {
			By("losing the primary once")
			losePrimary(before.CurrentPrimary)

			// The second failover has to wait for all three members to answer, because the
			// quorum gate needs two of them reachable to prove a promotion is safe at all -
			// but not for the returning member to have caught up. A member still short of
			// the position the next timeline forks at is exactly the one that cannot rejoin
			// by streaming afterwards, and it is what this scenario exists to produce.
			By("waiting for all three members to answer before breaking it again")
			Eventually(func(g Gomega) {
				_, answered := primariesNow()
				g.Expect(answered).To(Equal(chaosReplicas))
			}, "10m", "3s").Should(Succeed())

			By("losing the newly promoted primary too, which forks a third timeline")
			losePrimary(chaosCR().Status.CurrentPrimary)
		})

		awaitConverged()
		endpoints.assertServiceFollowedTheServingMember()
		assertOraclePassed("doublefailover", result)

		after := chaosCR().Status
		Expect(after.PrimaryEpoch).To(BeNumerically(">", before.PrimaryEpoch+1),
			"two promotions must bump the fence token twice")
		Expect(memberTimeline(after.CurrentPrimary)).To(BeNumerically(">", beforeTimeline+1),
			"two promotions must fork two timelines")
	})

	It("keeps the read-write Service on whichever member is serving", func() {
		primary := chaosCR().Status.CurrentPrimary
		Expect(mustChaosQuery(primary, "SELECT pg_is_in_recovery()")).To(Equal("f"))

		watch := watchReadWriteEndpoints()
		// Nothing is broken here on purpose. The failure this catches is a healthy primary
		// left unlabelled because the operator was merely considering a failover, and the
		// only way to see it is to keep asking while nothing is wrong.
		time.Sleep(30 * time.Second)
		watch.assertServiceFollowedTheServingMember()

		Expect(leaseHolder()).To(Equal(primary),
			"the member serving writes has to be the one holding the promotion lease")
	})

	It("refuses to promote when the quorum gate cannot be satisfied", func() {
		before := chaosCR().Status
		standby := otherMember(before.CurrentPrimary)

		By("taking one standby and then the primary down, and holding them there, leaving R = 1")
		// Both have to stay down for as long as the refusal is asserted. A member that is
		// merely deleted is back within seconds, and a gate that was never asked to deny
		// anything cannot be observed denying it.
		held := holdMembersDown([]string{standby, before.CurrentPrimary})
		defer held.release()

		watch := watchForSplitBrain()
		By("watching the operator refuse the failover it cannot prove is safe")
		Eventually(func(g Gomega) {
			status := chaosCR().Status
			g.Expect(status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending),
				"the sentinel is where a denied failover stops")
			condition := conditionOfChaos(pgelasticv1alpha1.ConditionFailingOver)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Reason).To(BeElementOf(
				pgelasticv1alpha1.ReasonQuorumNotProven,
				pgelasticv1alpha1.ReasonNoEligibleCandidate,
				pgelasticv1alpha1.ReasonWaitingWALReceivers,
				pgelasticv1alpha1.ReasonOperatorIsolated),
				"the refusal has to be named: %s", condition.Message)
		}, "5m", "3s").Should(Succeed())

		// The surviving member is a standby and must stay one. Promoting it is exactly the
		// "replica promoted while behind" failure the gate prevents rather than detects.
		survivor := remainingMember(before.CurrentPrimary, standby)
		// A survivor that stops answering is not evidence that it stayed a standby, so the
		// error is reported rather than read as "still in recovery".
		Consistently(func() string {
			output, err := chaosPsql(survivor, "SELECT pg_is_in_recovery()")
			if err != nil {
				return fmt.Sprintf("%s did not answer: %v: %s", survivor, err, output)
			}
			return output
		}, "45s", "5s").Should(Equal("t"),
			"a member that cannot be proven to hold the last acknowledged commit must not be promoted")

		Expect(chaosCR().Status.CurrentPrimary).To(Equal(before.CurrentPrimary),
			"no promotion may have happened")
		Expect(chaosCR().Status.PrimaryEpoch).To(Equal(before.PrimaryEpoch),
			"a denied failover must not bump the fence token")
		watch.assertNoSplitBrain()

		By("letting the members that were held down come back")
		held.release()
		awaitConverged()
	})
})

func conditionOfChaos(conditionType string) *metav1.Condition {
	for i, condition := range chaosCR().Status.Conditions {
		if condition.Type == conditionType {
			return &chaosCR().Status.Conditions[i]
		}
	}
	return nil
}

func otherMember(primary string) string {
	for _, member := range chaosMemberNames() {
		if member != primary {
			return member
		}
	}
	return ""
}

// standbysOf is every member that is not the named primary, sorted, which is the order
// pg_stat_replication is aggregated in.
func standbysOf(primary string) []string {
	standbys := make([]string, 0, chaosReplicas-1)
	for _, member := range chaosMemberNames() {
		if member != primary {
			standbys = append(standbys, member)
		}
	}
	slices.Sort(standbys)
	return standbys
}

func remainingMember(taken ...string) string {
	for _, member := range chaosMemberNames() {
		if !slices.Contains(taken, member) {
			return member
		}
	}
	return ""
}

func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// dumpOnFailure records what the operator decided when a scenario fails. It is the
// difference between a reproducible bug and a flake.
func dumpOnFailure() {
	if !CurrentSpecReport().Failed() {
		return
	}
	output, err := kubectlCommand("get", "pginstance", "-n", chaosNamespace,
		chaosInstance, "-o", "yaml").CombinedOutput()
	if err == nil {
		AddReportEntry("PgInstance at failure", string(output))
	}
	for _, member := range chaosMemberNames() {
		logs, logErr := kubectlCommand("logs", "-n", chaosNamespace, member,
			"-c", "postgres", "--tail", "200").CombinedOutput()
		if logErr == nil {
			AddReportEntry(member+" at failure", string(logs))
		}
	}
}
