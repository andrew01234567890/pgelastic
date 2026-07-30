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
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
)

const (
	// coexistenceWindow is how long the fleet is watched with both operators running. The
	// churn two of them produce turned Pods over roughly every eight seconds, so this is a
	// dozen chances to observe one flip; a window that caught nothing at this length is
	// evidence rather than luck.
	coexistenceWindow = 90 * time.Second
	// coexistenceSample is short relative to one of those flips.
	coexistenceSample = 3 * time.Second
)

// deployedOperator locates the operator already running on the cluster. The whole claim
// rests on it being there, so it is looked up and asserted rather than assumed.
var deployedOperator = client.ObjectKey{
	Namespace: envOr("PGELASTIC_OPERATOR_NAMESPACE", "pgelastic-system"),
	Name:      envOr("PGELASTIC_OPERATOR_DEPLOYMENT", "pgelastic-controller-manager"),
}

// fleetShape is what "nobody else is writing to this fleet" is measured as.
//
// A second operator rewriting the proxy Deployment shows up in all four at once: the
// structural half of the rendered configuration is what the two of them disagree about and
// it is carried on the Pod template as an annotation, the Deployment's generation advances
// on every rewrite, a second ReplicaSet appears to serve the template it prefers, and the
// Pods turn over as the two ReplicaSets are scaled against each other. Reading only one of
// them would leave a way for the fight to hide.
//
// configHash is also the sharpest thing a roll can be held to. It is the value the rollout
// is a consequence of, so it moves before any Pod does: a spec that watches it accuses the
// configuration that changed rather than the replica that went away because of it.
type fleetShape struct {
	configHash  string
	generation  int64
	replicaSets []string
	pods        []string
}

func (s fleetShape) String() string {
	return fmt.Sprintf("configHash=%s generation=%d replicaSets=[%s] pods=[%s]",
		s.configHash, s.generation, strings.Join(s.replicaSets, " "),
		strings.Join(s.pods, " "))
}

// observeFleet reads the fleet straight from the API server. Nothing here goes through the
// suite operator's informer cache: what is under suspicion is precisely what an operator
// believes about this Deployment.
func observeFleet() fleetShape {
	GinkgoHelper()
	shape, err := readFleet()
	Expect(err).NotTo(HaveOccurred())
	return shape
}

// readFleet is observeFleet without the assertions, so a sampler running on its own
// goroutine can call it. Gomega's failure handler belongs to the spec's goroutine, and a
// transient read error from a background sampler is not a verdict on anything.
func readFleet() (fleetShape, error) {
	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
	}, deployment); err != nil {
		return fleetShape{}, err
	}

	selector := client.MatchingLabels(proxyobjects.Selector(poolName))
	replicaSets := &appsv1.ReplicaSetList{}
	if err := k8sClient.List(suiteCtx, replicaSets,
		client.InNamespace(e2eNamespace), selector); err != nil {
		return fleetShape{}, err
	}
	live := make([]string, 0, len(replicaSets.Items))
	for i := range replicaSets.Items {
		set := &replicaSets.Items[i]
		if set.Status.Replicas > 0 || (set.Spec.Replicas != nil && *set.Spec.Replicas > 0) {
			live = append(live, set.Name)
		}
	}
	slices.Sort(live)

	pods := &corev1.PodList{}
	if err := k8sClient.List(suiteCtx, pods,
		client.InNamespace(e2eNamespace), selector); err != nil {
		return fleetShape{}, err
	}
	identities := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp.IsZero() {
			identities = append(identities, string(pod.UID))
		}
	}
	slices.Sort(identities)

	return fleetShape{
		configHash:  deployment.Spec.Template.Annotations[proxyobjects.AnnotationConfigHash],
		generation:  deployment.Generation,
		replicaSets: live,
		pods:        identities,
	}, nil
}

// fleetWatch samples the fleet for as long as it is open and keeps every distinct shape it
// saw, in the order it saw them.
//
// Comparing the two ends is not enough and the reason is not hypothetical: a fleet that
// rolled and settled back onto the same ReplicaSet with the same replica count ends where it
// started, and every client it was carrying is gone. What has to be asserted is that the
// fleet never moved, not that it came back.
type fleetWatch struct {
	mu     sync.Mutex
	seen   []fleetShape
	stop   chan struct{}
	closed chan struct{}
	once   sync.Once
}

// watchFleet begins sampling. The interval is short relative to a rollout: with maxSurge
// zero a replacement takes seconds, so this is several looks at every one of them.
func watchFleet(every time.Duration) *fleetWatch {
	w := &fleetWatch{stop: make(chan struct{}), closed: make(chan struct{})}
	w.sample()
	go func() {
		defer close(w.closed)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				w.sample()
			}
		}
	}()
	return w
}

func (w *fleetWatch) sample() {
	shape, err := readFleet()
	if err != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.seen) > 0 && fmt.Sprint(w.seen[len(w.seen)-1]) == fmt.Sprint(shape) {
		return
	}
	w.seen = append(w.seen, shape)
}

// close stops sampling, takes one final look, and reports every distinct shape observed. A
// fleet nothing touched reports exactly one.
func (w *fleetWatch) close() []fleetShape {
	w.once.Do(func() {
		close(w.stop)
		<-w.closed
		w.sample()
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]fleetShape(nil), w.seen...)
}

// journalOf renders a sequence of observations one per line, so a failure shows what the
// fleet did rather than only that it did something.
func journalOf(shapes []fleetShape) string {
	lines := make([]string, 0, len(shapes))
	for i, shape := range shapes {
		lines = append(lines, fmt.Sprintf("  %d. %s", i+1, shape))
	}
	return strings.Join(lines, "\n")
}

// requireDeployedOperator fails the spec unless another pgelastic operator is serving on
// this cluster. Skipping instead would turn the one spec that proves coexistence into a
// spec that passes hardest when there is nothing to coexist with.
func requireDeployedOperator() {
	GinkgoHelper()
	deployment := &appsv1.Deployment{}
	Expect(k8sClient.Get(suiteCtx, deployedOperator, deployment)).To(Succeed(),
		"this spec is the proof that two pgelastic operators can share a cluster, so the "+
			"deployed operator %s has to be running; deploy it and run again", deployedOperator)
	Expect(deployment.Status.ReadyReplicas).To(BeNumerically(">=", 1),
		"the deployed operator %s has no ready replica, so nothing would be competing for "+
			"this suite's objects", deployedOperator)
}
