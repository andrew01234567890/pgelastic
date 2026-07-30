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
// A second operator rewriting the proxy Deployment shows up in all three at once: the
// Deployment's generation advances on every rewrite, a second ReplicaSet appears to serve
// the template it prefers, and the Pods turn over as the two ReplicaSets are scaled against
// each other. Reading only one of them would leave a way for the fight to hide.
//
// A Pod is identified by its UID and its restart count together, because the two failures
// they catch are different ones: a replaced Pod gets a new UID, and a replica whose
// container the kubelet restarted in place keeps the UID it had. Both drop every client
// socket the replica was holding, so identity alone would let the second one through.
type fleetShape struct {
	generation  int64
	replicaSets []string
	pods        []string
}

func (s fleetShape) String() string {
	return fmt.Sprintf("generation=%d replicaSets=[%s] pods=[%s]",
		s.generation, strings.Join(s.replicaSets, " "), strings.Join(s.pods, " "))
}

// observeFleet reads the fleet straight from the API server. Nothing here goes through the
// suite operator's informer cache: what is under suspicion is precisely what an operator
// believes about this Deployment.
func observeFleet() fleetShape {
	GinkgoHelper()
	deployment := &appsv1.Deployment{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: e2eNamespace, Name: proxyobjects.DeploymentName(poolName),
	}, deployment)).To(Succeed())

	selector := client.MatchingLabels(proxyobjects.Selector(poolName))
	replicaSets := &appsv1.ReplicaSetList{}
	Expect(k8sClient.List(suiteCtx, replicaSets, client.InNamespace(e2eNamespace), selector)).
		To(Succeed())
	live := make([]string, 0, len(replicaSets.Items))
	for i := range replicaSets.Items {
		set := &replicaSets.Items[i]
		if set.Status.Replicas > 0 || (set.Spec.Replicas != nil && *set.Spec.Replicas > 0) {
			live = append(live, set.Name)
		}
	}
	slices.Sort(live)

	pods := &corev1.PodList{}
	Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace), selector)).To(Succeed())
	identities := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp.IsZero() {
			identities = append(identities,
				fmt.Sprintf("%s restarts=%d", pod.UID, containerRestarts(pod)))
		}
	}
	slices.Sort(identities)

	return fleetShape{generation: deployment.Generation, replicaSets: live, pods: identities}
}

func containerRestarts(pod *corev1.Pod) int32 {
	total := int32(0)
	for _, status := range pod.Status.ContainerStatuses {
		total += status.RestartCount
	}
	return total
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
