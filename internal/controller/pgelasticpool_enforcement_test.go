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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

var enforcementNamespaces int

// spec.timeouts.query and spec.timeouts.clientIdleInTransaction were documented, defaulted and
// stored on every pool for as long as the CRD has existed, and read by nothing. Deploying the
// operator that reads them turns them on across an estate with no spec change by anybody, and
// the only other signal an operator would get is their own application's errors.
var _ = Describe("warning about a timeout this upgrade starts enforcing", func() {
	var (
		namespace string
		poolName  = "enforcement-pool"
		className = "enforcement-class"
		recorder  *events.FakeRecorder
	)

	BeforeEach(func() {
		enforcementNamespaces++
		namespace = fmt.Sprintf("enforcement-%d", enforcementNamespaces)
		ensureNamespace(namespace)
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx,
			makeElasticClass(className, defaultControllerName)))).To(Succeed())
		recorder = events.NewFakeRecorder(64)
	})

	// What an upgrade looks like: a Secret written by an operator that could not enforce the
	// bound, and a document from this one that can.
	writePreviousDocument := func(pool string, document string) {
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      proxy.ConfigSecretName(pool),
				Namespace: namespace,
			},
			StringData: map[string]string{proxy.ConfigKey: document},
		})).To(Succeed())
	}

	drain := func() []string {
		var seen []string
		for {
			select {
			case event := <-recorder.Events:
				seen = append(seen, event)
			default:
				return seen
			}
		}
	}

	It("warns once for each bound the previous document did not carry", func() {
		pool := makePool(namespace, poolName, className, 300)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		writePreviousDocument(poolName, "[pool]\nqueryWaitSeconds = 30\n")

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
		}
		reconciler.warnNewlyEnforcedTimeouts(ctx, pool,
			"[pool]\nqueryWaitSeconds = 30\nqueryDeadlineSeconds = 120\n"+
				"clientIdleInTransactionSeconds = 60\nmaxPinnedPercent = 20\n")

		events := drain()
		Expect(events).To(HaveLen(3))
		Expect(events[0]).To(ContainSubstring("spec.timeouts.query is now enforced at 120s"))
		Expect(events[0]).To(ContainSubstring("Set it to 0"))
		Expect(events[1]).To(ContainSubstring(
			"spec.timeouts.clientIdleInTransaction is now enforced at 60s"))
		Expect(events[2]).To(ContainSubstring(
			"spec.pooling.maxPinnedFractionPercent is now enforced at 20%"))
	})

	// The second reconcile of the same pool, and every one after it. A warning repeated on
	// every pass is one an operator filters out, which is the same as not sending it.
	It("says nothing once the fleet is already running the bound", func() {
		pool := makePool(namespace, poolName, className, 300)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		writePreviousDocument(poolName,
			"[pool]\nqueryDeadlineSeconds = 120\nclientIdleInTransactionSeconds = 60\n"+
				"maxPinnedPercent = 20\n")

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
		}
		reconciler.warnNewlyEnforcedTimeouts(ctx, pool,
			"[pool]\nqueryDeadlineSeconds = 90\nclientIdleInTransactionSeconds = 60\n"+
				"maxPinnedPercent = 50\n")

		Expect(drain()).To(BeEmpty())
	})

	// An operator who has already chosen to be unbounded is not being changed under, so there
	// is nothing to warn about.
	It("says nothing about a bound the pool has turned off", func() {
		pool := makePool(namespace, poolName, className, 300)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		writePreviousDocument(poolName, "[pool]\nqueryWaitSeconds = 30\n")

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
		}
		reconciler.warnNewlyEnforcedTimeouts(ctx, pool,
			"[pool]\nqueryDeadlineSeconds = 0\nclientIdleInTransactionSeconds = 0\n"+
				"maxPinnedPercent = 0\n")

		Expect(drain()).To(BeEmpty())
	})

	// Every one of these bounds is enforced by the transaction-mode relay. A session-mode pool
	// runs none of them, and an operator told otherwise audits for a limit that does not exist.
	It("says nothing to a pool that pools by session", func() {
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Pooling = &pgelasticv1alpha1.PoolingConfig{
			Mode: pgelasticv1alpha1.PoolModeSession,
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		writePreviousDocument(poolName, "[pool]\nqueryWaitSeconds = 30\n")

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
		}
		reconciler.warnNewlyEnforcedTimeouts(ctx, pool,
			"[pool]\nmode = \"session\"\nqueryDeadlineSeconds = 120\n")

		Expect(drain()).To(BeEmpty())
	})

	// A pool being created now has never run under an operator that did not enforce these, so
	// nobody can be surprised by them.
	It("says nothing to a pool that has no fleet yet", func() {
		pool := makePool(namespace, poolName, className, 300)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
		}
		reconciler.warnNewlyEnforcedTimeouts(ctx, pool,
			"[pool]\nqueryDeadlineSeconds = 120\n")

		Expect(drain()).To(BeEmpty())
	})
})

var _ = Describe("reading a rendered integer back out of a document", func() {
	// The key is matched whole. A prefix match would find queryDeadlineSeconds inside a longer
	// name and report somebody else's number as the deadline.
	It("does not match a longer key that starts with the one asked for", func() {
		// Ordered so a prefix match would find the wrong line first and report 7 as the
		// deadline. The scan returns the first key it matches.
		document := "[pool]\nqueryDeadlineSecondsLegacy = 7\nqueryDeadlineSeconds = 120\n"
		seconds, found := renderedSeconds(document, "queryDeadlineSeconds")
		Expect(found).To(BeTrue())
		Expect(seconds).To(Equal(int64(120)))
	})

	It("reports an absent key as absent rather than as zero", func() {
		seconds, found := renderedSeconds("[pool]\nqueryWaitSeconds = 30\n",
			"queryDeadlineSeconds")
		Expect(found).To(BeFalse())
		Expect(seconds).To(BeZero())
	})
})
