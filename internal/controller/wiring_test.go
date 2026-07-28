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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/andrew01234567890/pgelastic/internal/index"
	webhookv1alpha1 "github.com/andrew01234567890/pgelastic/internal/webhook/v1alpha1"
)

// The manager built here is never started; the point is that every index, controller and
// webhook the operator wires up registers without colliding, which is the failure the
// binary would otherwise only show on a cluster.
var _ = Describe("manager wiring", func() {
	It("registers every index, reconciler and webhook exactly once", func() {
		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:         scheme.Scheme,
			Metrics:        metricsserver.Options{BindAddress: "0"},
			LeaderElection: false,
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(index.Setup(ctx, manager.GetFieldIndexer())).To(Succeed())

		Expect((&PgElasticClassReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())
		Expect((&PgWorkloadClassReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())
		Expect((&PgElasticPoolReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())
		Expect((&PgInstanceReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())
		Expect((&PgTenantReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())
		Expect((&PgTenantMigrationReconciler{
			Client: manager.GetClient(), Scheme: manager.GetScheme(),
		}).SetupWithManager(manager)).To(Succeed())

		Expect(webhookv1alpha1.SetupPgElasticPoolWebhookWithManager(manager)).To(Succeed())
		Expect(webhookv1alpha1.SetupPgTenantWebhookWithManager(manager)).To(Succeed())
		Expect(webhookv1alpha1.SetupPgWorkloadClassWebhookWithManager(manager)).To(Succeed())
	})
})
