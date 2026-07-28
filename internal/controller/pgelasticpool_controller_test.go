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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

var _ = Describe("PgElasticPool Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-elastic-pool"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		pgelasticpool := &pgelasticv1alpha1.PgElasticPool{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind PgElasticPool")
			err := k8sClient.Get(ctx, typeNamespacedName, pgelasticpool)
			if err != nil && errors.IsNotFound(err) {
				resource := &pgelasticv1alpha1.PgElasticPool{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: pgelasticv1alpha1.PgElasticPoolSpec{
						ClassRef: pgelasticv1alpha1.ClassReference{
							Kind: elasticClassKind,
							Name: "saas-shared-gp",
						},
						Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 900},
						Instances: pgelasticv1alpha1.PoolInstances{
							Template: pgelasticv1alpha1.PgInstanceTemplate{
								Class: "gp-8",
								Storage: pgelasticv1alpha1.InstanceStorage{
									Size: resource.MustParse("500Gi"),
									WALVolume: pgelasticv1alpha1.WALVolume{
										Size: resource.MustParse("100Gi"),
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &pgelasticv1alpha1.PgElasticPool{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance PgElasticPool")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &PgElasticPoolReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
