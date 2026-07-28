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

package v1alpha1

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	elasticClassKind  = "PgElasticClass"
	instanceClassName = "gp-8"

	// A fixture pool starts from a budget of 100 with a quarter withheld, which leaves
	// exactly 75 allocatable — the figure the reservation ledger specs count against.
	poolBackendConnections int32 = 100
	poolHeadroomPercent    int32 = 25
)

func ptrTo[T any](value T) *T { return &value }

func quantity(text string) resource.Quantity { return resource.MustParse(text) }

func ensureNamespace(name string, namespaceLabels map[string]string) {
	GinkgoHelper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: namespaceLabels}}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, namespace))).To(Succeed())
}

func makeElasticClass(name string) *pgelasticv1alpha1.PgElasticClass {
	return &pgelasticv1alpha1.PgElasticClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pgelasticv1alpha1.PgElasticClassSpec{
			ControllerName: "pgelastic.io/elastic-pool-controller",
		},
	}
}

func makePool(namespace, name, className string) *pgelasticv1alpha1.PgElasticPool {
	return &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgElasticPoolSpec{
			ClassRef: pgelasticv1alpha1.ClassReference{
				APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
				Kind:     elasticClassKind,
				Name:     className,
			},
			Capacity: pgelasticv1alpha1.PoolCapacity{
				BackendConnections: poolBackendConnections,
				HeadroomPercent:    ptrTo(poolHeadroomPercent),
			},
			Instances: pgelasticv1alpha1.PoolInstances{
				Template: pgelasticv1alpha1.PgInstanceTemplate{
					Class: instanceClassName,
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      quantity("100Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: quantity("20Gi")},
					},
				},
			},
		},
	}
}

func makeWorkloadClass(name string, guaranteed, burstable int32) *pgelasticv1alpha1.PgWorkloadClass {
	return &pgelasticv1alpha1.PgWorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
			Priority: 1000,
			Capacity: pgelasticv1alpha1.WorkloadCapacity{
				Guaranteed: ptrTo(guaranteed),
				Burstable:  burstable,
			},
		},
	}
}

func makeTenant(namespace, name, pool, database, workloadClassName string) *pgelasticv1alpha1.PgTenant {
	tenant := &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: pool},
			DatabaseName: database,
		},
	}
	if workloadClassName != "" {
		tenant.Spec.WorkloadClassName = ptrTo(workloadClassName)
	}
	return tenant
}

// mustCreate creates objects the webhook is expected to admit. Nothing is torn down
// afterwards: every fixture is uniquely named and the control plane is thrown away with
// the suite, so a spec that needs cluster-wide state gone deletes it explicitly.
func mustCreate(objects ...client.Object) {
	GinkgoHelper()
	for _, object := range objects {
		Expect(k8sClient.Create(ctx, object)).To(Succeed())
	}
}

func mustDelete(objects ...client.Object) {
	GinkgoHelper()
	for _, object := range objects {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object))).To(Succeed())
	}
}

func keyOf(name string) types.NamespacedName { return types.NamespacedName{Name: name} }

func keyIn(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

func duration(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }
