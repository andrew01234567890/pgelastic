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

package policy_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

func TestPolicy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Policy Suite")
}

func ptrTo[T any](value T) *T { return &value }

func quantity(text string) *resource.Quantity {
	parsed := resource.MustParse(text)
	return &parsed
}

func duration(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

func workloadClass(name string, guaranteed, burstable int32) *pgelasticv1alpha1.PgWorkloadClass {
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

func problemPaths(problems []policy.Problem) []string {
	paths := make([]string, 0, len(problems))
	for _, problem := range problems {
		paths = append(paths, problem.Path)
	}
	return paths
}

var _ = Describe("QoS derivation", func() {
	DescribeTable("follows the kubelet's rule",
		func(guaranteed, burstable int32, expected pgelasticv1alpha1.QoSClass) {
			Expect(policy.DeriveQoS(guaranteed, burstable)).To(Equal(expected))
		},
		Entry("a floor equal to the ceiling", int32(8), int32(8), pgelasticv1alpha1.QoSGuaranteed),
		Entry("a floor below the ceiling", int32(4), int32(40), pgelasticv1alpha1.QoSBurstable),
		Entry("no floor at all", int32(0), int32(8), pgelasticv1alpha1.QoSBestEffort),
		Entry("a floor of one below a ceiling of two", int32(1), int32(2), pgelasticv1alpha1.QoSBurstable),
	)
})

var _ = Describe("effective capacity", func() {
	It("takes the workload class triple when the tenant overrides nothing", func() {
		tenant := &pgelasticv1alpha1.PgTenant{}
		class := workloadClass("premium", 4, 40)
		class.Spec.Capacity.Weight = ptrTo(int32(400))
		class.Spec.Limits = &pgelasticv1alpha1.TenantLimits{
			StatementTimeout: duration(60 * time.Second),
			TempFileLimit:    quantity("8Gi"),
		}

		effective := policy.EffectiveFor(tenant, class)

		Expect(effective.Guaranteed).To(Equal(int32(4)))
		Expect(effective.Burstable).To(Equal(int32(40)))
		Expect(effective.Weight).To(Equal(int32(400)))
		Expect(effective.QoSClass).To(Equal(pgelasticv1alpha1.QoSBurstable))
		Expect(effective.StatementTimeout.Duration).To(Equal(60 * time.Second))
	})

	It("re-derives the QoS class from a tenant override", func() {
		tenant := &pgelasticv1alpha1.PgTenant{
			Spec: pgelasticv1alpha1.PgTenantSpec{
				Capacity: &pgelasticv1alpha1.PgTenantCapacity{
					Guaranteed: ptrTo(int32(60)),
					Burstable:  ptrTo(int32(60)),
				},
			},
		}

		effective := policy.EffectiveFor(tenant, workloadClass("standard", 0, 8))

		Expect(effective.Guaranteed).To(Equal(int32(60)))
		Expect(effective.QoSClass).To(Equal(pgelasticv1alpha1.QoSGuaranteed))
	})

	It("carries the class's automatic-migration opt-out onto the effective policy", func() {
		class := workloadClass("no-auto-moves", 0, 8)
		class.Spec.Migration = &pgelasticv1alpha1.WorkloadMigrationPolicy{
			AllowAutomatic: ptrTo(false),
		}

		effective := policy.EffectiveFor(&pgelasticv1alpha1.PgTenant{}, class)

		Expect(effective.AutomaticMigrationAllowed).To(BeFalse(),
			"the rebalancer would move a tenant whose class forbids being moved")
	})

	It("allows automatic migration when the class says nothing about it", func() {
		class := workloadClass("standard", 0, 8)
		Expect(policy.EffectiveFor(&pgelasticv1alpha1.PgTenant{}, class).AutomaticMigrationAllowed).
			To(BeTrue())

		class.Spec.Migration = &pgelasticv1alpha1.WorkloadMigrationPolicy{}
		Expect(policy.EffectiveFor(&pgelasticv1alpha1.PgTenant{}, class).AutomaticMigrationAllowed).
			To(BeTrue())
	})

	It("defaults the weight when the class predates the field", func() {
		class := workloadClass("legacy", 0, 8)
		class.Spec.Capacity.Weight = nil

		Expect(policy.EffectiveFor(&pgelasticv1alpha1.PgTenant{}, class).Weight).To(Equal(int32(100)))
	})
})

var _ = Describe("allocatable capacity", func() {
	DescribeTable("withholds headroom before any guarantee is counted",
		func(backendConnections, headroomPercent, expected int32) {
			Expect(policy.Allocatable(backendConnections, headroomPercent)).To(Equal(expected))
		},
		Entry("the documented pool", int32(900), int32(25), int32(675)),
		Entry("no headroom", int32(100), int32(0), int32(100)),
		Entry("the maximum headroom", int32(100), int32(50), int32(50)),
		Entry("a budget that does not divide evenly", int32(10), int32(25), int32(7)),
	)
})

var _ = Describe("self-consistency problems", func() {
	DescribeTable("are reported at the field that is actually wrong",
		func(mutate func(*pgelasticv1alpha1.PgWorkloadClass), expectedPath string) {
			class := workloadClass("in-memory", 0, 8)
			mutate(class)

			Expect(problemPaths(policy.WorkloadClassProblems(class))).To(ContainElement(expectedPath))
		},
		Entry("a zero lock timeout", func(class *pgelasticv1alpha1.PgWorkloadClass) {
			class.Spec.Limits = &pgelasticv1alpha1.TenantLimits{LockTimeout: duration(0)}
		}, "spec.limits.lockTimeout"),
		Entry("a negative temp file limit", func(class *pgelasticv1alpha1.PgWorkloadClass) {
			class.Spec.Limits = &pgelasticv1alpha1.TenantLimits{TempFileLimit: quantity("-1Gi")}
		}, "spec.limits.tempFileLimit"),
		Entry("a required quarantine with no window", func(class *pgelasticv1alpha1.PgWorkloadClass) {
			class.Spec.Admission = &pgelasticv1alpha1.WorkloadAdmission{
				Quarantine: &pgelasticv1alpha1.QuarantinePolicy{
					Required: ptrTo(true),
					Duration: duration(0),
				},
			}
		}, "spec.admission.quarantine.duration"),
	)

	It("passes a class whose limits are all real limits", func() {
		class := workloadClass("in-memory", 4, 40)
		class.Spec.Limits = &pgelasticv1alpha1.TenantLimits{
			StatementTimeout: duration(30 * time.Second),
			TempFileLimit:    quantity("2Gi"),
		}

		Expect(policy.WorkloadClassProblems(class)).To(BeEmpty())
	})

	It("passes a pool class whose headroom leaves a migration room to work", func() {
		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			Spec: pgelasticv1alpha1.PgElasticClassSpec{
				Defaults: &pgelasticv1alpha1.ElasticClassDefaults{
					HeadroomPercent:          ptrTo(int32(25)),
					MigrationHeadroomPercent: ptrTo(int32(10)),
				},
			},
		}

		Expect(policy.ElasticClassProblems(elasticClass)).To(BeEmpty())
	})
})
