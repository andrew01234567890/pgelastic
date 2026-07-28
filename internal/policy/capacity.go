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

// Package policy resolves the three-object policy graph — PgElasticClass,
// PgElasticPool and PgWorkloadClass — into the single set of numbers that admission
// and the reservation ledger act on. Both the controllers and the validating webhook
// resolve through this package so that what a webhook admits and what a controller
// publishes in status can never disagree.
package policy

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// defaultWeight matches the PgWorkloadClass capacity.weight CRD default and is applied
// when a class predates the default or was written by a client that omitted it.
const defaultWeight int32 = 100

// DefaultHeadroomPercent matches the PgElasticPool capacity.headroomPercent CRD default.
const DefaultHeadroomPercent int32 = 25

// Effective is the set of limits actually in force for one tenant, after the workload
// class supplies its triple and the tenant's own overrides are applied. It is what
// status.effective publishes and what the reservation ledger sums.
type Effective struct {
	// WorkloadClassName is the class the ladder resolved to.
	WorkloadClassName string
	// Guaranteed is the reserved backend-connection floor.
	Guaranteed int32
	// Burstable is the backend-connection ceiling.
	Burstable int32
	// Weight is the share of contended surplus.
	Weight int32
	// QoSClass is derived from Guaranteed and Burstable, never declared.
	QoSClass pgelasticv1alpha1.QoSClass
	// StatementTimeout is the deadline published to the proxy.
	StatementTimeout *metav1.Duration
	// TempFileLimit is the per-process temporary file cap published to the backend.
	TempFileLimit *resource.Quantity
}

// DeriveQoS derives a tenant's QoS class from its effective capacity exactly as the
// kubelet derives Pod QoS: a floor equal to the ceiling is Guaranteed, a floor below a
// ceiling is Burstable, and no floor at all is BestEffort.
//
// The comparison is >= rather than == so that a stored object whose guarantee somehow
// exceeds its ceiling still reports the class its admission behaviour will actually
// have, rather than falling through to Burstable.
func DeriveQoS(guaranteed, burstable int32) pgelasticv1alpha1.QoSClass {
	switch {
	case guaranteed <= 0:
		return pgelasticv1alpha1.QoSBestEffort
	case guaranteed >= burstable:
		return pgelasticv1alpha1.QoSGuaranteed
	default:
		return pgelasticv1alpha1.QoSBurstable
	}
}

// EffectiveFor applies the tenant's capacity overrides on top of its workload class and
// derives the QoS class from the result.
func EffectiveFor(tenant *pgelasticv1alpha1.PgTenant, class *pgelasticv1alpha1.PgWorkloadClass) Effective {
	effective := Effective{
		WorkloadClassName: class.Name,
		Burstable:         class.Spec.Capacity.Burstable,
		Weight:            defaultWeight,
	}
	if class.Spec.Capacity.Guaranteed != nil {
		effective.Guaranteed = *class.Spec.Capacity.Guaranteed
	}
	if class.Spec.Capacity.Weight != nil {
		effective.Weight = *class.Spec.Capacity.Weight
	}
	if limits := class.Spec.Limits; limits != nil {
		effective.StatementTimeout = limits.StatementTimeout
		effective.TempFileLimit = limits.TempFileLimit
	}

	if override := tenant.Spec.Capacity; override != nil {
		if override.Guaranteed != nil {
			effective.Guaranteed = *override.Guaranteed
		}
		if override.Burstable != nil {
			effective.Burstable = *override.Burstable
		}
	}

	effective.QoSClass = DeriveQoS(effective.Guaranteed, effective.Burstable)
	return effective
}

// Allocatable is the part of a pool's budget that guarantees may be granted against:
// the raw budget less headroom. Headroom is withheld before any guarantee is counted,
// which is what leaves a fully reserved pool with enough connections to survive a
// failover or a rolling restart.
func Allocatable(backendConnections, headroomPercent int32) int32 {
	headroomPercent = max(min(headroomPercent, 100), 0)
	return int32(int64(backendConnections) * int64(100-headroomPercent) / 100)
}

// HeadroomPercent reports the headroom in force for a pool, falling back to the pool
// class's default and then to the CRD default.
func HeadroomPercent(pool *pgelasticv1alpha1.PgElasticPool, class *pgelasticv1alpha1.PgElasticClass) int32 {
	if pool.Spec.Capacity.HeadroomPercent != nil {
		return *pool.Spec.Capacity.HeadroomPercent
	}
	if class != nil && class.Spec.Defaults != nil && class.Spec.Defaults.HeadroomPercent != nil {
		return *class.Spec.Defaults.HeadroomPercent
	}
	return DefaultHeadroomPercent
}

// Ledger is the pool's reservation arithmetic at one instant. Reserved counts every
// tenant already admitted to the pool rather than only those already placed on an
// instance: a guarantee is a credit taken at admission, so a tenant that has been
// accepted but not yet bound is still holding capacity nobody else may be promised.
type Ledger struct {
	// BackendConnections is the pool's raw budget.
	BackendConnections int32
	// HeadroomPercent is the share withheld before guarantees are counted.
	HeadroomPercent int32
	// Allocatable is the budget guarantees are granted against.
	Allocatable int32
	// Reserved is the sum of effective guarantees over the pool's tenants.
	Reserved int32
	// Available is Allocatable less Reserved, floored at zero.
	Available int32
	// CommittedBurst is the sum of effective ceilings over the pool's tenants.
	CommittedBurst int32
	// Tenants is how many tenants contributed to the sums.
	Tenants int32
}

func (l *Ledger) add(effective Effective) {
	l.Reserved += effective.Guaranteed
	l.CommittedBurst += effective.Burstable
	l.Tenants++
	l.settle()
}

func (l *Ledger) settle() {
	l.Available = max(l.Allocatable-l.Reserved, 0)
}
