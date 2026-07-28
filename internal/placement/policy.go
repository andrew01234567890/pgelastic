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

package placement

import (
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// CRD defaults for PgElasticPool.spec.placement, restated here so a pool written before a
// field existed resolves to the same numbers a freshly defaulted one does.
const (
	DefaultPackOnPercentile    = pgelasticv1alpha1.PercentileP95
	DefaultObservationWindow   = 168 * time.Hour
	DefaultMaxSkewTenants      = int32(15)
	DefaultPlacementStrategy   = pgelasticv1alpha1.PlacementBestFitDecreasing
	DefaultHotThresholdPercent = int32(15)
)

// QuantileFor maps the API's percentile enum onto the fraction the store is asked for.
// Anything unrecognised resolves to the 95th percentile, which is the documented default and
// the conservative choice: a lower percentile would understate demand.
func QuantileFor(percentile pgelasticv1alpha1.Percentile) float64 {
	switch percentile {
	case pgelasticv1alpha1.PercentileP50:
		return 0.50
	case pgelasticv1alpha1.PercentileP75:
		return 0.75
	case pgelasticv1alpha1.PercentileP90:
		return 0.90
	case pgelasticv1alpha1.PercentileP99:
		return 0.99
	default:
		return 0.95
	}
}

// PolicyFor resolves a pool's placement policy, applying the CRD defaults.
func PolicyFor(pool *pgelasticv1alpha1.PgElasticPool) Policy {
	policy := Policy{PackOn: DefaultPackOnPercentile, MaxSkewTenants: DefaultMaxSkewTenants}
	if pool == nil || pool.Spec.Placement == nil {
		return policy
	}
	if pool.Spec.Placement.PackOnPercentile != "" {
		policy.PackOn = pool.Spec.Placement.PackOnPercentile
	}
	if pool.Spec.Placement.MaxSkewTenants != nil {
		policy.MaxSkewTenants = *pool.Spec.Placement.MaxSkewTenants
	}
	return policy
}

// ObservationWindowFor resolves the trailing window the packing statistic is computed over.
func ObservationWindowFor(pool *pgelasticv1alpha1.PgElasticPool) time.Duration {
	if pool != nil && pool.Spec.Placement != nil && pool.Spec.Placement.ObservationWindow != nil {
		if window := pool.Spec.Placement.ObservationWindow.Duration; window > 0 {
			return window
		}
	}
	return DefaultObservationWindow
}

// AntiAffinityFor reads the values of the label keys a tenant declared. A declared key the
// tenant carries no value for is skipped rather than treated as the empty value, which would
// make every such tenant collide with every other.
func AntiAffinityFor(tenant *pgelasticv1alpha1.PgTenant) map[string]string {
	if tenant.Spec.Placement == nil || len(tenant.Spec.Placement.AntiAffinityLabelKeys) == 0 {
		return nil
	}
	values := make(map[string]string, len(tenant.Spec.Placement.AntiAffinityLabelKeys))
	for _, key := range tenant.Spec.Placement.AntiAffinityLabelKeys {
		if value, ok := tenant.Labels[key]; ok && value != "" {
			values[key] = value
		}
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

// PinnedInstanceFor reads a tenant's instance pin.
func PinnedInstanceFor(tenant *pgelasticv1alpha1.PgTenant) string {
	if tenant.Spec.Placement == nil || tenant.Spec.Placement.InstanceRef == nil {
		return ""
	}
	return tenant.Spec.Placement.InstanceRef.Name
}

// BoundInstanceFor reads where a tenant is currently placed.
func BoundInstanceFor(tenant *pgelasticv1alpha1.PgTenant) string {
	if tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
		return ""
	}
	return tenant.Status.Binding.InstanceRef.Name
}

// InstanceFrom turns a PgInstance's published capacity into a placement target.
//
// An instance that has not published its capacity yet contributes no headroom rather than
// unlimited headroom, and an instance that is not Ready is refused outright: capacity on
// paper is not capacity, and a re-cloning replica is the case that proves it.
func InstanceFrom(instance *pgelasticv1alpha1.PgInstance) Instance {
	target := Instance{
		Name:        instance.Name,
		Schedulable: true,
		Ready:       instance.Status.Phase == pgelasticv1alpha1.InstancePhaseReady,
	}
	if admission := instance.Spec.Admission; admission != nil {
		if admission.Schedulable != nil && !*admission.Schedulable {
			target.Schedulable = false
		}
		if admission.Cordoned != nil && *admission.Cordoned {
			target.Schedulable = false
		}
	}
	if instance.Spec.Drain != nil && instance.Spec.Drain.Mode != nil &&
		*instance.Spec.Drain.Mode == pgelasticv1alpha1.InstanceDrainRequested {
		target.Schedulable = false
	}
	if capacity := instance.Status.Capacity; capacity != nil {
		target.Capacity.Connections = capacity.Allocatable
	}
	if storage := instance.Status.Storage; storage != nil && storage.Allocated != nil {
		target.Capacity.StorageBytes = storage.Allocated.Value()
	}
	return target
}
