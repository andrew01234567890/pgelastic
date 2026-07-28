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

// Package index registers the field indexes the pgelastic controllers select on.
// Indexes live on the manager's informer cache only, so code that must read live —
// the validating webhook — filters in Go instead.
package index

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	// TenantByPool indexes PgTenant on the pool it claims capacity from.
	TenantByPool = "spec.poolRef.name"
	// TenantByWorkloadClass indexes PgTenant on the workload class it names outright.
	// A tenant that inherits its class from a pool or cluster default is absent from
	// this index, because a field index cannot follow a reference.
	TenantByWorkloadClass = "spec.workloadClassName"
	// PoolByElasticClass indexes PgElasticPool on the policy class it is bound to.
	PoolByElasticClass = "spec.classRef.name"
	// PoolByDefaultWorkloadClass indexes PgElasticPool on the workload class its
	// tenants inherit when they name none.
	PoolByDefaultWorkloadClass = "spec.admission.defaultWorkloadClassName"
)

// Setup registers every pgelastic field index on a cache's indexer.
func Setup(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &pgelasticv1alpha1.PgTenant{}, TenantByPool,
		func(object client.Object) []string {
			tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
			if !ok || tenant.Spec.PoolRef.Name == "" {
				return nil
			}
			return []string{tenant.Spec.PoolRef.Name}
		}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &pgelasticv1alpha1.PgTenant{}, TenantByWorkloadClass,
		func(object client.Object) []string {
			tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
			if !ok || tenant.Spec.WorkloadClassName == nil || *tenant.Spec.WorkloadClassName == "" {
				return nil
			}
			return []string{*tenant.Spec.WorkloadClassName}
		}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &pgelasticv1alpha1.PgElasticPool{}, PoolByElasticClass,
		func(object client.Object) []string {
			pool, ok := object.(*pgelasticv1alpha1.PgElasticPool)
			if !ok || pool.Spec.ClassRef.Name == "" {
				return nil
			}
			return []string{pool.Spec.ClassRef.Name}
		}); err != nil {
		return err
	}

	return indexer.IndexField(ctx, &pgelasticv1alpha1.PgElasticPool{}, PoolByDefaultWorkloadClass,
		func(object client.Object) []string {
			pool, ok := object.(*pgelasticv1alpha1.PgElasticPool)
			if !ok || pool.Spec.Admission == nil || pool.Spec.Admission.DefaultWorkloadClassName == "" {
				return nil
			}
			return []string{pool.Spec.Admission.DefaultWorkloadClassName}
		})
}
