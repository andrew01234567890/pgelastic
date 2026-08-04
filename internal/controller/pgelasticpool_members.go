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
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// provisionMembers makes up the difference between the members a pool declares and the
// members it has, one member per pass.
//
// Membership is poolRef and only poolRef, which is the rule the ledger, the packer and the
// proxy renderer have all read since before anything could create a member. So an instance
// somebody wrote by hand is counted here exactly as one the pool made - every suite in this
// tree and the demo hand-write their members, and a provisioner that counted only what it
// owned would have doubled every one of them on its first pass.
//
// Adoption stops at counting. The pool does not stamp itself onto an instance it did not
// create, because an ownerReference is a deletion rule as much as a label: seizing a
// hand-written instance would mean deleting the pool deletes a machine somebody else made,
// and holds ~200 tenants' data.
//
// One per pass, and the count is recomputed from the API server every pass rather than
// remembered. The desired count is the only number written down; how many exist is a
// question with an answer, and asking it is what stops the two drifting apart.
func (r *PgElasticPoolReconciler) provisionMembers(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) (bool, error) {
	declared := declaredInstances(pool)
	// The class bounds the pool, not the other way round. spec.instances.replicas is written
	// by whoever owns the pool, and it now commands the operator to build primaries with
	// volumes attached - so the ceiling the platform team set on the class has to be the one
	// that decides, or the cap is advice.
	if ceiling := maxMembersOf(view.elasticClass); ceiling > 0 && declared > ceiling {
		declared = ceiling
	}
	if int32(len(view.instances)) >= declared {
		return false, nil
	}
	// Never on the first sighting of a spec. A pool and the members somebody wrote for it
	// arrive together - one `kubectl apply` over a directory, one Helm release, one test
	// fixture - and nothing orders them, so a pool reconciled between its own creation and
	// its members' looks exactly like a pool with none. Provisioning into that window makes
	// members nobody asked for, and they cannot be taken back: an instance is a machine with
	// a volume, and the pool has no way to know later that it was the one that jumped early.
	//
	// One pass of patience costs a status write that was going to happen anyway, and the
	// second pass reads the membership the applier actually intended.
	if pool.Status.ObservedGeneration != pool.Generation {
		return false, nil
	}

	// Every instance in the namespace, not only this pool's, because the name has to be free
	// against all of them and a name taken by another pool's member is still taken.
	occupied := &pgelasticv1alpha1.PgInstanceList{}
	if err := r.List(ctx, occupied, client.InNamespace(pool.Namespace)); err != nil {
		return false, err
	}
	taken := make(map[string]struct{}, len(occupied.Items))
	for i := range occupied.Items {
		taken[occupied.Items[i].Name] = struct{}{}
	}

	member := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pool.Namespace,
			Name:      freeMemberName(pool.Name, taken),
		},
		Spec: memberSpecFrom(pool),
	}
	if err := controllerutil.SetControllerReference(pool, member, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Create(ctx, member); err != nil {
		return false, client.IgnoreAlreadyExists(err)
	}
	logf.FromContext(ctx).Info("Provisioned a member instance",
		"instance", member.Name, "members", len(view.instances)+1, "declared", declared)
	return true, nil
}

// maxMembersOf is the class's ceiling on a pool's membership. Zero means the class does not
// say, which is only reachable for a pool whose class has no density block at all.
func maxMembersOf(class *pgelasticv1alpha1.PgElasticClass) int32 {
	if class == nil || class.Spec.Density == nil || class.Spec.Density.MaxInstancesPerPool == nil {
		return 0
	}
	return *class.Spec.Density.MaxInstancesPerPool
}

// freeMemberName is the lowest unused ordinal, so a pool that loses its second member of
// three makes that name again rather than a fourth. The ordinal is a name and nothing else:
// nothing reads it back, and a member adopted under some other name is as much a member.
func freeMemberName(pool string, taken map[string]struct{}) string {
	for ordinal := 1; ; ordinal++ {
		name := fmt.Sprintf("%s-%d", pool, ordinal)
		if _, exists := taken[name]; !exists {
			return name
		}
	}
}

// memberSpecFrom stamps the template onto a new member.
//
// It is a copy taken once, at creation, and never applied again. The template's own doc
// comment invites diffing a member against it, and that has to wait: Builder.ConfigHash()
// covers the whole rendered configuration, so one edit to template.parameters would rewrite
// every member's spec at once and roll the entire pool for a parameter PostgreSQL would have
// reloaded without a flicker.
func memberSpecFrom(pool *pgelasticv1alpha1.PgElasticPool) pgelasticv1alpha1.PgInstanceSpec {
	template := pool.Spec.Instances.Template
	spec := pgelasticv1alpha1.PgInstanceSpec{
		PoolRef:                corev1.LocalObjectReference{Name: pool.Name},
		Class:                  template.Class,
		PostgresVersion:        template.PostgresVersion,
		HighAvailability:       template.HighAvailability.DeepCopy(),
		Storage:                *template.Storage.DeepCopy(),
		Resources:              template.Resources.DeepCopy(),
		Backup:                 template.Backup.DeepCopy(),
		PerTenantLogicalBackup: template.PerTenantLogicalBackup.DeepCopy(),
	}
	if len(template.Parameters) > 0 {
		spec.Parameters = make(map[string]pgelasticv1alpha1.GUCValue, len(template.Parameters))
		maps.Copy(spec.Parameters, template.Parameters)
	}
	return spec
}
