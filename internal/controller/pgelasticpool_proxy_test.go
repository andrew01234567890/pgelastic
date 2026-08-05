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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

// A tenant whose PgWorkloadClass cannot be read has unknown capacity, not a cancelled
// existence. It used to be dropped from the pool view entirely, which took it out of the
// rendered routing table AND the login table - so deleting one PgWorkloadClass stopped every
// tenant that named it from being routed anywhere or authenticating at all, and the fix for
// that is somewhere the operator has to think of rather than something the fleet reports.
func TestATenantWhoseClassIsGoneIsStillServed(t *testing.T) {
	named := func(name string) *pgelasticv1alpha1.PgTenant {
		return &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec:       pgelasticv1alpha1.PgTenantSpec{DatabaseName: name},
		}
	}
	view := &poolView{
		tenants: []tenantView{{
			tenant:    named("resolved"),
			effective: policy.Effective{WorkloadClassName: "gold", Burstable: 8},
		}},
		unresolved: []*pgelasticv1alpha1.PgTenant{named("orphaned")},
	}

	served := view.allTenants()
	names := make([]string, 0, len(served))
	for _, tenant := range served {
		names = append(names, tenant.Name)
	}

	if len(names) != 2 {
		t.Fatalf("the pool serves %v; a tenant whose class went missing was dropped from "+
			"everything the document is rendered from", names)
	}
}

// The webhook that refuses a duplicate databaseName reads through the uncached reader and
// says in its own comment that two racing creates each see a cluster without the other. So a
// duplicate pair is a state the renderer meets, and the routing table has always dropped the
// loser. The login table did not - and a login without a route is not a tenant that cannot
// connect, it is a tenant that connects as somebody else: the database resolves to the
// winner's [[pool.tenants]] entry and backend_for assumes the winner's owner role.
//
// Driven through proxyUsers rather than through the holder helper, because a test that asks
// the helper the question the helper answers proves only that it was called.
func TestTheLoserOfADuplicateDatabaseNameGetsNoLogin(t *testing.T) {
	const namespace = "dup-login"
	scheme := runtime.NewScheme()
	if err := pgelasticv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	claimed := func(name string, age int) *pgelasticv1alpha1.PgTenant {
		return &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         namespace,
				CreationTimestamp: metav1.NewTime(time.Unix(int64(age), 0)),
			},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				DatabaseName: "orders",
				Owner:        ptr.To(name + "-owner"),
				Auth: &pgelasticv1alpha1.PgTenantAuth{
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: name + "-auth"},
				},
			},
		}
	}
	secretFor := func(name string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-auth", Namespace: namespace},
			Data:       map[string][]byte{tenantCredentialsKey: []byte("SCRAM-SHA-256$4096:x$y:z")},
		}
	}
	winner, loser := claimed("first", 100), claimed("second", 200)
	pool := &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: namespace},
	}
	reconciler := &PgElasticPoolReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pool, winner, loser, secretFor("first"), secretFor("second")).Build(),
		Scheme: scheme,
	}
	view := &poolView{tenants: []tenantView{
		{tenant: winner, effective: policy.Effective{Burstable: 8}},
		{tenant: loser, effective: policy.Effective{Burstable: 8}},
	}}

	users, err := reconciler.proxyUsers(context.Background(), pool, view)
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(users))
	for _, user := range users {
		names = append(names, user.Name)
	}
	if len(users) != 1 || users[0].Name != "first-owner" {
		t.Fatalf("the login table carries %v; the loser of the database name authenticates "+
			"and resolves to the winner's tenant entry, so its client is dialled into the "+
			"winner's database as the winner's owner", names)
	}
}

// The tenant a pool renders always names the role its backend sessions run as, whether or not
// the credential has been minted. Naming neither is the single-tenant shape on the far side,
// whose answer is the instance's own identity - the control plane's role.
func TestATenantWithNoCredentialStillNamesItsBackendRole(t *testing.T) {
	rendered := proxy.Config{Pool: &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "ns"},
		Spec: pgelasticv1alpha1.PgElasticPoolSpec{
			Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 100},
		},
	}, Tenants: []proxy.Tenant{{
		Name:        "orders",
		BackendRole: "pgt_orders_c0ffee",
		Burstable:   8,
	}}}.Render().TOML

	if !strings.Contains(rendered, `backendRole = "pgt_orders_c0ffee"`) {
		t.Fatalf("a tenant whose credential is not minted yet named no role, so the proxy "+
			"reads it as the single-tenant shape and dials the control plane's own "+
			"identity:\n%s", rendered)
	}
	if strings.Contains(rendered, "backendSaltedPassword") {
		t.Fatalf("a credential that does not exist was rendered anyway:\n%s", rendered)
	}
}
