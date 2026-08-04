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

package provision

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

func classNamed(t *testing.T, name string) pgconf.SizingClass {
	t.Helper()
	class, err := pgconf.LookupSizingClass(name)
	if err != nil {
		t.Fatalf("looking up %s: %v", name, err)
	}
	return class
}

// Nothing in this repository sets spec.resources - not one PgInstance, not one e2e suite.
// So "no resources declared" was never an edge case; it was the only case that had ever run,
// and it meant shared_buffers was omitted from the file entirely and every instance booted on
// PostgreSQL's 128 MB default. A gp-32 rated at 1200 connections and a dev-1 rated at 50 got
// byte-identical memory settings.
func TestAnInstanceWithNoResourcesIsSizedFromItsClass(t *testing.T) {
	small := sharedBuffersFor(nil, classNamed(t, "dev-1"))
	large := sharedBuffersFor(nil, classNamed(t, "gp-32"))

	if small == large {
		t.Errorf("dev-1 and gp-32 both get shared_buffers = %s, so the class buys nothing", small)
	}
	if small == "" || large == "" {
		t.Error("shared_buffers renders empty, which omits the line and boots on the 128MB default")
	}
}

// A limit is what the cgroup will actually kill the postmaster for exceeding, so it wins over
// a request; either wins over the class, which is only ever a fallback.
func TestDeclaredResourcesWinOverTheClassRating(t *testing.T) {
	class := classNamed(t, "dev-1")

	requested := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Gi")},
	}
	limited := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}

	if fromClass, fromRequests := sharedBuffersFor(nil, class), sharedBuffersFor(requested, class); fromClass == fromRequests {
		t.Errorf("a declared request changed nothing: both give %s", fromClass)
	}
	if requests, limits := sharedBuffersFor(requested, class), sharedBuffersFor(limited, class); requests == limits {
		t.Errorf("the limit did not win over the larger request: both give %s", limits)
	}
}

// The reserve is what the Pod runs besides the postmaster. Taking a quarter of the gross
// cgroup limit would be more aggressive than RDS is, by exactly the overhead AWS nets out
// and never publishes.
func TestTheNonPostgresReserveIsSubtractedBeforeTheFraction(t *testing.T) {
	class := classNamed(t, "gp-8")
	resources := &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Gi")},
	}

	gross := int64(32) << 30
	if got, want := instanceMemory(resources, class), gross-nonPostgresReserve; got != want {
		t.Errorf("usable memory = %d, want the limit less the reserve (%d)", got, want)
	}
}

// A dev-1's quarter-share is below what ~200 tenants' catalogs alone need warm: each database
// carries its own copy of every catalog and its own pg_internal.init.
func TestASmallClassStillGetsTheCatalogFloor(t *testing.T) {
	if got := sharedBuffersFor(nil, classNamed(t, "dev-1")); got != megabytes(minSharedBuffers) {
		t.Errorf("dev-1 shared_buffers = %s, want the floor %s", got, megabytes(minSharedBuffers))
	}
}

// The second pass, and the reason it stays even now that the webhook refuses these at
// admission: an object stored before a parameter became owned is admitted history, and it
// must not be able to poison a pod that reads it later. It used to be dropped in silence -
// the only caller of UserParameters discarded the list - so the value sat in the manifest,
// absent from the postmaster, with nothing anywhere saying the two disagreed.
func TestAnOwnedParameterIsDroppedAndNamed(t *testing.T) {
	builder := Builder{Instance: &pgelasticv1alpha1.PgInstance{
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			Parameters: map[string]pgelasticv1alpha1.GUCValue{
				"max_connections":  "5000",
				"random_page_cost": "1.1",
			},
		},
	}}

	dropped := builder.DroppedParameters()

	if !slices.Equal(dropped, []string{"max_connections"}) {
		t.Errorf("dropped = %v, want exactly [max_connections]", dropped)
	}
	if _, kept := pgconf.UserParameters(builder.Instance.Spec.Parameters); len(kept) != 1 {
		t.Errorf("the tenant's own parameter did not survive alongside the refusal")
	}
}
