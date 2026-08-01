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

package ownership

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// failingReader answers every Get with one error, which is what makes the mapping from read
// failures to verdicts testable at all.
type failingReader struct{ err error }

func (r failingReader) Get(
	_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption,
) error {
	return r.err
}

func (r failingReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return r.err
}

// A read failure that is not a genuine absence must never resolve to Unresolved.
//
// The whole safety argument for what the controllers do with Unresolved rests on this: an
// object under deletion has this operator's finalizers taken off it without its reclaim
// running, on the grounds that its parent is really gone and nothing will ever free it. A
// Forbidden from namespace-scoped RBAC, or a timeout, is not that - the parent may be alive
// and owned by somebody else - and treating either as absence would release finalizers on
// objects belonging to another operator every time the API server had a bad minute.
func TestReadFailuresNeverResolveToUnresolved(t *testing.T) {
	resource := schema.GroupResource{Group: pgelasticv1alpha1.SchemeGroupVersion.Group, Resource: "pgelasticclasses"}
	failures := map[string]error{
		"forbidden":     apierrors.NewForbidden(resource, "any", errors.New("nope")),
		"timeout":       apierrors.NewTimeoutError("the server timed out", 1),
		"unauthorized":  apierrors.NewUnauthorized("no token"),
		"server error":  apierrors.NewInternalError(errors.New("boom")),
		"unavailable":   apierrors.NewServiceUnavailable("try later"),
		"not a k8s err": errors.New("connection refused"),
	}

	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			resolver := Resolver{Reader: failingReader{err: failure}}
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "somewhere"},
				Spec: pgelasticv1alpha1.PgElasticPoolSpec{
					ClassRef: pgelasticv1alpha1.ClassReference{Name: "a-class"},
				},
			}

			verdict, err := resolver.Of(context.Background(), pool)

			if err == nil {
				t.Fatalf("a %s was swallowed; the caller cannot tell it apart from an answer", name)
			}
			if verdict == Unresolved {
				t.Fatalf("a %s resolved to Unresolved, so a live parent looks like a gone one", name)
			}
		})
	}
}

// The other half of the same invariant: a genuine absence really does mean Unresolved, so the
// deadlock this is all here to break stays broken.
func TestAbsentParentResolvesToUnresolved(t *testing.T) {
	resource := schema.GroupResource{Group: pgelasticv1alpha1.SchemeGroupVersion.Group, Resource: "pgelasticpools"}
	resolver := Resolver{Reader: failingReader{err: apierrors.NewNotFound(resource, "gone")}}

	for name, object := range map[string]client.Object{
		"tenant": &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "n"},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef: corev1.LocalObjectReference{Name: "a-pool"},
			},
		},
		"instance": &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: "n"},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				PoolRef: corev1.LocalObjectReference{Name: "a-pool"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			verdict, err := resolver.Of(context.Background(), object)

			if err != nil {
				t.Fatalf("an absent parent was reported as a failure: %v", err)
			}
			if verdict != Unresolved {
				t.Fatalf("an absent parent resolved to %v, so nothing would ever free the object", verdict)
			}
		})
	}
}
