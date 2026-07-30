//go:build e2e

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

// Package certcheck answers one question three suites need: has cert-manager issued the
// certificate the operator makes every control call under.
//
// It exists because the answer was previously guessed. Each suite polled only for the Secret
// and, on timeout, blamed an issuance stall - a Certificate it had never looked at. On the
// cluster where this actually failed there was no Certificate at all, because cert-manager was
// not installed and the operator therefore rendered no control listener, silently. Five
// minutes of waiting reported the wrong cause of a fault that was knowable in one call.
package certcheck

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck // Ginkgo DSL
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck // Gomega DSL

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
)

const (
	group   = "cert-manager.io"
	version = "v1"
)

// Installed reports whether cert-manager's CRDs are registered on this cluster.
//
// Deliberately the same test the operator itself makes, so the suite and the operator cannot
// disagree about whether a control listener was going to be rendered.
func Installed(c client.Client) bool {
	_, err := c.RESTMapper().RESTMapping(schema.GroupKind{Group: group, Kind: "Certificate"}, version)
	return err == nil
}

// AwaitControlClientSecret blocks until cert-manager has issued the operator's client
// certificate for the pool, and fails usefully when it has not.
//
// Absence of cert-manager fails immediately rather than after the poll: it is a property of
// the cluster, not of the reconcile, so waiting for it to change is waiting for nothing. When
// cert-manager is present but the Secret is not, the Certificate's own conditions are what say
// why, so they are carried into the failure rather than left in the cluster.
func AwaitControlClientSecret(ctx context.Context, c client.Client, namespace, pool string) {
	GinkgoHelper()

	Expect(Installed(c)).To(BeTrue(),
		"cert-manager is not installed on this cluster, so the operator renders no control "+
			"listener at all and no cutover can reach the fleet's gate. Install it with "+
			"`make install-cert-manager E2E_CONTEXT=<context>`; CI does this as a prerequisite "+
			"of every suite that stands a fleet up.")

	secret := client.ObjectKey{Namespace: namespace, Name: proxyobjects.ControlClientSecretName(pool)}
	certificate := client.ObjectKey{
		Namespace: namespace, Name: proxyobjects.ControlClientCertificateName(pool),
	}

	Eventually(func() error {
		err := c.Get(ctx, secret, &corev1.Secret{})
		if err == nil {
			return nil
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("secret %q does not exist: %s", secret.Name, describe(ctx, c, certificate))
	}, "5m", "5s").Should(Succeed(),
		"cert-manager never issued the operator's control certificate, so no cutover could "+
			"reach the fleet's gate")
}

// describe renders a Certificate's conditions, or says that the object itself is missing -
// which is a different fault with a different cause, and the one that is easy to miss.
func describe(ctx context.Context, c client.Client, key client.ObjectKey) string {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: version, Kind: "Certificate"})
	if err := c.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Certificate %q does not exist either, so nothing has asked "+
				"cert-manager for it", key.Name)
		}
		return fmt.Sprintf("Certificate %q could not be read: %v", key.Name, err)
	}

	conditions, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil || !found || len(conditions) == 0 {
		return fmt.Sprintf("Certificate %q exists but reports no conditions yet", key.Name)
	}

	rendered := make([]string, 0, len(conditions))
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		rendered = append(rendered, fmt.Sprintf("%v=%v(%v: %v)",
			condition["type"], condition["status"], condition["reason"], condition["message"]))
	}
	return fmt.Sprintf("Certificate %q reports %s", key.Name, strings.Join(rendered, " | "))
}
