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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/scram"
)

// userBackendCredentialSecretName is where one login's backend credential lives.
//
// The suffix differs from a tenant's so that a PgTenant and a PgTenantUser of the same object
// name in one namespace cannot claim the same Secret - which they otherwise would, because
// both are named after their object.
func userBackendCredentialSecretName(login string) string {
	return login + "-pgelastic-user-backend"
}

// ensureUserBackendCredential mints one login's backend credential once and returns it
// thereafter.
//
// The same shape as a tenant's, and for the same reasons: minted rather than derived from
// anything the tenant supplies, and re-read rather than re-minted, because it has to be
// identical on every instance the login is provisioned on and re-minting on every reconcile
// would leave every pooled link holding a secret PostgreSQL no longer accepts.
//
// This is the operator's credential, not the tenant's. spec.credentialsSecretRef is the
// tenant's - it authenticates a *client to the proxy* and the tenant knows it. This one
// authenticates the *proxy to PostgreSQL*, and the tenant must not be able to derive it from
// the other, so the two are independent random values with nothing linking them.
func (r *PgTenantUserReconciler) ensureUserBackendCredential(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
	role string,
	iterations int32,
) (backendCredential, error) {
	key := client.ObjectKey{
		Namespace: user.Namespace,
		Name:      userBackendCredentialSecretName(user.Name),
	}

	secret := &corev1.Secret{}
	err := r.Get(ctx, key, secret)
	switch {
	case err == nil:
		if existing, ok := readBackendCredential(secret, role); ok {
			return existing, nil
		}
	case !apierrors.IsNotFound(err):
		return backendCredential{}, err
	}

	password := make([]byte, 32)
	if _, err := rand.Read(password); err != nil {
		return backendCredential{}, fmt.Errorf("generating a login's backend password: %w", err)
	}
	derived, err := scram.Derive(base64.RawStdEncoding.EncodeToString(password), iterations)
	if err != nil {
		return backendCredential{}, err
	}

	credential := backendCredential{
		Role:           role,
		SaltedPassword: derived.SaltedPassword,
		Salt:           derived.Salt,
		Iterations:     derived.Iterations,
		Generation:     nextGeneration(secret),
		Verifier:       derived.Verifier,
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			backendVerifierKey:       credential.Verifier,
			backendSaltedPasswordKey: credential.SaltedPassword,
			backendSaltKey:           credential.Salt,
			backendIterationsKey:     strconv.Itoa(int(credential.Iterations)),
			backendGenerationKey:     strconv.Itoa(int(credential.Generation)),
		},
	}
	// Owned by the login, so Kubernetes collects it and the finalizer does not have to.
	if err := controllerutil.SetControllerReference(user, desired, r.Scheme); err != nil {
		return backendCredential{}, err
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return backendCredential{}, fmt.Errorf("writing a login's backend credential: %w", err)
		}
		existing := &corev1.Secret{}
		if err := r.Get(ctx, key, existing); err != nil {
			return backendCredential{}, err
		}
		existing.StringData = desired.StringData
		if err := r.Update(ctx, existing); err != nil {
			return backendCredential{}, fmt.Errorf("replacing a login's backend credential: %w", err)
		}
	}
	return credential, nil
}

// userBackendCredentialFor reads a login's backend credential for the fleet's configuration.
//
// Absence is reported rather than raised, for the reason the tenant's equivalent gives: the
// login controller mints it on its own reconcile, and a pool reconcile that happens first
// should render that login without an identity rather than fail the whole fleet's document.
//
// The role is derived here rather than read from status.roleName, so a login whose status has
// not been written yet cannot cause the Secret to be rejected as belonging to something else.
func (r *PgElasticPoolReconciler) userBackendCredentialFor(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
) (backendCredential, bool) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Namespace: user.Namespace,
		Name:      userBackendCredentialSecretName(user.Name),
	}
	if err := r.Get(ctx, key, secret); err != nil {
		return backendCredential{}, false
	}
	role := migration.TenantUserRoleName(user.Namespace, user.Spec.TenantRef.Name, user.Name)
	return readBackendCredential(secret, role)
}
