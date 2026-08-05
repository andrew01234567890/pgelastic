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

// Keys in the backend credential Secret. Both halves of the SCRAM credential live here: the
// verifier is what PostgreSQL is given, the salted password is what the proxy proves it with.
// The plaintext is not among them - it exists only inside one reconcile and is never persisted.
const (
	backendVerifierKey       = "verifier"
	backendSaltedPasswordKey = "saltedPassword"
	backendSaltKey           = "salt"
	backendIterationsKey     = "iterations"
	backendGenerationKey     = "generation"
)

// backendCredentialSecretName is where a tenant's backend credential lives.
//
// Separate from spec.auth.credentialsSecretRef on purpose, and the separation is the whole
// trust boundary: that one is the tenant's, it authenticates a client to the proxy, and the
// tenant knows it. This one is the operator's, it authenticates the proxy to PostgreSQL, and
// the tenant must not be able to derive it from the other.
func backendCredentialSecretName(tenant string) string {
	return tenant + "-pgelastic-backend"
}

// backendCredential is the tenant's backend identity as the proxy needs it.
type backendCredential struct {
	Role           string
	SaltedPassword string
	Salt           string
	Iterations     int32
	Generation     int32
	// Verifier is handed to PostgreSQL and to nobody else.
	Verifier string
}

// ensureBackendCredential mints the tenant's backend credential once and returns it thereafter.
//
// Minted rather than derived from anything the tenant supplies, and re-read rather than
// re-minted: the credential has to be identical everywhere the tenant is provisioned, because a
// migration re-establishes it on the target from this Secret rather than copying pg_authid
// between instances. Re-minting on every reconcile would rotate it constantly and leave every
// pooled link holding a secret PostgreSQL no longer accepts.
func (r *PgTenantReconciler) ensureBackendCredential(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
	iterations int32,
) (backendCredential, error) {
	role := migration.BackendRoleName(tenant.Namespace, tenant.Name)
	key := client.ObjectKey{Namespace: tenant.Namespace, Name: backendCredentialSecretName(tenant.Name)}

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
		return backendCredential{}, fmt.Errorf("generating a backend password: %w", err)
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
	if err := controllerutil.SetControllerReference(tenant, desired, r.Scheme); err != nil {
		return backendCredential{}, err
	}
	// Created rather than applied, and an existing Secret is updated in place. A credential is
	// written once and read for ever afterwards, so there is no field-ownership contest to
	// settle - and a Secret that already exists here is one whose contents this reconcile has
	// already decided are unusable.
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return backendCredential{}, fmt.Errorf("writing the backend credential: %w", err)
		}
		existing := &corev1.Secret{}
		if err := r.Get(ctx, key, existing); err != nil {
			return backendCredential{}, err
		}
		existing.StringData = desired.StringData
		if err := r.Update(ctx, existing); err != nil {
			return backendCredential{}, fmt.Errorf("replacing the backend credential: %w", err)
		}
	}
	return credential, nil
}

// readBackendCredential returns what the Secret holds, if it holds a complete credential for
// the role currently derived. A Secret naming an older role is not reused: the role name is a
// function of the tenant's identity, so a mismatch means the Secret belongs to something else.
func readBackendCredential(secret *corev1.Secret, role string) (backendCredential, bool) {
	value := func(key string) string { return string(secret.Data[key]) }
	if value(backendSaltedPasswordKey) == "" || value(backendVerifierKey) == "" {
		return backendCredential{}, false
	}
	iterations, err := strconv.Atoi(value(backendIterationsKey))
	if err != nil || iterations <= 0 {
		return backendCredential{}, false
	}
	generation, err := strconv.Atoi(value(backendGenerationKey))
	if err != nil {
		return backendCredential{}, false
	}
	return backendCredential{
		Role:           role,
		SaltedPassword: value(backendSaltedPasswordKey),
		Salt:           value(backendSaltKey),
		Iterations:     int32(iterations),
		Generation:     int32(generation),
		Verifier:       value(backendVerifierKey),
	}, true
}

// defaultScramIterations matches the API default, so a pool that has not set one still derives
// a credential the proxy renders the same iteration count for.
const defaultScramIterations int32 = 4096

// scramIterationsOf is the pool's configured cost. It is part of every verifier rather than a
// tunable: a credential derived under one value authenticates against nothing derived under
// another, which is why it is read from the pool the tenant belongs to and not from a flag.
func scramIterationsOf(resolved resolution) int32 {
	if resolved.pool == nil || resolved.pool.Spec.Auth == nil ||
		resolved.pool.Spec.Auth.ScramIterations == nil ||
		*resolved.pool.Spec.Auth.ScramIterations <= 0 {
		return defaultScramIterations
	}
	return *resolved.pool.Spec.Auth.ScramIterations
}

// scramIterationsOfPool is the same cost, resolved for a caller that holds a tenant rather
// than a resolution. A pool that cannot be read falls back to the CRD default, because a
// credential is better derived under the documented cost than not derived at all - and the
// tenant's own credential took that same default in the same situation.
func scramIterationsOfPool(
	ctx context.Context,
	reader client.Reader,
	tenant *pgelasticv1alpha1.PgTenant,
) int32 {
	if tenant.Spec.PoolRef.Name == "" {
		return defaultScramIterations
	}
	pool := &pgelasticv1alpha1.PgElasticPool{}
	key := client.ObjectKey{Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := reader.Get(ctx, key, pool); err != nil {
		return defaultScramIterations
	}
	return scramIterationsOf(resolution{pool: pool})
}

// nextGeneration bumps past whatever the previous credential carried, so a re-mint is visible
// to the pool key and the links opened under the old secret become unreachable.
func nextGeneration(secret *corev1.Secret) int32 {
	previous, err := strconv.Atoi(string(secret.Data[backendGenerationKey]))
	if err != nil {
		return 1
	}
	return int32(previous) + 1
}

// backendCredentialFor reads a tenant's backend credential for the fleet's configuration.
//
// Reports absence rather than an error, because absence is an ordinary state: the tenant
// controller mints the credential on its own reconcile, and a pool reconcile that happens
// first should render the tenant without one rather than fail the whole fleet's document.
//
// A NotFound is that state. Anything else is not: an RBAC change, an expired token or an
// unreachable API server are all reasons the Secret could not be read, and reading them as
// "this tenant has no credential" would rewrite the fleet's document to drop the credential
// of every tenant at once. That is returned as an error and fails the reconcile instead.
//
// The role name comes back either way. It is a pure function of namespace and tenant, and it
// is the same value the tenant controller provisions the database owner as, so it is knowable
// without the Secret - which is what lets the proxy tell "no identity was published for this
// tenant" from "an identity was published and its credential is missing".
func (r *PgElasticPoolReconciler) backendCredentialFor(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
) (backendCredential, bool, error) {
	role := migration.BackendRoleName(tenant.Namespace, tenant.Name)
	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Namespace: tenant.Namespace, Name: backendCredentialSecretName(tenant.Name),
	}
	if err := r.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return backendCredential{Role: role}, false, nil
		}
		return backendCredential{}, false, fmt.Errorf(
			"reading the backend credential of PgTenant %q: %w", tenant.Name, err)
	}
	credential, ok := readBackendCredential(secret, role)
	if !ok {
		credential.Role = role
	}
	return credential, ok, nil
}
