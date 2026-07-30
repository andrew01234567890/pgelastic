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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/placement"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

// proxyFieldOwner is the field manager every proxy object is applied under. Server-side
// apply rather than get-then-update: the fleet's objects are rewritten on every pass from a
// value that is a pure function of the pool, so there is nothing to read back first, and a
// read-modify-write would spend reconciles losing races with itself.
const proxyFieldOwner = client.FieldOwner("pgelastic-proxy")

// tenantCredentialsKey is the Secret key a tenant's password is read from when the tenant
// names a credentials Secret.
const tenantCredentialsKey = "password"

// defaultTenantPriority matches the proxy's own default for a tenant nobody has ranked.
const defaultTenantPriority int32 = 1000

// The control listener's PKI. cert-manager is already a hard dependency of the webhooks,
// so this issues certificates rather than adding a dependency.
const (
	certManagerGroup   = "cert-manager.io"
	certManagerVersion = "v1"

	fieldSecret    = "secretName"
	fieldKey       = "privateKey"
	fieldIssuerRef = "issuerRef"
)

// controlPrivateKey is the key every control certificate is issued with. P-256 rather than
// RSA because these are handshake keys on a hot path and nothing about them needs to
// outlive the pool.
func controlPrivateKey() map[string]any {
	return map[string]any{"algorithm": "ECDSA", "size": int64(256)}
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;watch;create;update;patch;delete

// reconcileProxy brings the pool's inline proxy fleet into existence and reports what it
// found. It returns nil when the pool declares no fleet.
func (r *PgElasticPoolReconciler) reconcileProxy(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) (*pgelasticv1alpha1.ProxyStatus, error) {
	if pool.Spec.Proxy == nil {
		return nil, r.removeProxy(ctx, pool)
	}

	instances, err := r.proxyInstances(ctx, pool, view)
	if err != nil {
		return nil, err
	}
	users, err := r.proxyUsers(ctx, pool, view)
	if err != nil {
		return nil, err
	}

	config := proxy.Config{
		Pool:      pool,
		Instances: instances,
		Tenants:   r.proxyTenants(ctx, view, instances),
		Users:     users,
		Control:   r.certManagerInstalled(),
	}
	if tls := pool.Spec.Proxy.TLS; tls != nil {
		config.ClientTLS = tls.CertificateSecretRef != nil
		config.BackendCA = tls.BackendTLS != nil && tls.BackendTLS.CASecretRef != nil
	}

	builder := proxy.Builder{
		Pool:     pool,
		Image:    r.proxyImage(),
		Document: config.Render(),
	}
	if config.Control {
		builder.ControlTLSSecret = proxy.ControlServerSecretName(pool.Name)
	}
	if tls := pool.Spec.Proxy.TLS; tls != nil {
		if config.ClientTLS {
			builder.ClientTLSSecret = tls.CertificateSecretRef.Name
		}
		if config.BackendCA {
			builder.BackendCASecret = tls.BackendTLS.CASecretRef.Name
		}
	}

	if config.Control {
		if err := r.applyControlPKI(ctx, pool); err != nil {
			return nil, err
		}
	}
	if err := r.applyProxyObjects(ctx, pool, builder); err != nil {
		return nil, err
	}
	return r.proxyStatus(ctx, pool, builder, view)
}

// certManagerInstalled reports whether the cluster can issue the control listener's
// certificates.
//
// Probed rather than assumed. The listener is refused by the proxy without a client CA and
// a name to check it against, so a cluster with no cert-manager gets a fleet with no
// control listener — degraded, and running — rather than one that cannot start. The
// alternative, rendering the section and hoping, produces pods stuck mounting a Secret that
// will never exist.
func (r *PgElasticPoolReconciler) certManagerInstalled() bool {
	_, err := r.RESTMapper().RESTMapping(
		schema.GroupKind{Group: certManagerGroup, Kind: "Certificate"}, certManagerVersion)
	return err == nil
}

// applyProxyObjects writes the fleet, configuration first.
//
// The order is load-bearing in one place only: the Secret must exist before the Deployment,
// because a replica whose configuration volume has no content crash-loops rather than
// waiting. Everything after that is independent.
func (r *PgElasticPoolReconciler) applyProxyObjects(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	builder proxy.Builder,
) error {
	deployment, err := builder.Deployment()
	if err != nil {
		return err
	}
	objects := []struct {
		object client.Object
		gvk    schema.GroupVersionKind
	}{
		{builder.ConfigSecret(), corev1.SchemeGroupVersion.WithKind("Secret")},
		{builder.ServiceAccount(), corev1.SchemeGroupVersion.WithKind("ServiceAccount")},
		{builder.Role(), rbacv1.SchemeGroupVersion.WithKind("Role")},
		{builder.RoleBinding(), rbacv1.SchemeGroupVersion.WithKind("RoleBinding")},
		{builder.Service(), corev1.SchemeGroupVersion.WithKind("Service")},
		{
			builder.PodDisruptionBudget(),
			policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"),
		},
		{deployment, appsv1.SchemeGroupVersion.WithKind("Deployment")},
	}
	for _, entry := range objects {
		if err := r.applyProxyObject(ctx, pool, entry.object, entry.gvk); err != nil {
			return err
		}
	}
	return nil
}

// applyControlPKI issues the two identities the cutover API needs: the listener's own
// certificate, and the operator's client certificate.
//
// A per-pool CA rather than a cluster-wide issuer. The CA's only job is to say which
// certificate belongs to the operator for this pool, so scoping it to the pool means a
// compromised issuer cannot authenticate a cutover on somebody else's tenants. The two
// leaves carry different DNS names and different extended key usages, which is why "signed
// by our CA" is not on its own the identity check — the listener also names the caller it
// will accept.
func (r *PgElasticPoolReconciler) applyControlPKI(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) error {
	name := pool.Name
	selfSigned := proxy.ControlSelfSignedIssuerName(name)
	poolIssuer := proxy.ControlIssuerName(name)

	objects := []*unstructured.Unstructured{
		issuer(pool, selfSigned, map[string]any{"selfSigned": map[string]any{}}),
		certificate(pool, proxy.ControlCACertificateName(name), map[string]any{
			"isCA":         true,
			"commonName":   proxy.ControlCACertificateName(name),
			fieldSecret:    proxy.ControlCASecretName(name),
			fieldKey:       controlPrivateKey(),
			fieldIssuerRef: issuerRef(selfSigned),
		}),
		issuer(pool, poolIssuer, map[string]any{
			"ca": map[string]any{fieldSecret: proxy.ControlCASecretName(name)},
		}),
		certificate(pool, proxy.ControlServerCertificateName(name), map[string]any{
			fieldSecret:    proxy.ControlServerSecretName(name),
			"dnsNames":     []any{proxy.ControlServerName(name, pool.Namespace)},
			"usages":       []any{"server auth", "digital signature", "key encipherment"},
			fieldKey:       controlPrivateKey(),
			fieldIssuerRef: issuerRef(poolIssuer),
		}),
		certificate(pool, proxy.ControlClientCertificateName(name), map[string]any{
			fieldSecret:    proxy.ControlClientSecretName(name),
			"dnsNames":     []any{proxy.ControlClientName(name, pool.Namespace)},
			"usages":       []any{"client auth", "digital signature", "key encipherment"},
			fieldKey:       controlPrivateKey(),
			fieldIssuerRef: issuerRef(poolIssuer),
		}),
	}
	for _, object := range objects {
		gvk := object.GroupVersionKind()
		if err := r.applyProxyObject(ctx, pool, object, gvk); err != nil {
			return err
		}
	}
	return nil
}

func issuer(
	pool *pgelasticv1alpha1.PgElasticPool,
	name string,
	spec map[string]any,
) *unstructured.Unstructured {
	return certManagerObject(pool, "Issuer", name, spec)
}

func certificate(
	pool *pgelasticv1alpha1.PgElasticPool,
	name string,
	spec map[string]any,
) *unstructured.Unstructured {
	return certManagerObject(pool, "Certificate", name, spec)
}

func certManagerObject(
	pool *pgelasticv1alpha1.PgElasticPool,
	kind, name string,
	spec map[string]any,
) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{
		Group: certManagerGroup, Version: certManagerVersion, Kind: kind,
	})
	object.SetName(name)
	object.SetNamespace(pool.Namespace)
	object.SetLabels(proxy.Labels(pool.Name))
	object.Object["spec"] = spec
	return object
}

func issuerRef(name string) map[string]any {
	return map[string]any{"name": name, "kind": "Issuer", "group": certManagerGroup}
}

func (r *PgElasticPoolReconciler) applyProxyObject(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	object client.Object,
	gvk schema.GroupVersionKind,
) error {
	object.GetObjectKind().SetGroupVersionKind(gvk)
	if err := controllerutil.SetControllerReference(pool, object, r.Scheme); err != nil {
		return err
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return fmt.Errorf("encoding %s %s: %w", gvk.Kind, object.GetName(), err)
	}
	// Apply sends only the fields the operator sets, so a field another manager owns — a
	// replica count written through the scale subresource, a label an admission webhook
	// injects — survives instead of being reverted on the next pass.
	return r.Apply(ctx,
		client.ApplyConfigurationFromUnstructured(&unstructured.Unstructured{Object: content}),
		proxyFieldOwner, client.ForceOwnership)
}

// removeProxy tears the fleet down when the pool stops declaring one.
//
// Deleting only the two objects that serve traffic. The rest are owned by the pool and are
// collected with it; leaving them costs nothing and deleting them would mean a fleet
// re-declared a minute later had to re-issue its own RBAC before it could read anything.
func (r *PgElasticPoolReconciler) removeProxy(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) error {
	for _, object := range []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: proxy.DeploymentName(pool.Name), Namespace: pool.Namespace,
		}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: proxy.ServiceName(pool.Name), Namespace: pool.Namespace,
		}},
	} {
		if err := client.IgnoreNotFound(r.Delete(ctx, object)); err != nil {
			return err
		}
	}
	return nil
}

// proxyInstances resolves each member to an address and the credentials that member issued.
//
// A member whose credentials Secret has not been created yet is left out rather than
// rendered with an empty password: the fleet would authenticate against it, fail, and cache
// the failure, which is a worse outcome than not knowing about it for one reconcile.
func (r *PgElasticPoolReconciler) proxyInstances(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) ([]proxy.Instance, error) {
	instances := make([]proxy.Instance, 0, len(view.instances))
	for i := range view.instances {
		member := &view.instances[i]
		secret := &corev1.Secret{}
		key := client.ObjectKey{
			Namespace: pool.Namespace,
			Name:      provision.CredentialsSecretName(member.Name),
		}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		password := string(secret.Data[provision.SecretKeyOpsPassword])
		if password == "" {
			continue
		}
		entry := proxy.Instance{
			Name: member.Name,
			// The read-write Service, so a failover moves the proxy's backend leg the same
			// way it moves any other client's: the endpoint follows the primary label.
			Address: fmt.Sprintf("%s.%s.svc:%d",
				provision.PrimaryServiceName(member.Name), pool.Namespace,
				provision.PostgresPort),
			User:     provision.OpsRole,
			Password: password,
		}
		if capacity := member.Status.Capacity; capacity != nil {
			entry.BackendConnections = capacity.Allocatable
		}
		instances = append(instances, entry)
	}
	return instances, nil
}

// proxyTenants builds the routing table and the capacity claims.
//
// Keyed on the database name rather than on the PgTenant object's name, because the object
// name is a Kubernetes identifier that nothing on the PostgreSQL wire ever sends. A tenant
// that has not been placed yet contributes its claim but no route, so its capacity is
// already reserved while its connections still land on the default instance.
func (r *PgElasticPoolReconciler) proxyTenants(
	ctx context.Context, view *poolView, instances []proxy.Instance,
) []proxy.Tenant {
	known := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		known[instance.Name] = struct{}{}
	}

	tenants := make([]proxy.Tenant, 0, len(view.tenants))
	for i := range view.tenants {
		entry := &view.tenants[i]
		if entry.tenant.Spec.DatabaseName == "" {
			continue
		}
		bound := placement.BoundInstanceFor(entry.tenant)
		if _, ok := known[bound]; !ok {
			bound = ""
		}
		rendered := proxy.Tenant{
			Name:       entry.tenant.Spec.DatabaseName,
			Instance:   bound,
			Guaranteed: entry.effective.Guaranteed,
			Burstable:  entry.effective.Burstable,
			Weight:     entry.effective.Weight,
			Priority:   defaultTenantPriority,
		}
		// The identity this tenant's backend sessions run as. Left out when the tenant
		// controller has not minted it yet, mirroring how an instance whose Secret is absent
		// is left out rather than rendered with an empty password: the fleet then refuses
		// that one tenant for a reconcile rather than serving it as the control plane.
		if credential, ok := r.backendCredentialFor(ctx, entry.tenant); ok {
			rendered.BackendRole = credential.Role
			rendered.BackendSaltedPassword = credential.SaltedPassword
			rendered.BackendSalt = credential.Salt
			rendered.BackendIterations = credential.Iterations
			rendered.CredentialGeneration = credential.Generation
		}
		tenants = append(tenants, rendered)
	}
	return tenants
}

// proxyUsers collects the SCRAM identities the fleet authenticates clients against.
//
// A tenant with no credentials Secret contributes nothing, and a fleet with no users at all
// admits every client without a challenge — which is why the pool's TLS posture and this
// list are the two halves of one decision, and why an empty list is reported rather than
// quietly accepted.
func (r *PgElasticPoolReconciler) proxyUsers(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) ([]proxy.User, error) {
	users := make([]proxy.User, 0, len(view.tenants))
	for i := range view.tenants {
		tenant := view.tenants[i].tenant
		auth := tenant.Spec.Auth
		if auth == nil || auth.CredentialsSecretRef == nil {
			continue
		}
		secret := &corev1.Secret{}
		key := client.ObjectKey{Namespace: pool.Namespace, Name: auth.CredentialsSecretRef.Name}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		password := string(secret.Data[tenantCredentialsKey])
		if password == "" {
			continue
		}
		name := tenant.Spec.DatabaseName
		if tenant.Spec.Owner != nil && *tenant.Spec.Owner != "" {
			name = *tenant.Spec.Owner
		}
		// Bound to the same name proxyTenants routes on, because the proxy compares the two: a
		// login authenticated here may act as this tenant and no other.
		users = append(users, proxy.User{
			Name:     name,
			Tenant:   tenant.Spec.DatabaseName,
			Password: password,
		})
	}
	return users, nil
}

// proxyStatus reports what the fleet is actually doing.
//
// configVersion is the version every ready replica reports having applied, and is empty
// while any of them still reports something else. That is the distinction the field exists
// for: a fleet half-way through adopting a routing table is not one that has adopted it, and
// a controller that published the version it had merely written would be reporting its own
// intent back to itself.
//
// leasedConnections is the fleet-wide grant, and today it is a multiplication rather than a
// lease: every replica reads one configuration document carrying the undivided budget, so
// the pool has handed out replicas x allocatable. Reporting the arithmetic that is actually
// in force is the point — a zero there would read as "nothing is leased", which is the one
// thing that is certainly untrue.
func (r *PgElasticPoolReconciler) proxyStatus(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	builder proxy.Builder,
	view *poolView,
) (*pgelasticv1alpha1.ProxyStatus, error) {
	status := &pgelasticv1alpha1.ProxyStatus{
		Replicas:          builder.Replicas(),
		LeasedConnections: builder.Replicas() * view.ledger.Allocatable,
	}

	deployment := &appsv1.Deployment{}
	key := client.ObjectKey{Namespace: pool.Namespace, Name: proxy.DeploymentName(pool.Name)}
	switch err := r.Get(ctx, key, deployment); {
	case apierrors.IsNotFound(err):
		return status, nil
	case err != nil:
		return nil, err
	}
	status.Ready = deployment.Status.ReadyReplicas

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(pool.Namespace),
		client.MatchingLabels(proxy.Selector(pool.Name))); err != nil {
		return nil, err
	}
	converged := int32(0)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podReady(pod) {
			continue
		}
		if pod.Annotations[proxy.AnnotationAppliedVersion] == builder.Document.Version {
			converged++
		}
	}
	if converged > 0 && converged == status.Ready {
		status.ConfigVersion = builder.Document.Version
	}
	return status, nil
}

func (r *PgElasticPoolReconciler) proxyImage() string {
	if r.ProxyImage != "" {
		return r.ProxyImage
	}
	return envOrDefault(envProxyImage, "pgelastic/proxy:latest")
}
