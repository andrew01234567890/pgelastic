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

// Package proxy builds the Kubernetes objects a PgElasticPool's inline proxy fleet is made
// of, and renders the configuration document the fleet reads.
//
// The fleet is inline on the pool rather than a kind of its own because it holds the pool's
// reservation ledger and its tenants' credentials: sharing one fleet across pools would put
// a configuration blast radius and a CVE blast radius across a tenancy boundary.
//
// A Deployment rather than individually managed Pods, which is the opposite of the choice
// made for a PgInstance. The reason is the same in both cases — what the workload needs.
// An instance member owns storage and an identity, so ordering and recreation semantics
// have to belong to the operator. A proxy replica owns nothing: it is fungible, it holds no
// volume, and the only thing that must not happen to it is being restarted for a change it
// could have adopted. That last property is a property of the pod template, not of the
// controller, and is what Render's structural/dynamic split exists to give.
package proxy

import pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"

// Label keys. The selector is the one published in status.selector for the scale
// subresource, so it is part of the API rather than decoration.
const (
	// LabelPool ties every object back to its PgElasticPool.
	LabelPool = "pgelastic.io/pool"
	// LabelComponent distinguishes the proxy fleet from the pool's other objects.
	LabelComponent = "pgelastic.io/component"
)

// ComponentProxy is the component label value every object here carries.
const ComponentProxy = "proxy"

// Annotation keys.
const (
	// AnnotationConfigHash records the structural half of the configuration a pod was
	// created for. Only this half is hashed into the pod template: the routing table and
	// the per-tenant claims are adopted by a running replica, so hashing them here would
	// restart every replica — and drop every client on it — every time a tenant was added.
	AnnotationConfigHash = "pgelastic.io/proxyConfigHash"
	// AnnotationAppliedVersion is written by each replica onto its own Pod, naming the
	// configVersion it is serving. It is the operator's only ground truth for whether the
	// fleet has converged on a configuration or is half-way through picking it up.
	AnnotationAppliedVersion = "pgelastic.io/proxyConfigVersion"
)

// ConfigKey is the Secret key the rendered configuration document lives under.
const ConfigKey = "proxy.toml"

// Paths inside the proxy container.
const (
	// ConfigDir holds the projected configuration Secret.
	ConfigDir = "/etc/pgelastic/proxy"
	// ConfigPath is the document the proxy boots from. It is read from a projected volume
	// rather than fetched at start-up so that a replica can start while the API server
	// cannot be reached; the run-time re-read is a separate, optional path.
	ConfigPath = ConfigDir + "/" + ConfigKey
	// TLSDir holds the client-facing certificate and key.
	TLSDir = "/etc/pgelastic/tls"
	// BackendCADir holds the CA the backend certificates are verified against.
	BackendCADir = "/etc/pgelastic/backend-ca"
	// ControlTLSDir holds the control listener's own certificate and the CA the operator's
	// client certificate is verified against. Separate from TLSDir because the two answer
	// different questions: that one is what a tenant's client trusts, this one is what the
	// operator proves.
	ControlTLSDir = "/etc/pgelastic/control-tls"
)

// Ports. The client port is the pool's Service port; the others are never published on it.
const (
	// DefaultClientPort is the PostgreSQL wire port the fleet listens on inside the pod.
	// The Service port is configurable; this is not, because the container port and the
	// listen address are the same decision and nothing gains from having two of them.
	DefaultClientPort int32 = 5432
	// DefaultMetricsPort serves /metrics, /healthz, /readyz and /configz.
	DefaultMetricsPort int32 = 9127
	// DefaultControlPort serves the lease-bound cutover API: quiesce, drainStatus,
	// setRoute, resume, unquiesce. It is separate from the metrics port because these
	// endpoints change behaviour and /metrics does not, and it is reachable only over
	// mutual TLS — quiescing a tenant holds its clients' sockets open with nothing behind
	// them, so an unauthenticated caller could stall any tenant at will.
	DefaultControlPort int32 = 9128
)

// Port names, which is how a Service targets a container port without knowing its number.
const (
	PortNameClient  = "postgres"
	PortNameMetrics = "metrics"
	PortNameControl = "control"
)

// ContainerName names the proxy container, and is what a template override merges onto.
const ContainerName = "proxy"

// DefaultReplicas matches the spec.proxy.replicas CRD default and is applied to a pool
// whose fleet was written before the default existed.
const DefaultReplicas int32 = 3

// Replicas is the fleet size a pool declares.
//
// Exported because it is a capacity multiplier and not merely a pod count: every replica
// reads one configuration document carrying the undivided budget, so admission has to know
// how many of them there are.
func Replicas(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if proxy := pool.Spec.Proxy; proxy != nil && proxy.Replicas != nil {
		return *proxy.Replicas
	}
	return DefaultReplicas
}

// DeploymentName is the fleet.
func DeploymentName(pool string) string { return pool + "-proxy" }

// ServiceName is the endpoint every client of the pool connects to.
func ServiceName(pool string) string { return pool + "-proxy" }

// ConfigSecretName holds the rendered configuration document. A Secret rather than a
// ConfigMap because the document carries the backend role passwords and the tenants' SCRAM
// verifiers, and splitting it in two would mean two objects that can disagree.
func ConfigSecretName(pool string) string { return pool + "-proxy-config" }

// ServiceAccountName is the identity a replica re-reads its configuration and reports its
// applied version under.
func ServiceAccountName(pool string) string { return pool + "-proxy" }

// controlSuffix names every object of the control listener's PKI, which cert-manager
// issues per pool.
//
// A CA of its own rather than a cluster-wide issuer: its only job is to say which
// certificate belongs to the operator for this pool, so scoping it to the pool keeps one
// pool's compromised issuer from authenticating a cutover on another. cert-manager is
// already a hard dependency of the webhooks, so this adds an issuer rather than a
// dependency.
const controlSuffix = "-proxy-control"

// ControlSelfSignedIssuerName is the bootstrap issuer the control CA signs itself with.
func ControlSelfSignedIssuerName(pool string) string { return pool + controlSuffix + "-selfsign" }

// ControlCACertificateName is the per-pool CA both control certificates chain to.
func ControlCACertificateName(pool string) string { return pool + controlSuffix + "-ca" }

// ControlCASecretName holds that CA's certificate and key.
func ControlCASecretName(pool string) string { return pool + controlSuffix + "-ca" }

// ControlIssuerName signs the two leaf certificates from the pool's own CA.
func ControlIssuerName(pool string) string { return pool + controlSuffix }

// ControlServerCertificateName is the listener's own certificate.
func ControlServerCertificateName(pool string) string { return pool + controlSuffix + "-server" }

// ControlServerSecretName is mounted into every proxy replica.
func ControlServerSecretName(pool string) string { return pool + controlSuffix + "-server" }

// ControlClientCertificateName is the operator's identity for this pool.
func ControlClientCertificateName(pool string) string { return pool + controlSuffix + "-client" }

// ControlClientSecretName is the Secret the operator reads its client identity from. It is
// never mounted into a proxy replica: a replica holding the key that authenticates the
// operator to itself would make the whole check circular.
func ControlClientSecretName(pool string) string { return pool + controlSuffix + "-client" }

// ControlClientName is the DNS name the operator's certificate carries and the listener
// checks. It is a name rather than a mere "signed by our CA" test because the same CA also
// issues the listener's own certificate.
func ControlClientName(pool, namespace string) string {
	return ControlClientCertificateName(pool) + "." + namespace + ".svc"
}

// ControlServerName is the DNS name the listener's certificate carries, which is what the
// operator verifies when it dials a replica. A replica is reached by Pod IP, so the name is
// the fleet's Service name and the operator asks for it explicitly.
func ControlServerName(pool, namespace string) string {
	return ServiceName(pool) + "." + namespace + ".svc"
}

// RoleName is the fleet's permissions: read one Secret, annotate its own Pod.
func RoleName(pool string) string { return pool + "-proxy" }

// RoleBindingName binds RoleName to ServiceAccountName.
func RoleBindingName(pool string) string { return pool + "-proxy" }

// PDBName protects the fleet from voluntary disruption.
func PDBName(pool string) string { return pool + "-proxy" }

// Labels are the labels every object of a fleet carries.
func Labels(pool string) map[string]string {
	return map[string]string{
		LabelPool:                      pool,
		LabelComponent:                 ComponentProxy,
		"app.kubernetes.io/name":       "pgelastic",
		"app.kubernetes.io/component":  ComponentProxy,
		"app.kubernetes.io/instance":   pool,
		"app.kubernetes.io/managed-by": "pgelastic",
	}
}

// Selector is the label set the Deployment, the Service and the PDB all select on, and the
// string form published in status.selector for the scale subresource.
func Selector(pool string) map[string]string {
	return map[string]string{LabelPool: pool, LabelComponent: ComponentProxy}
}

// SelectorString is Selector in the form the scale subresource requires.
func SelectorString(pool string) string {
	return LabelPool + "=" + pool + "," + LabelComponent + "=" + ComponentProxy
}
