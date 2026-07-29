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
)

// Ports. The client port is the pool's Service port; the others are never published on it.
const (
	// DefaultClientPort is the PostgreSQL wire port the fleet listens on inside the pod.
	// The Service port is configurable; this is not, because the container port and the
	// listen address are the same decision and nothing gains from having two of them.
	DefaultClientPort int32 = 5432
	// DefaultMetricsPort serves /metrics, /healthz, /readyz and /configz.
	DefaultMetricsPort int32 = 9127
)

// Port names, which is how a Service targets a container port without knowing its number.
const (
	PortNameClient  = "postgres"
	PortNameMetrics = "metrics"
)

// ContainerName names the proxy container, and is what a template override merges onto.
const ContainerName = "proxy"

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
