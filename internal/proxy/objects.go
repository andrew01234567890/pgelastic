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

package proxy

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// proxyUID is the uid and gid the distroless nonroot user has.
const proxyUID int64 = 65532

// Builder renders the objects one pool's proxy fleet is made of.
type Builder struct {
	// Pool is the pool whose fleet this is.
	Pool *pgelasticv1alpha1.PgElasticPool
	// Image carries the proxy binary and nothing else.
	Image string
	// Document is the rendered configuration, already hashed.
	Document Document
	// ClientTLSSecret and BackendCASecret are mounted when named, and are the same decision
	// the rendered document was made under.
	ClientTLSSecret string
	BackendCASecret string
	// ControlTLSSecret carries the control listener's certificate and the CA the operator's
	// client certificate is checked against. Named exactly when Document renders a
	// [control] section, because the proxy refuses to boot with one and not the other.
	ControlTLSSecret string
}

func (b Builder) name() string      { return b.Pool.Name }
func (b Builder) namespace() string { return b.Pool.Namespace }

func (b Builder) spec() pgelasticv1alpha1.ProxySpec {
	if b.Pool.Spec.Proxy != nil {
		return *b.Pool.Spec.Proxy
	}
	return pgelasticv1alpha1.ProxySpec{}
}

func (b Builder) meta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: b.namespace(),
		Labels:    Labels(b.name()),
	}
}

// Replicas is the configured fleet size.
func (b Builder) Replicas() int32 { return Replicas(b.Pool) }

// ConfigSecret carries the rendered document.
func (b Builder) ConfigSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: b.meta(ConfigSecretName(b.name())),
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{ConfigKey: b.Document.TOML},
	}
}

// ServiceAccount is the identity a replica re-reads its configuration under.
func (b Builder) ServiceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: b.meta(ServiceAccountName(b.name()))}
}

// Role is the fleet's permissions, and they are the smallest set that works.
//
// The Secret rule is restricted by resourceName, which is why the fleet polls one object
// rather than watching for changes: RBAC cannot restrict a list or a watch by name, so a
// watch would mean read access to every Secret in the namespace — including every instance's
// bootstrap credentials — for a component that sits on the client's data path.
//
// The Pod rule is not restricted by name because a replica's Pod name is generated and
// cannot be enumerated when this Role is written. Each replica patches only its own Pod,
// named by the downward API; the grant is wider than the use, and that gap is the price of
// the operator having ground truth about which configuration the fleet is actually serving.
func (b Builder) Role() *rbacv1.Role {
	role := &rbacv1.Role{ObjectMeta: b.meta(RoleName(b.name()))}
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups:     []string{""},
			Resources:     []string{"secrets"},
			ResourceNames: []string{ConfigSecretName(b.name())},
			Verbs:         []string{"get"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "patch"},
		},
	}
	return role
}

// RoleBinding grants Role to ServiceAccount.
func (b Builder) RoleBinding() *rbacv1.RoleBinding {
	binding := &rbacv1.RoleBinding{ObjectMeta: b.meta(RoleBindingName(b.name()))}
	binding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     RoleName(b.name()),
	}
	binding.Subjects = []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      ServiceAccountName(b.name()),
		Namespace: b.namespace(),
	}}
	return binding
}

// Service is the one endpoint every client of the pool connects to.
//
// The metrics port is deliberately absent. A Service that carries both would let a client
// reach /configz and /metrics on the same address it sends SQL to, and the fleet's applied
// configuration is not a tenant-visible fact.
func (b Builder) Service() *corev1.Service {
	service := &corev1.Service{ObjectMeta: b.meta(ServiceName(b.name()))}
	spec := b.spec()
	serviceType := corev1.ServiceTypeClusterIP
	port := DefaultClientPort
	if spec.Service != nil {
		if spec.Service.Type != "" {
			serviceType = spec.Service.Type
		}
		if spec.Service.Port != nil {
			port = *spec.Service.Port
		}
	}
	service.Spec = corev1.ServiceSpec{
		Type:     serviceType,
		Selector: Selector(b.name()),
		Ports: []corev1.ServicePort{{
			Name:       PortNameClient,
			Port:       port,
			TargetPort: intstr.FromString(PortNameClient),
			Protocol:   corev1.ProtocolTCP,
		}},
	}
	return service
}

// PodDisruptionBudget bounds voluntary disruption of the fleet.
//
// maxUnavailable=1 when the pool declares neither field: the API cannot default it, because
// defaulting runs before CEL and would inject maxUnavailable into every object that set only
// minAvailable, which the mutual-exclusion rule would then reject.
func (b Builder) PodDisruptionBudget() *policyv1.PodDisruptionBudget {
	budget := &policyv1.PodDisruptionBudget{ObjectMeta: b.meta(PDBName(b.name()))}
	budget.Spec = policyv1.PodDisruptionBudgetSpec{
		Selector: &metav1.LabelSelector{MatchLabels: Selector(b.name())},
	}
	declared := b.spec().PodDisruptionBudget
	switch {
	case declared != nil && declared.MinAvailable != nil:
		budget.Spec.MinAvailable = declared.MinAvailable
	case declared != nil && declared.MaxUnavailable != nil:
		budget.Spec.MaxUnavailable = declared.MaxUnavailable
	default:
		budget.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(1))
	}
	return budget
}

// Deployment is the fleet.
func (b Builder) Deployment() (*appsv1.Deployment, error) {
	template, err := b.podTemplate()
	if err != nil {
		return nil, err
	}
	deployment := &appsv1.Deployment{ObjectMeta: b.meta(DeploymentName(b.name()))}
	deployment.Spec = appsv1.DeploymentSpec{
		Replicas: ptr.To(b.Replicas()),
		Selector: &metav1.LabelSelector{MatchLabels: Selector(b.name())},
		Strategy: b.strategy(),
		Template: *template,
	}
	return deployment, nil
}

// strategy rolls the fleet without surging.
//
// maxSurge stays at zero and the API says why: a surging rollout runs old and new replicas
// at once, each holding leased backend connections, so a surge transiently doubles the
// pool's backend usage against a budget that was sized for one fleet.
func (b Builder) strategy() appsv1.DeploymentStrategy {
	surge := intstr.FromInt32(0)
	unavailable := intstr.FromInt32(1)
	if drain := b.spec().Drain; drain != nil {
		if drain.MaxSurge != nil {
			surge = *drain.MaxSurge
		}
		if drain.MaxUnavailable != nil {
			unavailable = *drain.MaxUnavailable
		}
	}
	return appsv1.DeploymentStrategy{
		Type: appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{
			MaxSurge:       &surge,
			MaxUnavailable: &unavailable,
		},
	}
}

func (b Builder) podTemplate() (*corev1.PodTemplateSpec, error) {
	labels := Labels(b.name())
	annotations := map[string]string{AnnotationConfigHash: b.Document.StructuralHash}

	spec := b.spec()
	if template := spec.Template; template != nil && template.Metadata != nil {
		// The controller's own labels are applied last: the selector is derived from them,
		// and a template that could overwrite them would produce a Deployment whose pods it
		// does not select.
		merged := map[string]string{}
		maps.Copy(merged, template.Metadata.Labels)
		maps.Copy(merged, labels)
		labels = merged
		for key, value := range template.Metadata.Annotations {
			if _, owned := annotations[key]; !owned {
				annotations[key] = value
			}
		}
	}

	podSpec := b.podSpec()
	if template := spec.Template; template != nil && template.Spec != nil {
		merged, err := mergePodSpec(podSpec, template.Spec)
		if err != nil {
			return nil, err
		}
		podSpec = merged
	}

	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
		Spec:       *podSpec,
	}, nil
}

func (b Builder) podSpec() *corev1.PodSpec {
	spec := b.spec()
	grace := int64(150)
	if spec.TerminationGracePeriodSeconds != nil {
		grace = *spec.TerminationGracePeriodSeconds
	}
	return &corev1.PodSpec{
		ServiceAccountName:            ServiceAccountName(b.name()),
		TerminationGracePeriodSeconds: ptr.To(grace),
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(proxyUID),
			RunAsGroup:   ptr.To(proxyUID),
			FSGroup:      ptr.To(proxyUID),
		},
		Containers: []corev1.Container{b.container()},
		Volumes:    b.volumes(),
	}
}

func (b Builder) container() corev1.Container {
	spec := b.spec()
	container := corev1.Container{
		Name:  ContainerName,
		Image: b.Image,
		// The same policy an instance member carries, and for the same reason: pgelastic
		// pins its images, so re-pulling one on every restart buys nothing and makes a
		// registry outage able to stop a fleet that has the image on the node already. It
		// also has to be set explicitly, because Kubernetes defaults a :latest tag to Always
		// and a fleet whose image was side-loaded onto the node would then never start.
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            []string{"--config", ConfigPath},
		Ports:           b.ports(),
		Env:             b.env(),
		VolumeMounts:    b.volumeMounts(),
		ReadinessProbe:  b.readinessProbe(),
		Lifecycle:       b.lifecycle(),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	if spec.Resources != nil {
		container.Resources = *spec.Resources
	}
	if readiness := spec.Readiness; readiness != nil && readiness.EnableLivenessProbe != nil &&
		*readiness.EnableLivenessProbe {
		container.LivenessProbe = b.livenessProbe()
	}
	return container
}

// ports declares the client port, the metrics port, and — only when the control listener
// has certificates to serve it with — the cutover port. Declaring the control port on a
// replica that is not listening on it would leave the operator dialling a socket nothing
// answers, which is indistinguishable from a hung proxy.
func (b Builder) ports() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{Name: PortNameClient, ContainerPort: DefaultClientPort, Protocol: corev1.ProtocolTCP},
		{Name: PortNameMetrics, ContainerPort: metricsPort(b.Pool), Protocol: corev1.ProtocolTCP},
	}
	if b.ControlTLSSecret != "" {
		ports = append(ports, corev1.ContainerPort{
			Name:          PortNameControl,
			ContainerPort: DefaultControlPort,
			Protocol:      corev1.ProtocolTCP,
		})
	}
	return ports
}

// env carries the three facts the document cannot: how many runtime workers to start, which
// Pod this replica is, and what shape to write logs in.
//
// The log format is here rather than in the document for a reason that is not about
// convenience: the subscriber is installed before the document is read, so a format that
// travelled in the document would arrive after every line it was meant to shape.
//
// TOKIO_WORKER_THREADS rather than a count derived from the visible CPUs: a pod that spawns
// one worker per host core under a CPU limit spends its quota on CFS throttling.
//
// GOMAXPROCS carries the same number for the same reason, and is set unconditionally rather
// than per image. Both spellings being present means a proxy built on either runtime gets the
// worker count the spec asked for, so swapping the image cannot silently change the
// concurrency the pool was sized against.
func (b Builder) env() []corev1.EnvVar {
	workers := int32(2)
	if declared := b.spec().Workers; declared != nil {
		workers = *declared
	}
	env := []corev1.EnvVar{
		{Name: "TOKIO_WORKER_THREADS", Value: fmt.Sprintf("%d", workers)},
		{Name: "GOMAXPROCS", Value: fmt.Sprintf("%d", workers)},
		{
			Name: "PGELASTIC_POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
	}
	// Omitted entirely for a pool with no observability block, rather than set to the
	// default. Adding it unconditionally would roll every proxy fleet in the estate - and a
	// proxy roll drops client sessions - to hand the process a value it already picks.
	if observability := b.Pool.Spec.Observability; observability != nil && observability.LogFormat != "" {
		env = append(env, corev1.EnvVar{Name: EnvLogFormat, Value: observability.LogFormat})
	}
	return env
}

// readinessProbe reads admin state over HTTP and never opens a bare TCP connection.
//
// The distinction is the whole point of the setting: the listener is bound before the fleet
// can serve anything, so a connect-and-close probe would report every replica ready from the
// instant its socket existed, including one whose configuration was refused.
func (b Builder) readinessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intstr.FromString(PortNameMetrics),
			},
		},
		InitialDelaySeconds: 1,
		PeriodSeconds:       2,
		TimeoutSeconds:      2,
		FailureThreshold:    3,
	}
}

// livenessProbe is off unless the pool asks for it, and asks for a weaker fact than
// readiness does: a restart drops every client on the replica, which is worse than almost
// anything a liveness probe can detect.
func (b Builder) livenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromString(PortNameMetrics),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      2,
		FailureThreshold:    6,
	}
}

// lifecycle holds the pod in Terminating without draining for long enough that EndpointSlice
// removal has propagated. Without it, clients keep arriving at a replica that has already
// begun to drain and are dropped by it.
func (b Builder) lifecycle() *corev1.Lifecycle {
	delay := 20 * time.Second
	if drain := b.spec().Drain; drain != nil && drain.PreStopDelay != nil {
		delay = drain.PreStopDelay.Duration
	}
	if delay <= 0 {
		return nil
	}
	return &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Sleep: &corev1.SleepAction{
				Seconds: int64((delay + time.Second - 1) / time.Second),
			},
		},
	}
}

func (b Builder) volumes() []corev1.Volume {
	volumes := []corev1.Volume{{
		Name: "config",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  ConfigSecretName(b.name()),
				DefaultMode: ptr.To(int32(0o400)),
			},
		},
	}}
	if b.ClientTLSSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "client-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  b.ClientTLSSecret,
					DefaultMode: ptr.To(int32(0o400)),
				},
			},
		})
	}
	if b.BackendCASecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "backend-ca",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  b.BackendCASecret,
					DefaultMode: ptr.To(int32(0o400)),
				},
			},
		})
	}
	if b.ControlTLSSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "control-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  b.ControlTLSSecret,
					DefaultMode: ptr.To(int32(0o400)),
				},
			},
		})
	}
	return volumes
}

func (b Builder) volumeMounts() []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{{Name: "config", MountPath: ConfigDir, ReadOnly: true}}
	if b.ClientTLSSecret != "" {
		mounts = append(mounts,
			corev1.VolumeMount{Name: "client-tls", MountPath: TLSDir, ReadOnly: true})
	}
	if b.BackendCASecret != "" {
		mounts = append(mounts,
			corev1.VolumeMount{Name: "backend-ca", MountPath: BackendCADir, ReadOnly: true})
	}
	if b.ControlTLSSecret != "" {
		mounts = append(mounts,
			corev1.VolumeMount{Name: "control-tls", MountPath: ControlTLSDir, ReadOnly: true})
	}
	return mounts
}

// mergePodSpec applies the pool's template over the generated pod spec.
//
// A strategic merge rather than a plain overwrite, so that a template naming one container
// by ContainerName adds an env var or a volume mount to the generated proxy container
// instead of replacing it wholesale — which is what makes the escape hatch usable for the
// small additions it exists for.
func mergePodSpec(generated *corev1.PodSpec, override *corev1.PodSpec) (*corev1.PodSpec, error) {
	original, err := json.Marshal(generated)
	if err != nil {
		return nil, fmt.Errorf("encoding the generated proxy pod spec: %w", err)
	}
	patch, err := marshalOverride(override)
	if err != nil {
		return nil, err
	}
	merged, err := strategicpatch.StrategicMergePatch(original, patch, corev1.PodSpec{})
	if err != nil {
		return nil, fmt.Errorf("merging spec.proxy.template.spec: %w", err)
	}
	result := &corev1.PodSpec{}
	if err := json.Unmarshal(merged, result); err != nil {
		return nil, fmt.Errorf("decoding the merged proxy pod spec: %w", err)
	}
	return result, nil
}

// marshalOverride encodes the template's pod spec with its explicit nulls removed.
//
// PodSpec.Containers has no omitempty, so a template that overrides only nodeSelector still
// marshals to {"containers":null,...} — and a strategic merge reads an explicit null as
// "delete this field", which would produce a Deployment with no containers at all. A typed
// override cannot express a deletion in the first place, so dropping every null is the
// faithful reading of what the pool asked for.
func marshalOverride(override *corev1.PodSpec) ([]byte, error) {
	encoded, err := json.Marshal(override)
	if err != nil {
		return nil, fmt.Errorf("encoding spec.proxy.template.spec: %w", err)
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, fmt.Errorf("decoding spec.proxy.template.spec: %w", err)
	}
	patch, err := json.Marshal(stripNulls(decoded))
	if err != nil {
		return nil, fmt.Errorf("encoding spec.proxy.template.spec: %w", err)
	}
	return patch, nil
}

func stripNulls(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cleaned := make(map[string]any, len(typed))
		for key, entry := range typed {
			if entry == nil {
				continue
			}
			cleaned[key] = stripNulls(entry)
		}
		return cleaned
	case []any:
		cleaned := make([]any, 0, len(typed))
		for _, entry := range typed {
			cleaned = append(cleaned, stripNulls(entry))
		}
		return cleaned
	default:
		return value
	}
}
