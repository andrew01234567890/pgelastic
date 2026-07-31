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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgbackrest"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

// AntiAffinityPolicy selects how strictly members are kept apart.
//
// Required is the only correct setting for a real deployment: with uniform three-replica
// HA, two members on one node makes the quorum a lie. Preferred exists for single-node
// development and CI clusters, where the alternative is not testing replication at all.
type AntiAffinityPolicy string

const (
	AntiAffinityRequired  AntiAffinityPolicy = "Required"
	AntiAffinityPreferred AntiAffinityPolicy = "Preferred"
)

// postgresUID is the uid and gid the postgres user has in the official image.
const postgresUID int64 = 999

// componentName labels every object of an instance and names the postgres container.
const componentName = "postgres"

// bootstrapContainer is the init container that brings the data directory into existence.
const bootstrapContainer = "bootstrap"

// defaultRetentionWindow is the window the API server defaults both halves of the retention
// policy to, repeated here for an object that never went through admission.
const defaultRetentionWindow = "30d"

// verbGet is spelled once so an RBAC rule cannot lose a verb to a typo.
const (
	verbGet    = "get"
	verbList   = "list"
	verbWatch  = "watch"
	verbUpdate = "update"
	verbPatch  = "patch"
)

// Builder renders the objects one PgInstance is made of.
type Builder struct {
	// Instance is the object being provisioned.
	Instance *pgelasticv1alpha1.PgInstance
	// PostgresImage carries PostgreSQL 18 and pgBackRest.
	PostgresImage string
	// AgentImage carries nothing but the instance manager and a shell, so the init
	// container that copies it into the pod does nothing but cp.
	AgentImage string
	// Capacity is the derived max_connections split.
	Capacity pgconf.Capacity
	// AntiAffinity selects the placement strictness.
	AntiAffinity AntiAffinityPolicy
	// ProxySources are the CIDRs the proxy dials from.
	ProxySources []string
	// PeerSources are the CIDRs members dial each other from.
	PeerSources []string
	// PrimaryEpoch is the fence token the members publish as a custom GUC.
	PrimaryEpoch int64
}

func (b Builder) name() string      { return b.Instance.Name }
func (b Builder) namespace() string { return b.Instance.Namespace }

// Labels are the labels every object of an instance carries.
func (b Builder) Labels() map[string]string {
	return map[string]string{
		LabelInstanceName:              b.name(),
		"app.kubernetes.io/name":       "pgelastic",
		"app.kubernetes.io/component":  componentName,
		"app.kubernetes.io/instance":   b.name(),
		"app.kubernetes.io/managed-by": "pgelastic",
	}
}

func (b Builder) memberLabels(serial int32) map[string]string {
	labels := b.Labels()
	labels[LabelNodeSerial] = strconv.Itoa(int(serial))
	return labels
}

func (b Builder) meta(name string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: b.namespace(), Labels: labels}
}

// Replicas is the configured member count.
func (b Builder) Replicas() int32 {
	if highAvailability := b.Instance.Spec.HighAvailability; highAvailability != nil &&
		highAvailability.Replicas != nil {
		return *highAvailability.Replicas
	}
	return 3
}

// PVC builds one claim of one group.
func (b Builder) PVC(serial int32, role string) *corev1.PersistentVolumeClaim {
	name, size, class := DataPVCName(b.name(), serial), b.Instance.Spec.Storage.Size,
		b.Instance.Spec.Storage.ClassName
	if role == PVCRoleWAL {
		name, size, class = WALPVCName(b.name(), serial), b.Instance.Spec.Storage.WALVolume.Size,
			b.Instance.Spec.Storage.WALVolume.ClassName
	}

	labels := b.memberLabels(serial)
	labels[LabelPVCRole] = role
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: b.meta(name, labels)}
	claim.Annotations = map[string]string{AnnotationPVCStatus: PVCStatusInitializing}
	claim.Spec = corev1.PersistentVolumeClaimSpec{
		// ReadWriteOncePod makes the double-mount split brain structurally impossible
		// rather than merely unlikely: no second Pod can mount the volume at all, however
		// confused the control plane becomes about which member is the primary.
		AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
		StorageClassName: class,
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: size},
		},
	}
	return claim
}

// ConfigMap holds the operator's decisions for the agent to render.
func (b Builder) ConfigMap(config AgentConfig) (*corev1.ConfigMap, error) {
	document, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, err
	}
	configMap := &corev1.ConfigMap{ObjectMeta: b.meta(ConfigMapName(b.name()), b.Labels())}
	configMap.Data = map[string]string{ConfigFileName: string(document)}
	return configMap, nil
}

// AgentConfig assembles what the agent needs from the spec and the derived capacity.
func (b Builder) AgentConfig() AgentConfig {
	spec := b.Instance.Spec
	userParameters, _ := pgconf.UserParameters(spec.Parameters)
	initdb := pgconf.InstanceConfig{
		Capacity:                 b.Capacity,
		SocketDirectory:          SocketDir,
		Port:                     PostgresPort,
		LogDirectory:             LogDir,
		LogFilename:              LogFileName,
		ArchiveCommand:           fmt.Sprintf("%s wal-archive --segment %%p --name %%f", AgentBinary),
		ArchiveTimeout:           ArchiveTimeout,
		WALVolumeBytes:           spec.Storage.WALVolume.Size.Value(),
		SynchronousCommit:        string(synchronousCommit(spec)),
		AutovacuumWorkerSlots:    16,
		ActiveReplicationOrigins: 64,
		SharedBuffers:            memoryFraction(spec.Resources, 4),
		EffectiveCacheSize:       memoryFraction(spec.Resources, 2),
		// The epoch is bound into the postmaster as a custom GUC so the proxy can read it
		// off any backend connection with a plain SHOW, where it cannot drift from the
		// running postmaster the way a value fetched from the API server can.
		PrimaryEpoch:   b.PrimaryEpoch,
		UserParameters: userParameters,
	}
	return AgentConfig{
		Instance:  b.name(),
		Namespace: b.namespace(),
		Replicas:  b.Replicas(),
		Postgres:  initdb,
		HBA: pgconf.HBAConfig{
			ProxySources:    b.ProxySources,
			PeerSources:     b.PeerSources,
			ReplicationRole: ReplicationRole,
			OpsRole:         OpsRole,
			RewindRole:      RewindRole,
		},
		Quorum:            quorum(spec),
		DataDurability:    string(dataDurability(spec)),
		Lease:             leaseTimings(spec),
		SwitchoverTimeout: metav1.Duration{Duration: SwitchoverTimeout(spec)},
		PeerService:       PeerServiceName(b.name()),
		CollationContract: CollationContract{
			Encoding:       "UTF8",
			LocaleProvider: "builtin",
			Locale:         "C.UTF-8",
			WALSegmentSize: 16 << 20,
			DataChecksums:  true,
		},
		Backup: repository(spec.Backup),
	}
}

// repository carries the WAL and base-backup destination down to the agent, or nothing when
// none is configured.
func repository(backup *pgelasticv1alpha1.InstanceBackup) *pgbackrest.Repository {
	if backup == nil {
		return nil
	}
	// The API server defaults the retention block, but an object built in a test or applied
	// by a client that skipped validation has not been through it.
	full, wal := defaultRetentionWindow, defaultRetentionWindow
	if retention := backup.Retention; retention != nil {
		if retention.Full != "" {
			full = retention.Full
		}
		if retention.WAL != "" {
			wal = retention.WAL
		}
	}
	return &pgbackrest.Repository{
		Path:          backup.ObjectStore.Path,
		EndpointURL:   backup.ObjectStore.EndpointURL,
		Region:        backup.ObjectStore.Region,
		RetentionFull: full,
		RetentionWAL:  wal,
	}
}

// backupCredentialsSecret names the Secret holding the object store's key pair, or nothing
// when no repository is configured.
func (b Builder) backupCredentialsSecret() string {
	if b.Instance.Spec.Backup == nil {
		return ""
	}
	return b.Instance.Spec.Backup.ObjectStore.CredentialsSecretRef.Name
}

// Pod builds one member, stamped with the roll signature it is being created for.
func (b Builder) Pod(serial int32, stamp RollStamp) *corev1.Pod {
	member := MemberName(b.name(), serial)
	labels := b.memberLabels(serial)
	pod := &corev1.Pod{ObjectMeta: b.meta(member, labels)}
	pod.Annotations = stamp.Annotations()
	pod.Spec = corev1.PodSpec{
		ServiceAccountName: ServiceAccountName(b.name()),
		Hostname:           member,
		Subdomain:          PeerServiceName(b.name()),
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:  ptr.To(postgresUID),
			RunAsGroup: ptr.To(postgresUID),
			FSGroup:    ptr.To(postgresUID),
		},
		TerminationGracePeriodSeconds: ptr.To(int64(60)),
		Affinity:                      b.affinity(),
		RestartPolicy:                 corev1.RestartPolicyAlways,
		InitContainers: []corev1.Container{
			{
				Name:            "install-agent",
				Image:           b.AgentImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				// The init container does exactly one thing. Anything more would be logic
				// that has to be kept in step with the agent it is installing.
				Command:      []string{"cp", SourceAgentBinary, AgentBinary},
				VolumeMounts: []corev1.VolumeMount{{Name: "agent", MountPath: AgentMountPath}},
			},
			{
				Name:            bootstrapContainer,
				Image:           b.PostgresImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{AgentBinary, "bootstrap"},
				Env:             b.env(serial),
				VolumeMounts:    b.mounts(),
			},
		},
		Containers: []corev1.Container{{
			Name:            componentName,
			Image:           b.PostgresImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{AgentBinary, "run"},
			Env:             b.env(serial),
			Ports: []corev1.ContainerPort{
				{Name: componentName, ContainerPort: PostgresPort},
				{Name: "status", ContainerPort: StatusPort},
			},
			VolumeMounts:   b.mounts(),
			StartupProbe:   probe("/startup", 1, 3, 120),
			ReadinessProbe: probe("/readiness", 2, 3, 0),
			LivenessProbe:  probe("/liveness", 10, 6, 0),
		}},
		Volumes: b.volumes(serial),
	}
	if b.Instance.Spec.Resources != nil {
		pod.Spec.Containers[0].Resources = *b.Instance.Spec.Resources
	}
	return pod
}

func probe(path string, period, failures, startupFailures int32) *corev1.Probe {
	threshold := failures
	if startupFailures > 0 {
		threshold = startupFailures
	}
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: path,
			Port: intstr.FromInt32(StatusPort),
		}},
		PeriodSeconds:    period,
		TimeoutSeconds:   5,
		FailureThreshold: threshold,
	}
}

func (b Builder) affinity() *corev1.Affinity {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{LabelInstanceName: b.name()}}
	term := corev1.PodAffinityTerm{LabelSelector: selector, TopologyKey: corev1.LabelHostname}
	if b.AntiAffinity == AntiAffinityPreferred {
		return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{Weight: 100, PodAffinityTerm: term},
			},
		}}
	}
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{term},
	}}
}

func (b Builder) env(serial int32) []corev1.EnvVar {
	secret := CredentialsSecretName(b.name())
	fromSecret := func(name, key string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
				Key:                  key,
			},
		}}
	}
	return []corev1.EnvVar{
		{Name: EnvInstance, Value: b.name()},
		{Name: EnvNamespace, Value: b.namespace()},
		{Name: EnvMember, Value: MemberName(b.name(), serial)},
		{Name: EnvSerial, Value: strconv.Itoa(int(serial))},
		{Name: EnvPodIP, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		}},
		{Name: EnvDataDir, Value: DataDir},
		{Name: EnvWALDir, Value: WALDir},
		{Name: EnvConfigFile, Value: ConfigMountPath + "/" + ConfigFileName},
		{Name: EnvSocketDir, Value: SocketDir},
		{Name: EnvLogDir, Value: LogDir},
		{Name: EnvStatusPort, Value: strconv.Itoa(int(StatusPort))},
		{Name: EnvPeerService, Value: PeerServiceName(b.name())},
		fromSecret(EnvReplPassword, SecretKeyReplicationPassword),
		fromSecret(EnvOpsPassword, SecretKeyOpsPassword),
		fromSecret(EnvRewindPassword, SecretKeyRewindPassword),
	}
}

func (b Builder) mounts() []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: "agent", MountPath: AgentMountPath},
		{Name: "pgdata", MountPath: DataMountPath},
		{Name: "pgwal", MountPath: WALMountPath},
		{Name: "socket", MountPath: SocketDir},
		{Name: "logs", MountPath: LogDir},
		{Name: "config", MountPath: ConfigMountPath, ReadOnly: true},
	}
	// The credentials are mounted rather than handed over as environment variables, so that
	// rotating them is a file the kubelet refreshes in place instead of a Pod restart that
	// drops every tenant connection on the instance.
	if b.backupCredentialsSecret() != "" {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      backupCredentialsVolume,
			MountPath: BackupCredentialsMountPath,
			ReadOnly:  true,
		})
	}
	return mounts
}

// backupCredentialsVolume is the Pod volume carrying the object store Secret.
const backupCredentialsVolume = "backup-credentials"

func (b Builder) volumes(serial int32) []corev1.Volume {
	claim := func(name, claimName string) corev1.Volume {
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
		}}
	}
	empty := func(name string) corev1.Volume {
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		}}
	}
	volumes := []corev1.Volume{
		claim("pgdata", DataPVCName(b.name(), serial)),
		claim("pgwal", WALPVCName(b.name(), serial)),
		empty("agent"),
		empty("socket"),
		empty("logs"),
		{Name: "config", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName(b.name())},
			},
		}},
	}
	if secret := b.backupCredentialsSecret(); secret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: backupCredentialsVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secret},
			},
		})
	}
	return volumes
}

// PeerService is headless and publishes not-ready addresses, because a member that is not
// ready is exactly the member the others need to reach to find out why.
func (b Builder) PeerService() *corev1.Service {
	service := &corev1.Service{ObjectMeta: b.meta(PeerServiceName(b.name()), b.Labels())}
	service.Spec = corev1.ServiceSpec{
		ClusterIP:                corev1.ClusterIPNone,
		PublishNotReadyAddresses: true,
		Selector:                 map[string]string{LabelInstanceName: b.name()},
		Ports:                    servicePorts(),
	}
	return service
}

// RoleService selects the members currently carrying a role label.
func (b Builder) RoleService(name string, role pgelasticv1alpha1.InstanceRole) *corev1.Service {
	service := &corev1.Service{ObjectMeta: b.meta(name, b.Labels())}
	service.Spec = corev1.ServiceSpec{
		Selector: map[string]string{LabelInstanceName: b.name(), LabelRole: string(role)},
		Ports:    servicePorts(),
	}
	return service
}

func servicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: componentName, Port: PostgresPort, TargetPort: intstr.FromInt32(PostgresPort)},
		{Name: "status", Port: StatusPort, TargetPort: intstr.FromInt32(StatusPort)},
	}
}

// ServiceAccount is the agent's identity.
func (b Builder) ServiceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: b.meta(ServiceAccountName(b.name()), b.Labels())}
}

// Role is the agent's permissions: report its own member status, and hold the promotion
// Lease. The Lease is held by the agent rather than by the operator so that a dead
// operator cannot cause an unnecessary failover.
func (b Builder) Role() *rbacv1.Role {
	role := &rbacv1.Role{ObjectMeta: b.meta(ServiceAccountName(b.name()), b.Labels())}
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups: []string{pgelasticv1alpha1.SchemeGroupVersion.Group},
			Resources: []string{"pginstances"},
			Verbs:     []string{verbGet, verbList, verbWatch},
		},
		{
			APIGroups: []string{pgelasticv1alpha1.SchemeGroupVersion.Group},
			Resources: []string{"pginstances/status"},
			Verbs:     []string{verbGet, verbUpdate, verbPatch},
		},
		{
			// The agent reads the backup it was elected for and writes what happened to it.
			// It claims one with an optimistic update rather than a patch, so two members
			// that both believe they were elected race on the resource version and exactly
			// one wins.
			APIGroups: []string{pgelasticv1alpha1.SchemeGroupVersion.Group},
			Resources: []string{"pgbackups"},
			Verbs:     []string{verbGet, verbList, verbWatch},
		},
		{
			APIGroups: []string{pgelasticv1alpha1.SchemeGroupVersion.Group},
			Resources: []string{"pgbackups/status"},
			Verbs:     []string{verbGet, verbUpdate, verbPatch},
		},
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{verbGet, verbList, verbWatch, "create", verbUpdate, verbPatch},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{verbGet, verbList, verbWatch},
		},
	}
	return role
}

// RoleBinding binds the agent's Role to its ServiceAccount.
func (b Builder) RoleBinding() *rbacv1.RoleBinding {
	binding := &rbacv1.RoleBinding{ObjectMeta: b.meta(ServiceAccountName(b.name()), b.Labels())}
	binding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName, Kind: "Role", Name: ServiceAccountName(b.name()),
	}
	binding.Subjects = []rbacv1.Subject{{
		Kind: "ServiceAccount", Name: ServiceAccountName(b.name()), Namespace: b.namespace(),
	}}
	return binding
}

// PodDisruptionBudgets are two, keyed on the role label the operator flips on promotion.
//
// The replica budget keeps at least one sync-capable standby alive, so an "ANY 1" commit
// never stalls because of a voluntary disruption. The primary budget makes a node drain
// hosting the primary block until a switchover happens, rather than taking the primary
// down and finding out afterwards whether failover worked.
func (b Builder) PodDisruptionBudgets() []*policyv1.PodDisruptionBudget {
	replicaBudget := &policyv1.PodDisruptionBudget{
		ObjectMeta: b.meta(ReplicaPDBName(b.name()), b.Labels()),
	}
	replicaBudget.Spec = policyv1.PodDisruptionBudgetSpec{
		MinAvailable: ptr.To(intstr.FromInt32(max(b.Replicas()-2, 0))),
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
			LabelInstanceName: b.name(),
			LabelRole:         string(pgelasticv1alpha1.InstanceRoleReplica),
		}},
	}

	primaryBudget := &policyv1.PodDisruptionBudget{
		ObjectMeta: b.meta(PrimaryPDBName(b.name()), b.Labels()),
	}
	primaryBudget.Spec = policyv1.PodDisruptionBudgetSpec{
		MinAvailable: ptr.To(intstr.FromInt32(1)),
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
			LabelInstanceName: b.name(),
			LabelRole:         string(pgelasticv1alpha1.InstanceRolePrimary),
		}},
	}
	return []*policyv1.PodDisruptionBudget{replicaBudget, primaryBudget}
}

func synchronousCommit(spec pgelasticv1alpha1.PgInstanceSpec) pgelasticv1alpha1.SynchronousCommitLevel {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.SynchronousCommit != nil {
		return *highAvailability.SynchronousCommit
	}
	return pgelasticv1alpha1.SynchronousCommitOn
}

func dataDurability(spec pgelasticv1alpha1.PgInstanceSpec) pgelasticv1alpha1.DataDurability {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.DataDurability != nil {
		return *highAvailability.DataDurability
	}
	return pgelasticv1alpha1.DataDurabilityRequired
}

// leaseTimings resolves the promotion Lease's four durations, falling back to the validated
// defaults for anything the spec leaves unset.
func leaseTimings(spec pgelasticv1alpha1.PgInstanceSpec) LeaseTimings {
	defaults := ha.DefaultLeaseConfig()
	timings := LeaseTimings{
		LeaseDuration:         metav1.Duration{Duration: defaults.LeaseDuration},
		RenewDeadline:         metav1.Duration{Duration: defaults.RenewDeadline},
		RetryPeriod:           metav1.Duration{Duration: defaults.RetryPeriod},
		ReleasedLeaseDuration: metav1.Duration{Duration: defaults.ReleasedLeaseDuration},
	}
	lease := leaseSpec(spec)
	if lease == nil {
		return timings
	}
	for target, configured := range map[*metav1.Duration]*metav1.Duration{
		&timings.LeaseDuration:         lease.LeaseDuration,
		&timings.RenewDeadline:         lease.RenewDeadline,
		&timings.RetryPeriod:           lease.RetryPeriod,
		&timings.ReleasedLeaseDuration: lease.ReleasedLeaseDuration,
	} {
		if configured != nil && configured.Duration > 0 {
			*target = *configured
		}
	}
	return timings
}

func leaseSpec(spec pgelasticv1alpha1.PgInstanceSpec) *pgelasticv1alpha1.PrimaryLeaseSpec {
	if spec.HighAvailability == nil {
		return nil
	}
	return spec.HighAvailability.PrimaryLease
}

// FailoverDelay is how long an unhealthy primary is debounced before a failover starts. It
// is deliberately non-zero: a spurious failover costs a timeline bump, a rewind or a full
// re-clone, a window at reduced redundancy during which failover is impossible, and a
// connection reset for every tenant on the instance.
func FailoverDelay(spec pgelasticv1alpha1.PgInstanceSpec) time.Duration {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.FailoverDelay != nil {
		return highAvailability.FailoverDelay.Duration
	}
	return 10 * time.Second
}

// SwitchoverTimeout bounds a planned, operator-initiated role change end to end. It is the
// deadline the clean stop is given before it is retried with the rest of the termination
// grace period, and never a licence to escalate to an immediate stop: a switchover is
// always followed by this member starting again from the data directory it stopped with.
func SwitchoverTimeout(spec pgelasticv1alpha1.PgInstanceSpec) time.Duration {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.SwitchoverTimeout != nil && highAvailability.SwitchoverTimeout.Duration > 0 {
		return highAvailability.SwitchoverTimeout.Duration
	}
	return 60 * time.Second
}

// FailoverQuorumEnabled reports whether promotion is gated on quorum evidence. Turning it
// off permits promoting a standby that cannot be proven to hold the last acknowledged
// commit, so it defaults on.
func FailoverQuorumEnabled(spec pgelasticv1alpha1.PgInstanceSpec) bool {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.FailoverQuorum != nil {
		return *highAvailability.FailoverQuorum
	}
	return true
}

func quorum(spec pgelasticv1alpha1.PgInstanceSpec) string {
	if highAvailability := spec.HighAvailability; highAvailability != nil &&
		highAvailability.Quorum != nil && *highAvailability.Quorum != "" {
		return *highAvailability.Quorum
	}
	return "ANY 1"
}

// ConcurrentDumps is the per-tenant logical backup concurrency the agent overhead is
// charged against.
func ConcurrentDumps(spec pgelasticv1alpha1.PgInstanceSpec) int32 {
	backup := spec.PerTenantLogicalBackup
	if backup == nil {
		return 4
	}
	if backup.Enabled != nil && !*backup.Enabled {
		return 0
	}
	if backup.MaxConcurrentDumps != nil {
		return *backup.MaxConcurrentDumps
	}
	return 4
}

// memoryFraction returns the given fraction of the pod's memory allocation as a PostgreSQL
// size literal, or the empty string when the pod declares no memory at all. Inventing a
// number in that case would be worse than leaving the boot default: the default is at
// least a value somebody chose.
func memoryFraction(resources *corev1.ResourceRequirements, divisor int64) string {
	if resources == nil {
		return ""
	}
	memory, ok := resources.Limits[corev1.ResourceMemory]
	if !ok {
		if memory, ok = resources.Requests[corev1.ResourceMemory]; !ok {
			return ""
		}
	}
	bytes := memory.Value() / divisor
	if bytes <= 0 {
		return ""
	}
	return strconv.FormatInt(bytes/(1<<20), 10) + "MB"
}

// StorageQuantity is a convenience for reporting allocated bytes in status.
func StorageQuantity(size resource.Quantity) *resource.Quantity {
	copied := size.DeepCopy()
	return &copied
}
