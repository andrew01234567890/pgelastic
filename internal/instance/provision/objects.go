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
	"strings"
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

// archiveCommand hands a finished segment to the agent, which pushes it with pgBackRest.
//
// The trailing comment is load-bearing, however much it does not look it. `pgbackrest backup`
// reads archive_command out of the running postmaster and refuses to start unless the string
// contains "pgbackrest" - it has no other way to satisfy itself that WAL is reaching its
// repository. Ours names the agent, which shells out to pgbackrest a moment later, so the
// check is looking for the wrong thing and every base backup failed with
// "archive_command ... must contain pgbackrest" before this comment existed.
//
// The alternative was --no-archive-check on every backup, which switches off the same option's
// other half: the wait that confirms the WAL spanning the backup actually arrived in the
// repository. That half is the one that makes a base backup restorable, so it stays.
func archiveCommand() string {
	return fmt.Sprintf("%s wal-archive --segment %%p --name %%f # runs pgbackrest archive-push",
		AgentBinary)
}

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
	// SizingClass is the shape the instance was sold as. It is what the memory derivation
	// falls back to when spec.resources says nothing, which is every PgInstance in this
	// repository.
	SizingClass pgconf.SizingClass
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
		Major:                    majorOf(spec.PostgresVersion),
		Capacity:                 b.Capacity,
		SocketDirectory:          SocketDir,
		Port:                     PostgresPort,
		LogDirectory:             LogDir,
		LogFilename:              LogFileName,
		ArchiveCommand:           archiveCommand(),
		ArchiveTimeout:           ArchiveTimeout,
		WALVolumeBytes:           spec.Storage.WALVolume.Size.Value(),
		SynchronousCommit:        string(synchronousCommit(spec)),
		AutovacuumWorkerSlots:    16,
		ActiveReplicationOrigins: 64,
		SharedBuffers:            sharedBuffersFor(spec.Resources, b.SizingClass),
		EffectiveCacheSize:       effectiveCacheSizeFor(spec.Resources, b.SizingClass),
		ParallelWorkers:          pgconf.ParallelWorkersForCPU(instanceCPUMillis(spec.Resources, b.SizingClass)),
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
		Backup:     repository(spec.Backup),
		Restore:    restoreRequest(spec.Restore),
		Recovering: spec.Restore != nil,
	}
}

// restoreRequest carries the recovery instruction down to the agent.
//
// pgBackRest's spelling of the target type is lower case and differs from the API's field
// names, and translating here rather than in the agent keeps the agent free of any opinion
// about what an operator wrote.
func restoreRequest(restore *pgelasticv1alpha1.InstanceRestore) *RestoreRequest {
	if restore == nil {
		return nil
	}
	request := &RestoreRequest{
		Stanza:                 restore.Stanza,
		BackupID:               restore.BackupID,
		EnforcedParameterFloor: restore.EnforcedParameterFloor,
	}
	target := restore.Target
	if target == nil {
		return request
	}
	request.Timeline = target.Timeline
	request.Exclusive = target.Exclusive != nil && *target.Exclusive
	switch {
	case target.Time != "":
		request.TargetType, request.TargetValue = "time", target.Time
	case target.LSN != "":
		request.TargetType, request.TargetValue = "lsn", target.LSN
	case target.Name != "":
		request.TargetType, request.TargetValue = "name", target.Name
	case target.XID != "":
		request.TargetType, request.TargetValue = "xid", target.XID
	case target.Immediate != nil && *target.Immediate:
		// immediate takes no value: it stops as soon as the backup is consistent, which is
		// a property of the backup rather than a moment somebody chose.
		request.TargetType = "immediate"
	}
	return request
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
// nonPostgresReserve is what the Pod runs besides the postmaster: the instance agent, the
// physical-backup shim and the syslogger.
//
// It is subtracted by name rather than absorbed into a smaller fraction. A Kubernetes memory
// limit is a cgroup ceiling - gross, and a hard OOM-kill boundary - whereas the 25% that RDS
// applies is a fraction of a figure AWS has already netted the OS and its own agents out of.
// Taking 25% of the gross number is therefore *more* aggressive than RDS is, by exactly the
// overhead they never publish. Zalando solves the same problem by quietly using a fifth
// rather than a quarter inside a container; naming the subtraction instead means somebody can
// audit it against real pod RSS and argue with the number.
const nonPostgresReserve = int64(512) << 20

// minSharedBuffers is a floor rather than a fraction. Each of ~200 tenant databases carries
// its own copy of every catalog and its own pg_internal.init - at least 269 relations before
// a single tenant object exists - and none of that is optional. A dev-1's quarter-share would
// otherwise land below what the catalogs alone need warm.
const minSharedBuffers = int64(512) << 20

// maxSharedBuffers caps the quarter-share. DropDatabaseBuffers scans the entire buffer pool
// on every DROP DATABASE - an unconditional walk of NBuffers - so on a pool with tenant churn
// the cost of a larger cache is paid again on every tenant deletion.
const maxSharedBuffers = int64(16) << 30

// instanceMemory is how much memory the postmaster may plan around, and where the figure
// came from.
//
// Precedence is limits, then requests, then the class rating. There is no fourth step and no
// zero case: spec.resources is unset on every PgInstance in this repository, so "no resources
// declared" is not an edge case, it is the only case that has ever run - and it currently
// means shared_buffers is omitted entirely and PostgreSQL boots on its 128 MB default
// whether the class sold 50 connections or 1200.
func instanceMemory(resources *corev1.ResourceRequirements, class pgconf.SizingClass) int64 {
	declared := int64(0)
	if resources != nil {
		if memory, ok := resources.Limits[corev1.ResourceMemory]; ok {
			declared = memory.Value()
		} else if memory, ok := resources.Requests[corev1.ResourceMemory]; ok {
			declared = memory.Value()
		}
	}
	if declared <= 0 {
		declared = class.RatedMemoryBytes
	}
	return max(declared-nonPostgresReserve, minSharedBuffers)
}

// instanceCPUMillis is how much CPU the postmaster may plan around, and where the figure came
// from. The precedence is the one instanceMemory uses, for the same reason: limits, then
// requests, then the class rating.
//
// The milestone this belongs to is called "auto-configure PostgreSQL from CPU and memory" and
// CPU was read nowhere at all - max_worker_processes was the literal 16 whether the class sold
// one core or thirty-two.
func instanceCPUMillis(resources *corev1.ResourceRequirements, class pgconf.SizingClass) int64 {
	if resources != nil {
		if cpu, ok := resources.Limits[corev1.ResourceCPU]; ok {
			return cpu.MilliValue()
		}
		if cpu, ok := resources.Requests[corev1.ResourceCPU]; ok {
			return cpu.MilliValue()
		}
	}
	return class.RatedCPUMillis
}

// sharedBuffersFor is a quarter of what is left after the reserve, floored and capped.
func sharedBuffersFor(resources *corev1.ResourceRequirements, class pgconf.SizingClass) string {
	usable := instanceMemory(resources, class)
	return megabytes(min(max(usable/4, minSharedBuffers), maxSharedBuffers))
}

// effectiveCacheSizeFor is half, ratifying the divisor the tree already used - and not the
// three quarters pgtune and timescaledb-tune advise.
//
// Their 75% is single-tenant advice. The planner pro-rates this figure across the tables in
// the *current query* only, never across concurrent sessions and never across databases, so
// telling each of ~200 tenants that three quarters of RAM is available to it is optimistic by
// roughly the tenant count and biases every plan toward index scans.
func effectiveCacheSizeFor(resources *corev1.ResourceRequirements, class pgconf.SizingClass) string {
	return megabytes(instanceMemory(resources, class) / 2)
}

func megabytes(bytes int64) string {
	return strconv.FormatInt(bytes/(1<<20), 10) + "MB"
}

// StorageQuantity is a convenience for reporting allocated bytes in status.
func StorageQuantity(size resource.Quantity) *resource.Quantity {
	copied := size.DeepCopy()
	return &copied
}

// DroppedParameters lists the parameters in spec.parameters that will not be rendered.
//
// The refusal happens at admission now, so reaching this with anything in it means the
// object was stored before that parameter became owned - which is the exact case the second
// pass exists for, and the exact case that used to happen in total silence: the sole caller
// of UserParameters discarded this return, so a stale `max_connections` was written to the
// object, never rendered, and never mentioned again.
func (b Builder) DroppedParameters() []string {
	_, dropped := pgconf.UserParameters(b.Instance.Spec.Parameters)
	return dropped
}

// majorOf reads spec.postgresVersion as a number.
//
// An absent or unreadable value is the tree's own default rather than zero: the field is
// optional and defaulted by the CRD, so an object stored before it had a default reaches this
// with nothing set, and rendering that as major 0 would silently pick every "before 19" branch
// for a cluster nobody has said anything about. The enum is what refuses a version the tree
// does not know; this only has to turn the ones it admits into a number.
func majorOf(version *string) int {
	if version == nil {
		return pgconf.DefaultMajor
	}
	major, err := strconv.Atoi(strings.TrimSpace(*version))
	if err != nil || major <= 0 {
		return pgconf.DefaultMajor
	}
	return major
}
