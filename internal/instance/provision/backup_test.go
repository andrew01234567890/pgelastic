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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

func backupBuilder(backup *pgelasticv1alpha1.InstanceBackup) Builder {
	return Builder{
		Instance: &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: testInstance, Namespace: "tenants"},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				Storage: pgelasticv1alpha1.InstanceStorage{
					Size: resource.MustParse("20Gi"),
					WALVolume: pgelasticv1alpha1.WALVolume{
						Size: resource.MustParse("10Gi"),
					},
				},
				Backup: backup,
			},
		},
	}
}

func objectStoreBackup() *pgelasticv1alpha1.InstanceBackup {
	return &pgelasticv1alpha1.InstanceBackup{
		ObjectStore: pgelasticv1alpha1.ObjectStore{
			Path: "s3://backups/prod",
			CredentialsSecretRef: corev1.LocalObjectReference{
				Name: "prod-object-store",
			},
		},
	}
}

func TestNoRepositoryReachesTheAgentWhenNoneIsConfigured(t *testing.T) {
	if config := backupBuilder(nil).AgentConfig(); config.Backup != nil {
		t.Fatalf("backup = %+v, want nothing at all", config.Backup)
	}
}

func TestTheRepositoryTravelsInTheAgentConfig(t *testing.T) {
	backup := objectStoreBackup()
	backup.ObjectStore.EndpointURL = "https://objectstore.svc:9000"
	backup.ObjectStore.Region = "eu-west-2"

	config := backupBuilder(backup).AgentConfig()
	if config.Backup == nil {
		t.Fatal("the agent was given no repository")
	}
	if config.Backup.Path != "s3://backups/prod" {
		t.Errorf("path = %q", config.Backup.Path)
	}
	if config.Backup.EndpointURL != "https://objectstore.svc:9000" {
		t.Errorf("endpoint = %q", config.Backup.EndpointURL)
	}
	if config.Backup.Region != "eu-west-2" {
		t.Errorf("region = %q", config.Backup.Region)
	}
}

// The API server defaults the retention block, but an object built in a test or applied by
// a client that skipped validation has not been through it, and rendering a pgBackRest
// configuration with an empty retention window is an error rather than a default.
func TestRetentionFallsBackToTheDocumentedDefaults(t *testing.T) {
	config := backupBuilder(objectStoreBackup()).AgentConfig()
	if config.Backup.RetentionFull != defaultRetentionWindow ||
		config.Backup.RetentionWAL != defaultRetentionWindow {
		t.Fatalf("retention = %q/%q, want the documented 30d/30d",
			config.Backup.RetentionFull, config.Backup.RetentionWAL)
	}
}

func TestRetentionIsCarriedThroughWhenSet(t *testing.T) {
	backup := objectStoreBackup()
	backup.Retention = &pgelasticv1alpha1.RetentionPolicy{Full: "7d", WAL: "14d"}
	config := backupBuilder(backup).AgentConfig()
	if config.Backup.RetentionFull != "7d" || config.Backup.RetentionWAL != "14d" {
		t.Fatalf("retention = %q/%q", config.Backup.RetentionFull, config.Backup.RetentionWAL)
	}
}

// A Pod carrying a volume for a Secret that was never named would not schedule, so the
// mount and the volume have to appear and disappear together.
func TestTheCredentialsVolumeAppearsOnlyWithARepository(t *testing.T) {
	without := backupBuilder(nil).Pod(1, RollStamp{})
	if hasVolume(without, backupCredentialsVolume) {
		t.Error("an instance with no repository was given a credentials volume")
	}
	if hasMount(without, backupCredentialsVolume) {
		t.Error("an instance with no repository was given a credentials mount")
	}

	with := backupBuilder(objectStoreBackup()).Pod(1, RollStamp{})
	if !hasVolume(with, backupCredentialsVolume) {
		t.Error("the credentials Secret is not mounted into the Pod")
	}
	if !hasMount(with, backupCredentialsVolume) {
		t.Error("the postgres container cannot read the credentials")
	}
}

func TestTheCredentialsAreMountedReadOnlyFromTheNamedSecret(t *testing.T) {
	pod := backupBuilder(objectStoreBackup()).Pod(1, RollStamp{})

	index := slices.IndexFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool {
		return volume.Name == backupCredentialsVolume
	})
	if index < 0 {
		t.Fatal("the credentials volume is missing")
	}
	source := pod.Spec.Volumes[index].Secret
	if source == nil || source.SecretName != "prod-object-store" {
		t.Fatalf("volume source = %+v, want the Secret the spec named", source)
	}

	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if mount.Name != backupCredentialsVolume {
			continue
		}
		if !mount.ReadOnly {
			t.Error("the credentials are mounted writable")
		}
		if mount.MountPath != BackupCredentialsMountPath {
			t.Errorf("mount path = %q, want %q", mount.MountPath, BackupCredentialsMountPath)
		}
		return
	}
	t.Fatal("the credentials mount is missing from the postgres container")
}

// The bootstrap init container runs the same binary against the same data directory, and
// its rejoin path archives whatever pg_wal had left over before a rewind. Without the
// credentials it would mark those segments archived without archiving them.
func TestTheBootstrapContainerCanReachTheRepositoryToo(t *testing.T) {
	pod := backupBuilder(objectStoreBackup()).Pod(1, RollStamp{})
	for _, container := range pod.Spec.InitContainers {
		if container.Name != bootstrapContainer {
			continue
		}
		if !slices.ContainsFunc(container.VolumeMounts, func(mount corev1.VolumeMount) bool {
			return mount.Name == backupCredentialsVolume
		}) {
			t.Fatal("the bootstrap container cannot reach the repository")
		}
		return
	}
	t.Fatal("there is no bootstrap init container")
}

// archive_mode is PGC_POSTMASTER, so it is on from bootstrap whether or not a repository
// exists: turning it on later costs a restart that drops every tenant connection. The
// archive_command has to point at the agent for the same reason - it is the only artefact
// that can decide at runtime whether there is anywhere to push to.
func TestArchivingIsWiredEvenWithNoRepository(t *testing.T) {
	config := backupBuilder(nil).AgentConfig()
	if !strings.Contains(config.Postgres.ArchiveCommand, "wal-archive") {
		t.Errorf("archive_command = %q, want the agent's wal-archive subcommand",
			config.Postgres.ArchiveCommand)
	}
	if config.Postgres.ArchiveTimeout != ArchiveTimeout {
		t.Errorf("archive_timeout = %q, want %q", config.Postgres.ArchiveTimeout, ArchiveTimeout)
	}
}

// The spool holds WAL-sized files, so it belongs on the volume that was sized for WAL and
// whose exhaustion is already a first-class condition - and outside pg_wal, which
// PostgreSQL owns.
func TestTheArchiveSpoolIsOnTheWALVolumeAndOutsidePgWal(t *testing.T) {
	if !strings.HasPrefix(BackupSpoolPath, WALMountPath) {
		t.Errorf("the spool %q is not on the WAL volume", BackupSpoolPath)
	}
	if strings.HasPrefix(BackupSpoolPath, WALDir) {
		t.Errorf("the spool %q is inside pg_wal, which PostgreSQL owns", BackupSpoolPath)
	}
}

func hasVolume(pod *corev1.Pod, name string) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool {
		return volume.Name == name
	})
}

func hasMount(pod *corev1.Pod, name string) bool {
	return slices.ContainsFunc(pod.Spec.Containers[0].VolumeMounts,
		func(mount corev1.VolumeMount) bool { return mount.Name == name })
}
