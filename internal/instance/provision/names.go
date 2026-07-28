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

// Package provision builds the Kubernetes objects one PgInstance is made of, and derives
// the instance's identity from those objects.
//
// There is deliberately no StatefulSet. Individually managed Pods with a serial
// annotation and retained PVCs leave the operator owning ordering, naming and recreation
// semantics - which is what rolling by replication lag, promoting a specific member, and
// recreating a Pod onto storage that already exists all require.
package provision

import (
	"fmt"
	"strconv"
	"strings"
)

// Label keys. The PVC labels are what instance identity is derived from, so they are part
// of the contract rather than decoration.
const (
	// LabelInstanceName ties every object back to its PgInstance.
	LabelInstanceName = "pgelastic.io/instanceName"
	// LabelPVCRole distinguishes the two volume roles.
	LabelPVCRole = "pgelastic.io/pvcRole"
	// LabelNodeSerial is the member ordinal. It is a label rather than a StatefulSet
	// ordinal because the operator, not a controller it does not own, decides which
	// serial gets created, recreated or retired next.
	LabelNodeSerial = "pgelastic.io/nodeSerial"
	// LabelRole is flipped on promotion and is what the read-write and read-only Services
	// select on, and what the two PodDisruptionBudgets are keyed on.
	LabelRole = "pgelastic.io/role"
)

// Annotation keys.
const (
	// AnnotationPVCStatus tracks a volume through its lifecycle independently of the Pod
	// that mounts it, because a PVC resizing with no Pod attached is dangling and must
	// trigger Pod recreation: an offline-expansion CSI driver can never finish a
	// filesystem resize while nothing mounts the volume.
	AnnotationPVCStatus = "pgelastic.io/pvcStatus"
	// AnnotationConfigHash records the configuration a Pod was created for.
	AnnotationConfigHash = "pgelastic.io/configHash"
)

// PVC roles.
const (
	// PVCRoleData is PGDATA.
	PVCRoleData = "PG_DATA"
	// PVCRoleWAL is pg_wal, on its own volume without exception.
	PVCRoleWAL = "PG_WAL"
)

// PVC lifecycle states.
const (
	// PVCStatusInitializing is set at creation and cleared once the claim is bound.
	PVCStatusInitializing = "initializing"
	// PVCStatusReady means the claim is bound and may be mounted.
	PVCStatusReady = "ready"
	// PVCStatusDetached means the claim is retained but no Pod should mount it.
	PVCStatusDetached = "detached"
)

// MemberName is the Pod name for one serial.
func MemberName(instance string, serial int32) string {
	return fmt.Sprintf("%s-%d", instance, serial)
}

// DataPVCName is the PGDATA claim for one serial.
func DataPVCName(instance string, serial int32) string {
	return MemberName(instance, serial)
}

// WALPVCName is the pg_wal claim for one serial.
func WALPVCName(instance string, serial int32) string {
	return MemberName(instance, serial) + "-wal"
}

// PeerServiceName is the headless Service that gives every member a stable DNS name.
// Peer checks and primary_conninfo both address members through it rather than through a
// load-balanced Service, because a load-balanced Service is exactly what stops resolving
// to the right pod during the failures this has to work through.
func PeerServiceName(instance string) string { return instance + "-peers" }

// PrimaryServiceName selects the member currently labelled primary.
func PrimaryServiceName(instance string) string { return instance + "-rw" }

// ReplicaServiceName selects the members currently labelled replica.
func ReplicaServiceName(instance string) string { return instance + "-r" }

// ConfigMapName holds the generated custom.conf and pg_hba.conf.
func ConfigMapName(instance string) string { return instance + "-config" }

// CredentialsSecretName holds the replication and ops role passwords. The postgres
// superuser is deliberately absent: it has no password at all and is reachable only by
// peer authentication over a Unix socket in an emptyDir.
func CredentialsSecretName(instance string) string { return instance + "-credentials" }

// ServiceAccountName is the identity the in-pod agent uses to report its own member
// status and to hold the promotion Lease.
func ServiceAccountName(instance string) string { return instance + "-agent" }

// ReplicaPDBName keeps enough sync-capable standbys alive that an "ANY 1" commit never
// stalls because of a voluntary disruption.
func ReplicaPDBName(instance string) string { return instance }

// PrimaryPDBName makes a node drain hosting the primary block until a switchover happens,
// rather than taking the primary down and discovering afterwards whether failover worked.
func PrimaryPDBName(instance string) string { return instance + "-primary" }

// ReplicationSlotName is the persistent slot a standby streams from. Slot names admit
// only lower-case letters, digits and underscores, so the member name is transliterated
// rather than embedded.
func ReplicationSlotName(member string) string {
	var builder strings.Builder
	builder.WriteString("pgelastic_")
	for _, character := range strings.ToLower(member) {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			builder.WriteRune(character)
		default:
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

// SerialOf reads the member serial back off an object's labels.
func SerialOf(labels map[string]string) (int32, bool) {
	value, ok := labels[LabelNodeSerial]
	if !ok {
		return 0, false
	}
	serial, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(serial), true
}
