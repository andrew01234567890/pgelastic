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

	corev1 "k8s.io/api/core/v1"
)

// Group is the pair of volumes one member is made of.
//
// Instance identity is derived from the PVC groups, not from the Pods. A Pod is
// disposable and can be recreated at will; the pair of volumes underneath it is the
// member. Reading identity off Pods instead means a member disappears the moment its Pod
// is deleted, which is precisely when the operator most needs to know it exists.
type Group struct {
	// Serial is the member ordinal the two claims share.
	Serial int32
	// Data is the PGDATA claim.
	Data *corev1.PersistentVolumeClaim
	// WAL is the pg_wal claim.
	WAL *corev1.PersistentVolumeClaim
}

// Complete reports whether both volumes of the group exist and neither is being deleted.
// A Pod is never scheduled onto an incomplete group: a member with only one of its two
// volumes would start PostgreSQL with pg_wal on the data volume, which is the one layout
// the design exists to prevent.
func (g Group) Complete() bool {
	return g.Data != nil && g.WAL != nil &&
		g.Data.DeletionTimestamp.IsZero() && g.WAL.DeletionTimestamp.IsZero()
}

// Bound reports whether both claims have been bound to a volume.
func (g Group) Bound() bool {
	return g.Complete() &&
		g.Data.Status.Phase == corev1.ClaimBound &&
		g.WAL.Status.Phase == corev1.ClaimBound
}

// Dangling reports whether either claim is mid-resize. A resizing claim with no Pod
// mounting it can never finish on an offline-expansion CSI driver, so the operator has to
// recreate the Pod rather than wait: a simultaneous size and resource change during a
// rolling update otherwise orphans the volume forever.
func (g Group) Dangling() bool {
	return resizing(g.Data) || resizing(g.WAL)
}

func resizing(claim *corev1.PersistentVolumeClaim) bool {
	if claim == nil {
		return false
	}
	for _, status := range claim.Status.AllocatedResourceStatuses {
		switch status {
		case corev1.PersistentVolumeClaimControllerResizeInProgress,
			corev1.PersistentVolumeClaimNodeResizePending,
			corev1.PersistentVolumeClaimNodeResizeInProgress:
			return true
		}
	}
	return false
}

// GroupsOf assembles PVC groups from a flat list of claims, keyed on the serial and role
// labels the operator stamps at creation.
func GroupsOf(claims []corev1.PersistentVolumeClaim) []Group {
	bySerial := map[int32]*Group{}
	for i := range claims {
		claim := &claims[i]
		serial, ok := SerialOf(claim.Labels)
		if !ok {
			continue
		}
		group, ok := bySerial[serial]
		if !ok {
			group = &Group{Serial: serial}
			bySerial[serial] = group
		}
		switch claim.Labels[LabelPVCRole] {
		case PVCRoleData:
			group.Data = claim
		case PVCRoleWAL:
			group.WAL = claim
		}
	}

	groups := make([]Group, 0, len(bySerial))
	for _, group := range bySerial {
		groups = append(groups, *group)
	}
	slices.SortFunc(groups, func(a, b Group) int { return int(a.Serial - b.Serial) })
	return groups
}

// MissingSerials lists the serials that have to be created to reach the desired member
// count, in ascending order, skipping serials whose group already exists.
func MissingSerials(groups []Group, replicas int32) []int32 {
	present := make(map[int32]bool, len(groups))
	for _, group := range groups {
		present[group.Serial] = true
	}
	var missing []int32
	for serial := int32(1); serial <= replicas; serial++ {
		if !present[serial] {
			missing = append(missing, serial)
		}
	}
	return missing
}
