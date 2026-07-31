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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testInstance is the instance name every test in this package builds objects for.
const testInstance = "pg-a"

func claim(name string, serial, role string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				LabelInstanceName: testInstance,
				LabelNodeSerial:   serial,
				LabelPVCRole:      role,
			},
		},
	}
}

func TestGroupsOfDerivesIdentityFromTheVolumes(t *testing.T) {
	groups := GroupsOf([]corev1.PersistentVolumeClaim{
		claim("pg-a-2-wal", "2", PVCRoleWAL),
		claim("pg-a-1", "1", PVCRoleData),
		claim("pg-a-2", "2", PVCRoleData),
		claim("pg-a-1-wal", "1", PVCRoleWAL),
	})
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want two", len(groups))
	}
	if groups[0].Serial != 1 || groups[1].Serial != 2 {
		t.Errorf("serials = %d,%d, want them in ascending order", groups[0].Serial, groups[1].Serial)
	}
	for _, group := range groups {
		if !group.Complete() {
			t.Errorf("serial %d is not complete", group.Serial)
		}
	}
}

func TestGroupWithOneVolumeIsNeverComplete(t *testing.T) {
	groups := GroupsOf([]corev1.PersistentVolumeClaim{claim("pg-a-1", "1", PVCRoleData)})
	if groups[0].Complete() {
		t.Fatal("a member with no WAL volume would put pg_wal on the data volume")
	}
}

func TestGroupBeingDeletedIsNotComplete(t *testing.T) {
	claims := []corev1.PersistentVolumeClaim{
		claim("pg-a-1", "1", PVCRoleData),
		claim("pg-a-1-wal", "1", PVCRoleWAL),
	}
	now := metav1.Now()
	claims[1].DeletionTimestamp = &now
	if GroupsOf(claims)[0].Complete() {
		t.Fatal("a volume being deleted is not a volume to schedule onto")
	}
}

func TestGroupBoundNeedsBothClaims(t *testing.T) {
	claims := []corev1.PersistentVolumeClaim{
		claim("pg-a-1", "1", PVCRoleData),
		claim("pg-a-1-wal", "1", PVCRoleWAL),
	}
	claims[0].Status.Phase = corev1.ClaimBound
	if GroupsOf(claims)[0].Bound() {
		t.Fatal("half a bound group is not a bound group")
	}
	claims[1].Status.Phase = corev1.ClaimBound
	if !GroupsOf(claims)[0].Bound() {
		t.Fatal("both claims bound must make the group bound")
	}
}

func TestGroupDanglingWhileResizing(t *testing.T) {
	claims := []corev1.PersistentVolumeClaim{
		claim("pg-a-1", "1", PVCRoleData),
		claim("pg-a-1-wal", "1", PVCRoleWAL),
	}
	if GroupsOf(claims)[0].Dangling() {
		t.Fatal("a settled group is not dangling")
	}
	claims[0].Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
		corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizePending,
	}
	if !GroupsOf(claims)[0].Dangling() {
		t.Fatal("a claim pending a node resize with no Pod can never finish on its own")
	}
}

func TestMissingSerialsSkipsWhatExists(t *testing.T) {
	groups := GroupsOf([]corev1.PersistentVolumeClaim{
		claim("pg-a-2", "2", PVCRoleData),
		claim("pg-a-2-wal", "2", PVCRoleWAL),
	})
	if missing := MissingSerials(groups, 3); !slices.Equal(missing, []int32{1, 3}) {
		t.Errorf("missing = %v, want the two absent serials in ascending order", missing)
	}
	if missing := MissingSerials(groups, 2); !slices.Equal(missing, []int32{1}) {
		t.Errorf("missing = %v, want only serial 1", missing)
	}
}

func TestReplicationSlotNameIsAValidIdentifier(t *testing.T) {
	cases := map[string]string{
		"pg-a-2":        "pgelastic_pg_a_2",
		"saas.pool-1":   "pgelastic_saas_pool_1",
		"PG-Instance-3": "pgelastic_pg_instance_3",
	}
	for member, want := range cases {
		if got := ReplicationSlotName(member); got != want {
			t.Errorf("ReplicationSlotName(%q) = %q, want %q", member, got, want)
		}
	}
}

func TestSerialOf(t *testing.T) {
	if serial, ok := SerialOf(map[string]string{LabelNodeSerial: "7"}); !ok || serial != 7 {
		t.Errorf("SerialOf = %d,%v, want 7,true", serial, ok)
	}
	if _, ok := SerialOf(map[string]string{}); ok {
		t.Error("a label-less object has no serial")
	}
	if _, ok := SerialOf(map[string]string{LabelNodeSerial: "not-a-number"}); ok {
		t.Error("a malformed serial is not a serial")
	}
}

func TestNamesAreStableAcrossRecreation(t *testing.T) {
	if DataPVCName(testInstance, 2) != MemberName(testInstance, 2) {
		t.Error("the data claim shares the member's name so a recreated Pod finds its volume")
	}
	if WALPVCName(testInstance, 2) != "pg-a-2-wal" {
		t.Errorf("WALPVCName = %q, want pg-a-2-wal", WALPVCName(testInstance, 2))
	}
}
