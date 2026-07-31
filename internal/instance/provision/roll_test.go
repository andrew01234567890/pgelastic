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
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

func stampBuilder(epoch int64, dumps int32) Builder {
	return Builder{
		Instance: &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: testInstance, Namespace: "saas"},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				Class:   "dev-1",
				Storage: storageOf("2Gi", "512Mi"),
				PerTenantLogicalBackup: &pgelasticv1alpha1.PerTenantLogicalBackup{
					MaxConcurrentDumps: ptr.To(dumps),
				},
			},
		},
		Capacity:     pgconf.DeriveCapacity(50, dumps, 3, 4),
		PrimaryEpoch: epoch,
	}
}

// storageOf keeps the fixture readable; the sizes themselves are not the subject.
func storageOf(data, wal string) pgelasticv1alpha1.InstanceStorage {
	return pgelasticv1alpha1.InstanceStorage{
		Size:      resource.MustParse(data),
		WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse(wal)},
	}
}

func TestTheRollSignatureDoesNotMoveWhenThePrimaryEpochDoes(t *testing.T) {
	before := stampBuilder(7, 4).ConfigHash()
	after := stampBuilder(8, 4).ConfigHash()

	if before != after {
		t.Fatal("a promotion changed the configuration the roll compares against. " +
			"Every roll ends in a promotion, so this is a roll that restarts the instance, " +
			"bumps the epoch, and starts again - it ran, and it did not stop on its own")
	}
}

func TestTheRollSignatureMovesWhenMaxConnectionsDoes(t *testing.T) {
	before := stampBuilder(7, 4).ConfigHash()
	after := stampBuilder(7, 6).ConfigHash()

	if before == after {
		t.Error("max_connections changed and no member would be restarted for it, which is " +
			"the parameter the whole capacity model rests on left at its old value")
	}
}

func TestAPodCarriesTheStampItWasCreatedFor(t *testing.T) {
	builder := stampBuilder(7, 4)
	builder.Instance.Annotations = map[string]string{
		pgelasticv1alpha1.AnnotationRestartedAt: "2026-07-29T09:00:00Z",
	}

	stamp := builder.DesiredStamp()
	pod := builder.Pod(1, stamp)

	if got := StampOf(pod); got != stamp {
		t.Errorf("the Pod carries %+v, want %+v; a Pod that cannot be compared with the "+
			"desired stamp is a member the roll can never call current", got, stamp)
	}
}

func TestAnInstanceThatWasNeverAskedToRestartCarriesNoRequest(t *testing.T) {
	builder := stampBuilder(7, 4)

	pod := builder.Pod(1, builder.DesiredStamp())

	if _, present := pod.Annotations[AnnotationRestartedAt]; present {
		t.Error("an absent restart request has to read as absent, or upgrading the operator " +
			"rolls every instance in the fleet once for nothing")
	}
}
