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

package agent

import (
	"context"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Reporter publishes one member's own view of itself into PgInstance.status.
//
// Every write is a server-side apply under a field manager unique to this member, and it
// touches only the fields this member owns. That is what lets three agents and the
// operator write the same status object without any of them clobbering the others: the
// instances list is a listType=map keyed on the member name, so each agent owns exactly
// its own entry, and the operator never applies that field at all.
type Reporter struct {
	// Client talks to the API server with the agent's own ServiceAccount.
	Client client.Client
	// Namespace and Instance address the PgInstance.
	Namespace string
	Instance  string
	// Member is this pod's name and the key of its entry in the instances list.
	Member string
}

func (r Reporter) fieldOwner() client.FieldOwner {
	return client.FieldOwner("pgelastic-agent-" + r.Member)
}

func (r Reporter) object() *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetAPIVersion(pgelasticv1alpha1.SchemeGroupVersion.String())
	object.SetKind("PgInstance")
	object.SetNamespace(r.Namespace)
	object.SetName(r.Instance)
	return object
}

// Report applies this member's observation.
func (r Reporter) Report(ctx context.Context, observation MemberObservation, healthy bool, contract *Contract) error {
	member := map[string]any{
		"name":              r.Member,
		"role":              string(instanceRole(observation.Role)),
		"lsn":               observation.LSN,
		"timeline":          int64(observation.Timeline),
		"healthy":           healthy,
		"walReceiverActive": observation.WALReceiverActive,
	}
	status := map[string]any{"instances": []any{member}}

	if observation.Role == RolePrimary {
		// currentPrimary is written by the promoted pod itself, last, after everything
		// that has to be true before anybody may believe it.
		status["currentPrimary"] = r.Member
		status["quorumEvidence"] = map[string]any{
			"synchronousStandbyNames": observation.SyncStandbyNames,
			"numSync":                 int64(observation.NumSync),
			"votingMembers":           toAnySlice(observation.SyncStandbys),
			"reportedBy":              r.Member,
			"observedAt":              time.Now().UTC().Format(time.RFC3339),
		}
		if contract != nil {
			status["collationContract"] = map[string]any{
				"encoding":         contract.Encoding,
				"localeProvider":   contract.LocaleProvider,
				"locale":           contract.Locale,
				"collate":          contract.Collate,
				"ctype":            contract.Ctype,
				"icuRules":         contract.ICURules,
				"walSegmentSize":   contract.WALSegmentSize,
				"dataChecksums":    contract.DataChecksums,
				"systemIdentifier": contract.SystemIdentifier,
			}
		}
	}

	object := r.object()
	object.Object["status"] = status
	return r.Client.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(object),
		r.fieldOwner(), client.ForceOwnership)
}

func instanceRole(role Role) pgelasticv1alpha1.InstanceRole {
	switch role {
	case RolePrimary:
		return pgelasticv1alpha1.InstanceRolePrimary
	case RoleReplica:
		return pgelasticv1alpha1.InstanceRoleReplica
	default:
		return pgelasticv1alpha1.InstanceRoleUnknown
	}
}

func toAnySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

// SerialFromEnv reads the member serial the operator stamped into the Pod.
func SerialFromEnv(value string) int32 {
	serial, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0
	}
	return int32(serial)
}
