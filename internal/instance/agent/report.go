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
	"encoding/json"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Reporter publishes one member's own view of itself into PgInstance.status.
//
// It writes through two different mechanisms, and which one carries which field is a
// correctness decision rather than a stylistic one.
//
// The instances list goes through server-side apply under a field manager unique to this
// member. The list is a listType=map keyed on the member name, so three agents and the
// operator can write the same status object without any of them clobbering the others: each
// agent owns exactly its own entry, and the operator never applies that field at all.
//
// The singleton fields only a primary may write - currentPrimary, primaryEpoch, the quorum
// evidence and the collation contract - go through a merge patch instead. Under server-side
// apply a field belongs to the manager that last wrote it, and a manager that stops
// including a field in a later apply *removes* it. A demoted primary that began reporting
// itself as a replica would therefore delete the instance's quorum evidence and its record
// of which member is the primary, in the same instant its successor needs both of them to
// decide whether promoting is safe at all.
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

// Report applies this member's own entry in the instances list.
func (r Reporter) Report(
	ctx context.Context,
	observation MemberObservation,
	healthy bool,
	rejoining RejoinMethod,
) error {
	member := map[string]any{
		"name":              r.Member,
		"role":              string(instanceRole(observation.Role)),
		"lsn":               observation.LSN,
		"receivedLSN":       observation.ReceivedLSN,
		"replayLSN":         observation.ReplayLSN,
		"timeline":          int64(observation.Timeline),
		"healthy":           healthy,
		"walReceiverActive": observation.WALReceiverActive,
		"walVolumeFull":     observation.WALVolumeFull,
	}
	// The key is omitted rather than emptied when no rejoin is running. Under server-side
	// apply a field the manager stops including is removed, which is exactly the clearing
	// this needs, and the field is an enum that an empty string is not a member of.
	if rejoining != "" {
		member["rejoining"] = string(rejoining)
	}

	object := r.object()
	object.Object["status"] = map[string]any{"instances": []any{member}}
	return r.Client.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(object),
		r.fieldOwner(), client.ForceOwnership)
}

// PrimaryState is what the member holding the role publishes about the instance as a whole.
type PrimaryState struct {
	// ClaimRole writes currentPrimary. It is set at bootstrap, when nobody holds the role
	// yet, and at the end of a promotion - never in between, because taking the role from
	// somebody else is the promotion sequence's job and it writes this last, after the
	// checkpoint, the quorum clause and the epoch have all landed.
	ClaimRole bool
	// Epoch is the fence token this member holds the role at.
	Epoch int64
	// Observation carries the loaded quorum clause, read back out of the postmaster.
	Observation MemberObservation
	// Contract is the collation contract, recorded once and thereafter immutable.
	Contract *Contract
}

// PublishPrimaryState merge-patches the fields only a primary may write.
//
// The epoch travels in the same patch as the name rather than before it, so there is no
// instant at which a reader can see a new primary paired with the previous primary's fence
// token.
func (r Reporter) PublishPrimaryState(ctx context.Context, state PrimaryState) error {
	status := map[string]any{
		"quorumEvidence": map[string]any{
			"synchronousStandbyNames": state.Observation.SyncStandbyNames,
			"numSync":                 state.Observation.NumSync,
			// votingMembers is N, and it is parsed out of the clause the postmaster loaded
			// rather than taken from pg_stat_replication. Sourcing it from the observed
			// replication state would let a member PostgreSQL never loaded as a voter count
			// towards the quorum gate, which is the one thing the gate exists to prevent.
			"votingMembers":    state.Observation.VotingMembers,
			"streamingMembers": state.Observation.SyncStandbys,
			"reportedBy":       r.Member,
			"observedAt":       time.Now().UTC().Format(time.RFC3339),
		},
	}
	if state.ClaimRole {
		status["currentPrimary"] = r.Member
		status["primaryEpoch"] = state.Epoch
	}
	if state.Contract != nil {
		status["collationContract"] = map[string]any{
			"encoding":         state.Contract.Encoding,
			"localeProvider":   state.Contract.LocaleProvider,
			"locale":           state.Contract.Locale,
			"collate":          state.Contract.Collate,
			"ctype":            state.Contract.Ctype,
			"icuRules":         state.Contract.ICURules,
			"walSegmentSize":   state.Contract.WALSegmentSize,
			"dataChecksums":    state.Contract.DataChecksums,
			"systemIdentifier": state.Contract.SystemIdentifier,
		}
	}

	patch, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	return r.Client.Status().Patch(ctx, r.object(),
		client.RawPatch(types.MergePatchType, patch))
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

// SerialFromEnv reads the member serial the operator stamped into the Pod.
func SerialFromEnv(value string) int32 {
	serial, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0
	}
	return int32(serial)
}
