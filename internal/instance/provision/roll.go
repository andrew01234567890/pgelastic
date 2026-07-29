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
	corev1 "k8s.io/api/core/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

// RollStamp is what a Pod was last rolled for: the operator's configuration, and the
// explicit rollout request that was outstanding at the time.
//
// Both live on the Pod rather than in the operator's memory, because a rolling restart
// runs for minutes across many reconciles and an operator that is restarted in the middle
// of one must resume it at the member it had reached. A Pod carrying the desired stamp has
// demonstrably been started since both were current; anything else has not.
type RollStamp struct {
	// ConfigHash is over the operator's own inputs - the rendered parameters and the
	// rendered pg_hba - and so is identical across the three members.
	ConfigHash string
	// RestartedAt is the instance's restartedAt annotation, verbatim and unparsed.
	RestartedAt string
}

// Annotations renders the stamp for a Pod's metadata.
func (s RollStamp) Annotations() map[string]string {
	annotations := map[string]string{AnnotationConfigHash: s.ConfigHash}
	if s.RestartedAt != "" {
		annotations[AnnotationRestartedAt] = s.RestartedAt
	}
	return annotations
}

// StampOf reads back the stamp one Pod carries. A Pod created before this field existed
// reads as an empty restartedAt, which is what an instance that has never been asked to
// restart also produces, so upgrading the operator does not roll every instance once.
func StampOf(pod *corev1.Pod) RollStamp {
	return RollStamp{
		ConfigHash:  pod.Annotations[AnnotationConfigHash],
		RestartedAt: pod.Annotations[AnnotationRestartedAt],
	}
}

// DesiredStamp is the stamp every member should be carrying.
func (b Builder) DesiredStamp() RollStamp {
	return RollStamp{
		ConfigHash:  b.ConfigHash(),
		RestartedAt: b.Instance.GetAnnotations()[pgelasticv1alpha1.AnnotationRestartedAt],
	}
}

// ConfigHash identifies the configuration the operator wants every member running.
//
// It is deliberately not the hash the agent publishes through pgelastic.config_sha256.
// That one is over the file the member wrote, which carries its own name, so it differs
// between members - useless as the question "is this member on the current
// configuration". This one is over the operator's inputs alone.
//
// The primary epoch is excluded, and that exclusion is load-bearing rather than tidy. The
// fence token travels in the same document and is bumped by every promotion - including
// the promotion a rolling restart performs to get the role off the member it is about to
// restart. Hashing it makes the roll change its own answer: finishing a roll bumps the
// epoch, which makes all three members stale again, which starts another roll, which
// bumps the epoch. That is not a hypothesis; it ran, and it rolled two instances
// continuously until it was stopped. It is also not a correctness loss, because the epoch
// is a GUC the postmaster reloads rather than one it restarts for.
func (b Builder) ConfigHash() string {
	config := b.AgentConfig()
	postgres := config.Postgres
	postgres.PrimaryEpoch = 0
	settings := pgconf.FormatSettings("custom", pgconf.RenderCustomConf(postgres))
	return pgconf.Hash(settings, pgconf.RenderHBA(config.HBA))
}
