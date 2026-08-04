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

package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
)

// Metric label names, spelled once so a dashboard query cannot be broken by a typo in one
// of five places.
const (
	labelNamespace = "namespace"
	labelInstance  = "instance"
	labelKind      = "kind"
)

// Failover metrics. Each named veto is a label value rather than a metric of its own, so an
// alert can fire on "a failover is being held back" without enumerating the reasons, while
// a dashboard can still say which one.
var (
	failoverVeto = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgelastic_failover_veto",
		Help: "1 while a named veto is holding a failover back for this instance.",
	}, []string{labelNamespace, labelInstance, "veto"})

	failoverPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgelastic_failover_phase",
		Help: "1 for the phase the failover state machine is currently in.",
	}, []string{labelNamespace, labelInstance, fieldPhase})

	splitBrainObserved = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgelastic_split_brain_observed",
		Help: "1 while two members simultaneously report pg_is_in_recovery() = false. Page on this.",
	}, []string{labelNamespace, labelInstance})

	writeStalled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgelastic_write_stalled",
		Help: "1 while fewer standbys are streaming than the loaded synchronous_standby_names waits for.",
	}, []string{labelNamespace, labelInstance})

	quorumReachableVoters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgelastic_failover_quorum_reachable_voters",
		Help: "R in the R + W > N failover gate, as last evaluated.",
	}, []string{labelNamespace, labelInstance})
)

// phases is every phase the gauge can report, so that a phase going away sets its series to
// zero rather than leaving the previous one latched at one forever.
var phases = []ha.Phase{
	ha.PhaseSteady,
	ha.PhaseDebouncing,
	ha.PhaseSentinel,
	ha.PhaseWaitingWALReceivers,
	ha.PhaseCandidateChosen,
	ha.PhaseVetoed,
	ha.PhaseSplitBrain,
	ha.PhasePromoting,
}

func init() {
	metrics.Registry.MustRegister(failoverVeto, failoverPhase, splitBrainObserved,
		writeStalled, quorumReachableVoters)
}

// recordFailoverDecision publishes the decision as metrics.
//
// Every label value is written on every reconcile, including the zeros. A veto that stops
// applying has to have its series driven back to zero explicitly: a gauge left at its last
// value is how a resolved incident goes on paging somebody.
func recordFailoverDecision(instance *pgelasticv1alpha1.PgInstance, decision ha.Decision) {
	namespace, name := instance.Namespace, instance.Name
	for _, veto := range ha.Vetoes {
		failoverVeto.WithLabelValues(namespace, name, string(veto)).Set(boolValue(decision.Veto == veto))
	}
	for _, phase := range phases {
		failoverPhase.WithLabelValues(namespace, name, string(phase)).Set(boolValue(decision.Phase == phase))
	}
	splitBrainObserved.WithLabelValues(namespace, name).Set(boolValue(decision.SplitBrain))
	writeStalled.WithLabelValues(namespace, name).Set(
		boolValue(ha.WriteStalled(ha.EvidenceFrom(instance.Status.QuorumEvidence))))
	quorumReachableVoters.WithLabelValues(namespace, name).Set(float64(decision.Quorum.R))
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
