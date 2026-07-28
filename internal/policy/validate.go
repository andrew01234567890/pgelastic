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

package policy

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Problem is one way an object contradicts itself: the field path at fault and why.
// The webhook turns it into a field error and the controller into a condition message,
// so both say the same thing about the same object.
type Problem struct {
	Path   string
	Detail string
}

func (p Problem) String() string { return p.Path + ": " + p.Detail }

// JoinProblems renders a list of problems as one condition message.
func JoinProblems(problems []Problem) string {
	rendered := make([]string, 0, len(problems))
	for _, problem := range problems {
		rendered = append(rendered, problem.String())
	}
	return strings.Join(rendered, "; ")
}

// WorkloadClassProblems reports every way a workload class contradicts itself. These are
// the checks CEL cannot express, because each one turns on what a value means to
// PostgreSQL or to the admission ladder rather than on arithmetic between fields.
//
// The webhook rejects a class carrying any of them; the controller reports the same list
// as a condition, so a class written while the webhook was unavailable still says why it
// is not usable.
func WorkloadClassProblems(workloadClass *pgelasticv1alpha1.PgWorkloadClass) []Problem {
	problems := limitProblems(workloadClass.Spec.Limits)

	if admission := workloadClass.Spec.Admission; admission != nil && admission.Quarantine != nil {
		quarantine := admission.Quarantine
		if quarantine.Required != nil && *quarantine.Required &&
			quarantine.Duration != nil && quarantine.Duration.Duration <= 0 {
			problems = append(problems, Problem{
				Path:   "spec.admission.quarantine.duration",
				Detail: "must be positive when spec.admission.quarantine.required is set",
			})
		}
	}

	var guaranteed int32
	if workloadClass.Spec.Capacity.Guaranteed != nil {
		guaranteed = *workloadClass.Spec.Capacity.Guaranteed
	}
	if DeriveQoS(guaranteed, workloadClass.Spec.Capacity.Burstable) == pgelasticv1alpha1.QoSGuaranteed &&
		workloadClass.Spec.OnBudgetExhaustion != nil &&
		*workloadClass.Spec.OnBudgetExhaustion == pgelasticv1alpha1.BudgetExhaustionEvict {
		problems = append(problems, Problem{
			Path: "spec.onBudgetExhaustion",
			Detail: "Evict has nothing to evict when spec.capacity.guaranteed equals spec.capacity.burstable, " +
				"because the tenant holds no surplus above its floor",
		})
	}

	return problems
}

func limitProblems(limits *pgelasticv1alpha1.TenantLimits) []Problem {
	if limits == nil {
		return nil
	}
	problems := make([]Problem, 0, 4)

	durations := map[string]*metav1.Duration{
		"spec.limits.statementTimeout":                limits.StatementTimeout,
		"spec.limits.idleInTransactionSessionTimeout": limits.IdleInTransactionSessionTimeout,
		"spec.limits.idleSessionTimeout":              limits.IdleSessionTimeout,
		"spec.limits.lockTimeout":                     limits.LockTimeout,
	}
	for _, path := range slices.Sorted(maps.Keys(durations)) {
		if value := durations[path]; value != nil && value.Duration <= 0 {
			problems = append(problems, Problem{
				Path: path,
				Detail: fmt.Sprintf("%s is not a limit: PostgreSQL reads a non-positive timeout as no limit at all",
					value.Duration),
			})
		}
	}

	nonPositive := map[string]bool{
		"spec.limits.tempFileLimit":  limits.TempFileLimit != nil && limits.TempFileLimit.Sign() <= 0,
		"spec.limits.maxResultBytes": limits.MaxResultBytes != nil && limits.MaxResultBytes.Sign() <= 0,
		"spec.limits.rateLimit.bytesPerSecond": limits.RateLimit != nil && limits.RateLimit.BytesPerSecond != nil &&
			limits.RateLimit.BytesPerSecond.Sign() <= 0,
	}
	for _, path := range slices.Sorted(maps.Keys(nonPositive)) {
		if nonPositive[path] {
			problems = append(problems, Problem{Path: path, Detail: "must be positive"})
		}
	}

	return problems
}

// ElasticClassProblems reports the ways a pool class's own defaults contradict each other.
func ElasticClassProblems(elasticClass *pgelasticv1alpha1.PgElasticClass) []Problem {
	defaults := elasticClass.Spec.Defaults
	if defaults == nil {
		return nil
	}
	problems := make([]Problem, 0, 2)

	var headroom, migrationHeadroom int32
	if defaults.HeadroomPercent != nil {
		headroom = *defaults.HeadroomPercent
	}
	if defaults.MigrationHeadroomPercent != nil {
		migrationHeadroom = *defaults.MigrationHeadroomPercent
	}
	if headroom+migrationHeadroom >= 100 {
		problems = append(problems, Problem{
			Path: "spec.defaults.migrationHeadroomPercent",
			Detail: fmt.Sprintf("%d on top of headroomPercent %d withholds the whole budget, leaving a pool "+
				"no allocatable capacity during a migration", migrationHeadroom, headroom),
		})
	}

	if admission := defaults.Admission; admission != nil && admission.DefaultWorkloadClassName != nil &&
		len(admission.AllowedWorkloadClassNames) > 0 &&
		!slices.Contains(admission.AllowedWorkloadClassNames, *admission.DefaultWorkloadClassName) {
		problems = append(problems, Problem{
			Path: "spec.defaults.admission.defaultWorkloadClassName",
			Detail: fmt.Sprintf("%q is not in spec.defaults.admission.allowedWorkloadClassNames",
				*admission.DefaultWorkloadClassName),
		})
	}

	return problems
}
