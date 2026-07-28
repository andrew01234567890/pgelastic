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

package migration

import (
	"context"
	"strings"
	"testing"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

func sequenceSource(rows ...Row) *fakeSQL {
	return newFakeSQL().answer("FROM pg_sequences ORDER BY schemaname", rows...)
}

func TestSequencesAreAdvancedPastTheSourceByTheSafetyGap(t *testing.T) {
	sql := sequenceSource(Row{userSchema, ordersSequence, "4200"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap, SafetyGap: 1000}
	count, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%d sequences reconciled, wanted 1", count)
	}
	if sql.ran(QuoteQualified(userSchema, ordersSequence)+"', 5200, false)") < 0 {
		t.Fatalf("the sequence was not advanced past the source plus the gap: %v", sql.statement)
	}
}

// TestSkipRefusesATenantThatHasSequences is the check that stops the failure this whole
// step exists for: a skipped setval produces duplicate keys hours later, once the tenant's
// inserts catch up with rows that were copied in.
func TestSkipRefusesATenantThatHasSequences(t *testing.T) {
	sql := sequenceSource(Row{userSchema, ordersSequence, "4200"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSkip}
	if _, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt); err == nil {
		t.Fatal("Skip silently skipped a tenant that has sequences")
	} else if !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("the refusal does not explain the consequence: %s", err)
	}
}

func TestSkipIsAllowedForATenantWithNoSequences(t *testing.T) {
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSkip}
	count, err := plan.Reconcile(context.Background(), sequenceSource(), sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d sequences reconciled for a tenant that has none", count)
	}
}

func TestAnUnusedSequenceStartsFromItsStartValue(t *testing.T) {
	// pg_sequences reports a NULL last_value until a sequence has been read from, in which
	// case the next value the source would hand out is start_value rather than last+1.
	sql := sequenceSource(Row{userSchema, "fresh_seq", "1"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap, SafetyGap: 10}
	if _, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	if sql.ran(QuoteQualified(userSchema, "fresh_seq")+"', 11, false)") < 0 {
		t.Fatalf("an unused sequence was not carried across: %v", sql.statement)
	}
}

func TestEverySequenceIsCarriedAcross(t *testing.T) {
	sql := sequenceSource(
		Row{userSchema, firstSequence, "1"}, Row{userSchema, "b_seq", "2"}, Row{"billing", "c_seq", "3"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap, SafetyGap: 0}
	count, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("%d sequences reconciled, wanted 3", count)
	}
	if err := sql.sawAll(
		QuoteQualified(userSchema, firstSequence),
		QuoteQualified(userSchema, "b_seq"),
		QuoteQualified("billing", "c_seq")); err != nil {
		t.Fatal(err)
	}
}

func TestSequencesAreWrittenToTheTargetAndReadFromTheSource(t *testing.T) {
	sql := sequenceSource(Row{userSchema, firstSequence, "1"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap, SafetyGap: 0}
	if _, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	for index, statement := range sql.statement {
		if strings.Contains(statement, "setval") && sql.endpoints[index].Instance != targetAt.Instance {
			t.Fatalf("setval ran against %s rather than the target", sql.endpoints[index].Instance)
		}
		if strings.Contains(statement, "FROM pg_sequences") && sql.endpoints[index].Instance != sourceAt.Instance {
			t.Fatalf("the source positions were read from %s", sql.endpoints[index].Instance)
		}
	}
}

func TestAnUnreadableSequenceRecordIsAnError(t *testing.T) {
	sql := sequenceSource(Row{userSchema, firstSequence, "not a number"})
	plan := SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap}
	if _, err := plan.Reconcile(context.Background(), sql, sourceAt, targetAt); err == nil {
		t.Fatal("an unreadable sequence position was accepted")
	}
}
