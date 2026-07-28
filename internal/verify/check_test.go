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

package verify_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/verify"
)

func committed(values ...int64) []verify.Record {
	recs := make([]verify.Record, 0, len(values)*2)
	for _, v := range values {
		recs = append(recs,
			verify.Record{State: verify.StateAttempted, Value: v},
			verify.Record{State: verify.StateCommitted, Value: v},
		)
	}
	return recs
}

func indeterminate(values ...int64) []verify.Record {
	recs := make([]verify.Record, 0, len(values)*2)
	for _, v := range values {
		recs = append(recs,
			verify.Record{State: verify.StateAttempted, Value: v},
			verify.Record{State: verify.StateIndeterminate, Value: v},
		)
	}
	return recs
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name            string
		records         []verify.Record
		observed        []int64
		wantVerdict     verify.Verdict
		wantLost        []int64
		wantUnexpected  []int64
		wantRecovered   []int64
		wantExit        int
		wantOrphans     []int64
		wantConflicting []int64
	}{
		{
			name:        "every committed value survived",
			records:     committed(1, 2, 3),
			observed:    []int64{1, 2, 3},
			wantVerdict: verify.VerdictPass,
			wantExit:    verify.ExitPass,
		},
		{
			name:        "a lost committed transaction is a durability violation",
			records:     committed(1, 2, 3),
			observed:    []int64{1, 3},
			wantVerdict: verify.VerdictFail,
			wantLost:    []int64{2},
			wantExit:    verify.ExitDurabilityViolation,
		},
		{
			name:           "a value never attempted is an unexpected write",
			records:        committed(1, 2),
			observed:       []int64{1, 2, 99},
			wantVerdict:    verify.VerdictFail,
			wantUnexpected: []int64{99},
			wantExit:       verify.ExitUnexpectedWrite,
		},
		{
			name:           "a lost commit outranks an unexpected write",
			records:        committed(1, 2),
			observed:       []int64{1, 99},
			wantVerdict:    verify.VerdictFail,
			wantLost:       []int64{2},
			wantUnexpected: []int64{99},
			wantExit:       verify.ExitDurabilityViolation,
		},
		{
			name:          "an indeterminate value that landed is recovered, not a failure",
			records:       append(committed(1, 2), indeterminate(3)...),
			observed:      []int64{1, 2, 3},
			wantVerdict:   verify.VerdictPass,
			wantRecovered: []int64{3},
			wantExit:      verify.ExitPass,
		},
		{
			name:        "an indeterminate value that did not land is not a failure either",
			records:     append(committed(1, 2), indeterminate(3)...),
			observed:    []int64{1, 2},
			wantVerdict: verify.VerdictPass,
			wantExit:    verify.ExitPass,
		},
		{
			name:          "an attempted value with no outcome that landed is recovered",
			records:       append(committed(1), verify.Record{State: verify.StateAttempted, Value: 2}),
			observed:      []int64{1, 2},
			wantVerdict:   verify.VerdictPass,
			wantRecovered: []int64{2},
			wantExit:      verify.ExitPass,
		},
		{
			name:        "an empty database loses every committed value",
			records:     committed(1, 2, 3),
			observed:    nil,
			wantVerdict: verify.VerdictFail,
			wantLost:    []int64{1, 2, 3},
			wantExit:    verify.ExitDurabilityViolation,
		},
		{
			name:           "an outcome with no preceding attempt is a verifier bug and reads as an unexpected write",
			records:        []verify.Record{{State: verify.StateCommitted, Value: 5}},
			observed:       []int64{5},
			wantVerdict:    verify.VerdictFail,
			wantUnexpected: []int64{5},
			wantExit:       verify.ExitUnexpectedWrite,
			wantOrphans:    []int64{5},
		},
		{
			name: "a value recorded both committed and indeterminate is a verifier bug",
			records: []verify.Record{
				{State: verify.StateAttempted, Value: 5},
				{State: verify.StateIndeterminate, Value: 5},
				{State: verify.StateCommitted, Value: 5},
			},
			observed:        []int64{5},
			wantVerdict:     verify.VerdictPass,
			wantExit:        verify.ExitPass,
			wantConflicting: []int64{5},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := verify.Check(verify.Summarize(tc.records), tc.observed, verify.CheckOptions{})

			if report.Verdict != tc.wantVerdict {
				t.Fatalf("Verdict = %s, want %s", report.Verdict, tc.wantVerdict)
			}
			if !slices.Equal(report.LostCommitted, orEmpty(tc.wantLost)) {
				t.Fatalf("LostCommitted = %v, want %v", report.LostCommitted, tc.wantLost)
			}
			if !slices.Equal(report.Unexpected, orEmpty(tc.wantUnexpected)) {
				t.Fatalf("Unexpected = %v, want %v", report.Unexpected, tc.wantUnexpected)
			}
			if !slices.Equal(report.Recovered, orEmpty(tc.wantRecovered)) {
				t.Fatalf("Recovered = %v, want %v", report.Recovered, tc.wantRecovered)
			}
			if !slices.Equal(report.LedgerOrphans, tc.wantOrphans) {
				t.Fatalf("LedgerOrphans = %v, want %v", report.LedgerOrphans, tc.wantOrphans)
			}
			if !slices.Equal(report.LedgerConflicts, tc.wantConflicting) {
				t.Fatalf("LedgerConflicts = %v, want %v", report.LedgerConflicts, tc.wantConflicting)
			}
			if got := report.ExitCode(); got != tc.wantExit {
				t.Fatalf("ExitCode = %d, want %d", got, tc.wantExit)
			}
			if report.DurabilityViolation != (len(tc.wantLost) > 0) {
				t.Fatalf("DurabilityViolation = %v, want %v", report.DurabilityViolation, len(tc.wantLost) > 0)
			}
		})
	}
}

func TestCheckCountsAreReportedOnAPass(t *testing.T) {
	records := append(committed(1, 2, 3), indeterminate(4)...)
	report := verify.Check(verify.Summarize(records), []int64{1, 2, 3, 4}, verify.CheckOptions{})

	want := verify.Counts{Attempted: 4, Committed: 3, Indeterminate: 1, Observed: 4, Recovered: 1}
	if report.Counts != want {
		t.Fatalf("Counts = %+v, want %+v", report.Counts, want)
	}
}

func TestCheckCapsTheInformationalRecoveredSample(t *testing.T) {
	var values []int64
	for v := int64(1); v <= 10; v++ {
		values = append(values, v)
	}
	report := verify.Check(verify.Summarize(indeterminate(values...)), values, verify.CheckOptions{MaxRecoveredListed: 4})

	if len(report.Recovered) != 4 {
		t.Fatalf("listed %d recovered values, want 4", len(report.Recovered))
	}
	if report.RecoveredOmitted != 6 {
		t.Fatalf("RecoveredOmitted = %d, want 6", report.RecoveredOmitted)
	}
	if report.Counts.Recovered != 10 {
		t.Fatalf("Counts.Recovered = %d, want 10", report.Counts.Recovered)
	}
}

func TestReportTextNamesTheOffendingValues(t *testing.T) {
	report := verify.Check(verify.Summarize(committed(1, 2, 3)), []int64{1, 3, 77}, verify.CheckOptions{})

	text := report.Text()
	for _, want := range []string{"FAIL", "LOST COMMITTED TRANSACTIONS (1): 2", "UNEXPECTED VALUES (1): 77"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report text missing %q:\n%s", want, text)
		}
	}
}

func orEmpty(values []int64) []int64 {
	if values == nil {
		return []int64{}
	}
	return values
}
