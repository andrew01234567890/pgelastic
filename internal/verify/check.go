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

package verify

import (
	"fmt"
	"slices"
	"strings"
)

// Verdict is the top-level result of a check.
type Verdict string

const (
	// VerdictPass means no committed transaction was lost and no unexpected value appeared.
	VerdictPass Verdict = "PASS"
	// VerdictFail means at least one assertion was violated.
	VerdictFail Verdict = "FAIL"
)

// Exit codes. They are part of the tool's contract with CI.
const (
	// ExitPass is returned when both assertions hold.
	ExitPass = 0
	// ExitDurabilityViolation is returned when COMMITTED ⊄ R. This is the code that fails a release.
	ExitDurabilityViolation = 1
	// ExitUnexpectedWrite is returned when R ⊄ ATTEMPTED and no commit was lost.
	ExitUnexpectedWrite = 2
	// ExitOperational is returned when the check could not be completed at all.
	ExitOperational = 3
)

// Summary is the set view of a replayed ledger.
type Summary struct {
	Attempted     map[int64]struct{}
	Committed     map[int64]struct{}
	Indeterminate map[int64]struct{}
	// Orphans are values recorded COMMITTED or INDETERMINATE with no preceding ATTEMPTED.
	// They can only be a bug in the verifier, and they would silently weaken R ⊆ ATTEMPTED.
	Orphans []int64
	// Conflicts are values recorded both COMMITTED and INDETERMINATE, likewise a verifier bug.
	Conflicts []int64
}

// Summarize folds a replayed ledger into sets, preserving the ordering constraint that
// makes an orphan detectable: ATTEMPTED must precede any outcome for the same value.
func Summarize(recs []Record) Summary {
	s := Summary{
		Attempted:     map[int64]struct{}{},
		Committed:     map[int64]struct{}{},
		Indeterminate: map[int64]struct{}{},
	}
	orphans := map[int64]struct{}{}
	for _, rec := range recs {
		switch rec.State {
		case StateAttempted:
			s.Attempted[rec.Value] = struct{}{}
			continue
		case StateCommitted:
			s.Committed[rec.Value] = struct{}{}
		case StateIndeterminate:
			s.Indeterminate[rec.Value] = struct{}{}
		}
		if _, ok := s.Attempted[rec.Value]; !ok {
			orphans[rec.Value] = struct{}{}
		}
	}
	s.Orphans = sortedKeys(orphans)
	for v := range s.Committed {
		if _, ok := s.Indeterminate[v]; ok {
			s.Conflicts = append(s.Conflicts, v)
		}
	}
	slices.Sort(s.Conflicts)
	return s
}

// Counts is the numeric shape of a run, always reported even on a pass.
type Counts struct {
	Attempted     int `json:"attempted"`
	Committed     int `json:"committed"`
	Indeterminate int `json:"indeterminate"`
	Observed      int `json:"observed"`
	Lost          int `json:"lost"`
	Unexpected    int `json:"unexpected"`
	Recovered     int `json:"recovered"`
}

// Report is the machine-readable result of a check.
type Report struct {
	Verdict Verdict `json:"verdict"`
	// DurabilityViolation is the only field that fails a release: COMMITTED ⊄ R.
	DurabilityViolation bool   `json:"durabilityViolation"`
	UnexpectedWrites    bool   `json:"unexpectedWrites"`
	Counts              Counts `json:"counts"`
	// LostCommitted is reported in full: every lost value matters.
	LostCommitted []int64 `json:"lostCommitted"`
	// Unexpected is R − ATTEMPTED, reported in full.
	Unexpected []int64 `json:"unexpected"`
	// Recovered is (R ∩ ATTEMPTED) − COMMITTED: indeterminate writes that turned out to
	// be durable. Informational, and expected to be non-empty after any real chaos.
	Recovered        []int64 `json:"recovered"`
	RecoveredOmitted int     `json:"recoveredOmitted"`
	LedgerOrphans    []int64 `json:"ledgerOrphans,omitempty"`
	LedgerConflicts  []int64 `json:"ledgerConflicts,omitempty"`
}

// CheckOptions tunes reporting only. It cannot change either assertion.
type CheckOptions struct {
	// MaxRecoveredListed caps the informational RECOVERED sample; 0 means the default.
	MaxRecoveredListed int
}

const defaultMaxRecoveredListed = 100

// Check runs the two assertions of the patroni-set checker against the observed set R.
//
//	COMMITTED ⊆ R  — no lost committed transaction
//	R ⊆ ATTEMPTED  — no unexpected write
//
// RECOVERED = (R ∩ ATTEMPTED) − COMMITTED is reported but never fails the run.
func Check(s Summary, observed []int64, opts CheckOptions) Report {
	maxRecovered := opts.MaxRecoveredListed
	if maxRecovered <= 0 {
		maxRecovered = defaultMaxRecoveredListed
	}

	obs := make(map[int64]struct{}, len(observed))
	for _, v := range observed {
		obs[v] = struct{}{}
	}

	lost := difference(s.Committed, obs)
	unexpected := difference(obs, s.Attempted)

	recovered := make([]int64, 0)
	for v := range obs {
		_, attempted := s.Attempted[v]
		_, committed := s.Committed[v]
		if attempted && !committed {
			recovered = append(recovered, v)
		}
	}
	slices.Sort(recovered)

	rep := Report{
		Verdict:             VerdictPass,
		DurabilityViolation: len(lost) > 0,
		UnexpectedWrites:    len(unexpected) > 0,
		Counts: Counts{
			Attempted:     len(s.Attempted),
			Committed:     len(s.Committed),
			Indeterminate: len(s.Indeterminate),
			Observed:      len(obs),
			Lost:          len(lost),
			Unexpected:    len(unexpected),
			Recovered:     len(recovered),
		},
		LostCommitted:   lost,
		Unexpected:      unexpected,
		LedgerOrphans:   s.Orphans,
		LedgerConflicts: s.Conflicts,
	}
	if len(recovered) > maxRecovered {
		rep.RecoveredOmitted = len(recovered) - maxRecovered
		recovered = recovered[:maxRecovered]
	}
	rep.Recovered = recovered
	if rep.DurabilityViolation || rep.UnexpectedWrites {
		rep.Verdict = VerdictFail
	}
	return rep
}

// ExitCode maps a report onto the process exit status.
func (r Report) ExitCode() int {
	switch {
	case r.DurabilityViolation:
		return ExitDurabilityViolation
	case r.UnexpectedWrites:
		return ExitUnexpectedWrite
	default:
		return ExitPass
	}
}

// Text renders the report for a human reading CI output.
func (r Report) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "verdict: %s\n", r.Verdict)
	fmt.Fprintf(&b, "  attempted=%d committed=%d indeterminate=%d observed=%d\n",
		r.Counts.Attempted, r.Counts.Committed, r.Counts.Indeterminate, r.Counts.Observed)
	fmt.Fprintf(&b, "  COMMITTED subset of R: %s\n", assertionResult(!r.DurabilityViolation))
	if r.DurabilityViolation {
		fmt.Fprintf(&b, "    LOST COMMITTED TRANSACTIONS (%d): %s\n",
			r.Counts.Lost, formatValues(r.LostCommitted))
	}
	fmt.Fprintf(&b, "  R subset of ATTEMPTED: %s\n", assertionResult(!r.UnexpectedWrites))
	if r.UnexpectedWrites {
		fmt.Fprintf(&b, "    UNEXPECTED VALUES (%d): %s\n", r.Counts.Unexpected, formatValues(r.Unexpected))
	}
	fmt.Fprintf(&b, "  recovered (informational, not a failure): %d\n", r.Counts.Recovered)
	if len(r.LedgerOrphans) > 0 {
		fmt.Fprintf(&b, "  ledger orphans (verifier bug): %s\n", formatValues(r.LedgerOrphans))
	}
	if len(r.LedgerConflicts) > 0 {
		fmt.Fprintf(&b, "  ledger conflicts (verifier bug): %s\n", formatValues(r.LedgerConflicts))
	}
	return b.String()
}

func assertionResult(ok bool) string {
	if ok {
		return "holds"
	}
	return "VIOLATED"
}

func formatValues(values []int64) string {
	const maxShown = 32
	shown := values
	suffix := ""
	if len(shown) > maxShown {
		shown = shown[:maxShown]
		suffix = fmt.Sprintf(" ... and %d more", len(values)-maxShown)
	}
	parts := make([]string, 0, len(shown))
	for _, v := range shown {
		parts = append(parts, fmt.Sprint(v))
	}
	return strings.Join(parts, ", ") + suffix
}

func difference(a, b map[int64]struct{}) []int64 {
	out := make([]int64, 0)
	for v := range a {
		if _, ok := b[v]; !ok {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func sortedKeys(m map[int64]struct{}) []int64 {
	if len(m) == 0 {
		return nil
	}
	out := make([]int64, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}
