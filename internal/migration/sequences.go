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
	"fmt"
	"strconv"
	"strings"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// DefaultSafetyGap is the distance every target sequence is advanced past the source's
// next value. It has to exceed the largest CACHE any tenant backend holds, because a cached
// block of values was handed out to a client and never written to WAL, so the source's
// recorded position understates what it has actually issued.
const DefaultSafetyGap int64 = 1000

// Sequence is one sequence's position on the source.
type Sequence struct {
	Schema string
	Name   string
	// Next is the value the source's nextval would return. It is derived rather than read,
	// because the two cases - a sequence that has been used and one that has not - report
	// their position in different columns.
	Next int64
}

// Qualified is the schema-qualified, quoted name.
func (s Sequence) Qualified() string { return QuoteQualified(s.Schema, s.Name) }

// sequenceQuery reads both positions at once. pg_sequences.last_value is NULL until the
// sequence has been read from, in which case the next value is start_value.
const sequenceQuery = `SELECT schemaname, sequencename,
  (CASE WHEN last_value IS NULL THEN start_value ELSE last_value + increment_by END)::text
FROM pg_sequences ORDER BY schemaname, sequencename`

// ReadSequences lists every sequence on one side with the value its nextval would return.
//
// This exists because logical replication carries no sequence state through PostgreSQL 18;
// synchronization lands in PostgreSQL 19. A target sequence therefore sits wherever the
// schema copy left it, which is its creation-time value. Skipping the reconciliation
// produces duplicate key violations hours later, once the tenant's inserts catch up with
// rows that were copied in - long after the migration was declared successful, and with
// nothing left pointing at the migration as the cause.
func ReadSequences(ctx context.Context, sql SQL, at Endpoint) ([]Sequence, error) {
	rows, err := sql.Query(ctx, at, sequenceQuery)
	if err != nil {
		return nil, err
	}
	sequences := make([]Sequence, 0, len(rows))
	for _, row := range rows {
		if len(row) != 3 {
			return nil, fmt.Errorf("unreadable sequence record %q", strings.Join(row, "|"))
		}
		next, err := strconv.ParseInt(strings.TrimSpace(row[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unreadable sequence position in %q: %w", row[2], err)
		}
		sequences = append(sequences, Sequence{Schema: row[0], Name: row[1], Next: next})
	}
	return sequences, nil
}

// ApplySequences advances every target sequence past the source's next value plus the gap.
//
// is_called is false so the very next nextval returns exactly the value set here, which
// makes the gap the whole and only distance between the last value the source could have
// issued and the first the target will.
func ApplySequences(ctx context.Context, sql SQL, at Endpoint, sequences []Sequence, gap int64) error {
	for _, sequence := range sequences {
		statement := fmt.Sprintf(`SELECT setval(%s, %d, false)`,
			QuoteLiteral(sequence.Qualified()), sequence.Next+gap)
		if err := sql.Exec(ctx, at, statement); err != nil {
			return fmt.Errorf("advancing sequence %s: %w", sequence.Qualified(), err)
		}
	}
	return nil
}

// SequencePlan is the sequence work one cutover has to do.
type SequencePlan struct {
	Mode      pgelasticv1alpha1.SequenceHandlingMode
	SafetyGap int64
}

// Reconcile carries the source's sequence positions onto the target.
//
// Skip is honoured, but only ever correct for a tenant with no sequences at all, so a Skip
// that meets a sequence is an error rather than a no-op: silently skipping is precisely the
// failure mode this whole step exists to prevent.
func (p SequencePlan) Reconcile(ctx context.Context, sql SQL, source, target Endpoint) (int, error) {
	sequences, err := ReadSequences(ctx, sql, source)
	if err != nil {
		return 0, err
	}
	if p.Mode == pgelasticv1alpha1.SequenceHandlingSkip {
		if len(sequences) > 0 {
			return 0, fmt.Errorf(
				"sequenceHandling.mode is Skip but the tenant has %d sequence(s); logical replication "+
					"carries no sequence state through PostgreSQL 18, so skipping would produce duplicate "+
					"key violations hours after this migration was declared successful", len(sequences))
		}
		return 0, nil
	}
	gap := p.SafetyGap
	if gap < 0 {
		gap = DefaultSafetyGap
	}
	return len(sequences), ApplySequences(ctx, sql, target, sequences, gap)
}
