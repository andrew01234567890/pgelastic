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
	"slices"
	"strconv"
	"strings"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// VerifierSetup pins every setting that can change how a value is rendered as text, and is
// applied to BOTH sides from this one string.
//
// One string rather than two lists is the point. The verifier's whole claim rests on the
// two sides rendering the same row to the same bytes, and the settings below are each
// enough on their own to break that: a different DateStyle renders a date differently, a
// different TimeZone renders a timestamptz differently, a different extra_float_digits
// truncates a double. Two independently maintained setup lists would drift, and the drift
// would show up as a checksum mismatch nobody could explain - or, worse, as a match that
// only held because both sides happened to be misconfigured the same way.
//
// search_path is emptied because a bare type name in a rendered value would otherwise
// resolve differently under different schemas.
const VerifierSetup = `SET search_path = pg_catalog; ` +
	`SET DateStyle = 'ISO, MDY'; ` +
	`SET IntervalStyle = 'postgres'; ` +
	`SET TimeZone = 'UTC'; ` +
	`SET extra_float_digits = 1; ` +
	`SET bytea_output = 'hex'; ` +
	`SET client_encoding = 'UTF8'; ` +
	`SET lc_monetary = 'C'; ` +
	`SET lc_numeric = 'C'; ` +
	`SET lc_time = 'C'; ` +
	`SET standard_conforming_strings = on`

// VerifierLimits is what an equivalence verdict does NOT prove. It is exported and
// published on the migration's Verified condition because a verdict whose limits are not
// stated will be read as a stronger claim than it is.
//
// Equivalence is not correctness. Every item here is invisible to a content checksum:
//
//   - Physical row identity: ctid, xmin, xmax and cmin/cmax all differ after a logical
//     copy, and nothing in the product preserves them.
//   - TOAST layout: whether a wide value is stored inline, compressed or out of line, and
//     which compression method was used, is not observable through the value.
//   - Index physical structure: page layout, fill factor and leaf ordering are rebuilt on
//     the target, so an index that is corrupt on the source is silently rebuilt correct -
//     and one that is fine on the source can be rebuilt differently.
//   - Bloat and free space: the target is freshly written and will not resemble the source.
//   - Planner statistics: pg_statistic is not copied, so the target's first plans differ
//     until ANALYZE runs.
//   - Anything written after the checksum LSN: the verdict describes the instant the
//     checksums were taken, which is inside the cutover pause. It says nothing about a
//     write that arrives afterwards.
//   - Pre-existing corruption: a source whose data was already wrong produces a target that
//     is equivalently wrong, and this verifier will pass it.
var VerifierLimits = []string{
	"ctid, xmin, xmax and cmin/cmax differ after any logical copy and are not compared",
	"TOAST storage layout and compression method are not observable through the value",
	"index physical structure, page layout and leaf ordering are rebuilt on the target",
	"bloat and free-space distribution are not compared",
	"planner statistics are not copied and are not compared",
	"nothing written after the checksum LSN is covered by the verdict",
	"a source that was already corrupt yields an equivalently corrupt target that passes",
}

// LimitsMessage renders the limits for a condition message.
func LimitsMessage() string {
	return "equivalence is not correctness: " + strings.Join(VerifierLimits, "; ")
}

// VerificationLevel is how much evidence is gathered. It maps onto the pool's
// migration.verification setting.
type VerificationLevel string

const (
	// VerifySchema compares the ordered catalog digest only.
	VerifySchema VerificationLevel = "Schema"
	// VerifyRowCounts adds per-relation row counts.
	VerifyRowCounts VerificationLevel = "RowCounts"
	// VerifyChecksums adds a per-relation content digest. It reads every row on both sides
	// inside the cutover pause, so it is the level that trades pause for evidence.
	VerifyChecksums VerificationLevel = "Checksums"
)

// VerificationLevelFor maps the pool's setting onto a level, defaulting to row counts.
func VerificationLevelFor(setting pgelasticv1alpha1.MigrationVerification) VerificationLevel {
	switch setting {
	case pgelasticv1alpha1.MigrationVerifySchema:
		return VerifySchema
	case pgelasticv1alpha1.MigrationVerifyChecksums:
		return VerifyChecksums
	default:
		return VerifyRowCounts
	}
}

// Relation is one verified relation.
type Relation struct {
	Schema string
	Name   string
}

// Qualified is the schema-qualified, quoted name.
func (r Relation) Qualified() string { return QuoteQualified(r.Schema, r.Name) }

func (r Relation) String() string { return r.Schema + "." + r.Name }

// Difference is one place the two sides disagree.
type Difference struct {
	Relation string
	Source   string
	Target   string
	What     string
}

func (d Difference) String() string {
	if d.Relation == "" {
		return fmt.Sprintf("%s differs: source %s, target %s", d.What, d.Source, d.Target)
	}
	return fmt.Sprintf("%s of %s differs: source %s, target %s", d.What, d.Relation, d.Source, d.Target)
}

// VerificationResult is the evidence and the verdict.
type VerificationResult struct {
	Level VerificationLevel
	// SchemaFingerprintMatch is nil when the level did not gather it.
	SchemaFingerprintMatch *bool
	RowCountsMatch         *bool
	ChecksumsMatch         *bool
	Relations              int
	Differences            []Difference
}

// Equivalent reports whether every check that ran agreed.
func (r VerificationResult) Equivalent() bool { return len(r.Differences) == 0 }

// Message is the Verified condition's message. It states the verdict, the evidence behind
// it and the limits of the claim, in that order.
func (r VerificationResult) Message() string {
	var builder strings.Builder
	if r.Equivalent() {
		fmt.Fprintf(&builder, "the target is equivalent to the source at %s level over %d relation(s). ",
			r.Level, r.Relations)
	} else {
		differences := make([]string, 0, len(r.Differences))
		for _, difference := range r.Differences {
			differences = append(differences, difference.String())
		}
		fmt.Fprintf(&builder, "the target is NOT equivalent to the source: %s. ",
			strings.Join(differences, "; "))
	}
	builder.WriteString(LimitsMessage())
	return builder.String()
}

// APIVerification projects the result onto the CR's status shape.
func (r VerificationResult) APIVerification() *pgelasticv1alpha1.TenantMigrationVerification {
	return &pgelasticv1alpha1.TenantMigrationVerification{
		SchemaFingerprintMatch: r.SchemaFingerprintMatch,
		RowCountsMatch:         r.RowCountsMatch,
		ChecksumsMatch:         r.ChecksumsMatch,
	}
}

// Verifier compares two databases. It is a product feature rather than a test helper: its
// verdict is what gates the routing flip, and its limits are published alongside it.
type Verifier struct {
	SQL   SQL
	Level VerificationLevel
}

// Verify gathers the evidence its level calls for and returns the verdict.
func (v Verifier) Verify(ctx context.Context, source, target Endpoint) (VerificationResult, error) {
	result := VerificationResult{Level: v.Level}

	sourceFingerprint, err := v.schemaFingerprint(ctx, source)
	if err != nil {
		return result, fmt.Errorf("fingerprinting the source schema: %w", err)
	}
	targetFingerprint, err := v.schemaFingerprint(ctx, target)
	if err != nil {
		return result, fmt.Errorf("fingerprinting the target schema: %w", err)
	}
	match := sourceFingerprint == targetFingerprint
	result.SchemaFingerprintMatch = &match
	if !match {
		result.Differences = append(result.Differences, Difference{
			What: "schema fingerprint", Source: sourceFingerprint, Target: targetFingerprint})
	}

	relations, err := v.relations(ctx, source)
	if err != nil {
		return result, fmt.Errorf("listing the source's relations: %w", err)
	}
	result.Relations = len(relations)
	if v.Level == VerifySchema {
		return result, nil
	}

	counted := true
	for _, relation := range relations {
		sourceCount, targetCount, err := v.rowCounts(ctx, source, target, relation)
		if err != nil {
			return result, err
		}
		if sourceCount != targetCount {
			counted = false
			result.Differences = append(result.Differences, Difference{
				Relation: relation.String(), What: "row count",
				Source: strconv.FormatInt(sourceCount, 10), Target: strconv.FormatInt(targetCount, 10)})
		}
	}
	result.RowCountsMatch = &counted
	if v.Level != VerifyChecksums {
		return result, nil
	}

	checksummed := true
	for _, relation := range relations {
		sourceDigest, err := v.checksum(ctx, source, relation)
		if err != nil {
			return result, err
		}
		targetDigest, err := v.checksum(ctx, target, relation)
		if err != nil {
			return result, err
		}
		if sourceDigest != targetDigest {
			checksummed = false
			result.Differences = append(result.Differences, Difference{
				Relation: relation.String(), What: "content checksum",
				Source: sourceDigest, Target: targetDigest})
		}
	}
	result.ChecksumsMatch = &checksummed
	return result, nil
}

const relationsQuery = `SELECT n.nspname, c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND c.relispartition = false AND ` + UserSchemaPredicate + `
ORDER BY n.nspname, c.relname`

func (v Verifier) relations(ctx context.Context, at Endpoint) ([]Relation, error) {
	rows, err := v.SQL.Query(ctx, at, relationsQuery)
	if err != nil {
		return nil, err
	}
	relations := make([]Relation, 0, len(rows))
	for _, row := range rows {
		if len(row) != 2 {
			return nil, fmt.Errorf("unreadable relation record %q", strings.Join(row, "|"))
		}
		relations = append(relations, Relation{Schema: row[0], Name: row[1]})
	}
	return relations, nil
}

// schemaFingerprintParts are the three catalog projections the fingerprint is built from.
// Each is aggregated with COLLATE "C" on the ordering for the same reason the content
// checksum is: a glibc or ICU difference between the two instances would otherwise change
// the aggregation order and produce a mismatch that has nothing to do with the schema.
var schemaFingerprintParts = []string{
	// attidentity and attgenerated are the "char" type, and "text || char" is ambiguous
	// enough that PostgreSQL refuses to choose an operator at all, so both are cast.
	`SELECT n.nspname || '.' || c.relname || '.' || a.attnum::text || ':' || a.attname || ':' ||
	   format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull::text || ':' ||
	   coalesce(pg_get_expr(d.adbin, d.adrelid), '') || ':' || a.attidentity::text || ':' ||
	   a.attgenerated::text
	 FROM pg_attribute a
	   JOIN pg_class c ON c.oid = a.attrelid
	   JOIN pg_namespace n ON n.oid = c.relnamespace
	   LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
	 WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped AND ` + UserSchemaPredicate,

	`SELECT n.nspname || '.' || c.relname || '.' || con.conname || ':' || pg_get_constraintdef(con.oid)
	 FROM pg_constraint con
	   JOIN pg_class c ON c.oid = con.conrelid
	   JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE ` + UserSchemaPredicate,

	`SELECT n.nspname || '.' || c.relname || ':' || pg_get_indexdef(i.indexrelid)
	 FROM pg_index i
	   JOIN pg_class c ON c.oid = i.indexrelid
	   JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE ` + UserSchemaPredicate,

	`SELECT 'sequence:' || schemaname || '.' || sequencename || ':' || data_type::text || ':' ||
	   increment_by::text || ':' || min_value::text || ':' || max_value::text
	 FROM pg_sequences`,
}

func (v Verifier) schemaFingerprint(ctx context.Context, at Endpoint) (string, error) {
	digests := make([]string, 0, len(schemaFingerprintParts))
	for _, part := range schemaFingerprintParts {
		statement := fmt.Sprintf(
			`%s; SELECT coalesce(md5(string_agg(entry, E'\n' ORDER BY entry COLLATE "C")), '')
			 FROM (%s) AS parts(entry)`, VerifierSetup, part)
		digest, err := scalar(ctx, v.SQL, at, statement)
		if err != nil {
			return "", err
		}
		digests = append(digests, digest)
	}
	slices.Sort(digests)
	return strings.Join(digests, ":"), nil
}

func (v Verifier) rowCounts(
	ctx context.Context, source, target Endpoint, relation Relation,
) (int64, int64, error) {
	statement := fmt.Sprintf(`SELECT count(*)::text FROM %s`, relation.Qualified())
	sourceCount, err := scalarInt64(ctx, v.SQL, source, statement)
	if err != nil {
		return 0, 0, fmt.Errorf("counting %s on the source: %w", relation, err)
	}
	targetCount, err := scalarInt64(ctx, v.SQL, target, statement)
	if err != nil {
		return 0, 0, fmt.Errorf("counting %s on the target: %w", relation, err)
	}
	return sourceCount, targetCount, nil
}

// checksum digests one relation's whole content.
//
// COLLATE "C" on the ORDER BY is the load-bearing part. string_agg's order is the only
// thing making the digest deterministic, and ordering rendered rows under a locale-aware
// collation makes that order a function of the instance's glibc or ICU version. Two
// instances that agree on every byte of data would then disagree on the digest - or, far
// worse, two instances that differ could agree, because the aggregation order absorbed the
// difference. The C collation is byte ordering and is the same everywhere.
func (v Verifier) checksum(ctx context.Context, at Endpoint, relation Relation) (string, error) {
	statement := fmt.Sprintf(
		`%s; SELECT coalesce(md5(string_agg(rendered, E'\n' ORDER BY rendered COLLATE "C")), '')
		 FROM (SELECT t::text AS rendered FROM %s AS t) AS rendered_rows`,
		VerifierSetup, relation.Qualified())
	digest, err := scalar(ctx, v.SQL, at, statement)
	if err != nil {
		return "", fmt.Errorf("checksumming %s: %w", relation, err)
	}
	return digest, nil
}
