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
)

// recordingSQL answers every verifier query the same on both sides unless a per-endpoint
// override says otherwise, and keeps every statement for inspection.
type recordingSQL struct {
	shared    *fakeSQL
	overrides map[string]*fakeSQL
	seen      []string
}

func (r *recordingSQL) pick(at Endpoint) *fakeSQL {
	if fake, ok := r.overrides[at.Instance]; ok {
		return fake
	}
	return r.shared
}

func (r *recordingSQL) Exec(ctx context.Context, at Endpoint, statement string) error {
	r.seen = append(r.seen, statement)
	return r.pick(at).Exec(ctx, at, statement)
}

func (r *recordingSQL) Query(ctx context.Context, at Endpoint, statement string) ([]Row, error) {
	r.seen = append(r.seen, statement)
	return r.pick(at).Query(ctx, at, statement)
}

func (r *recordingSQL) sawFragment(fragment string) bool {
	for _, statement := range r.seen {
		if strings.Contains(statement, fragment) {
			return true
		}
	}
	return false
}

func equivalentSides() *recordingSQL {
	shared := newFakeSQL().
		scalarAnswer("string_agg(entry", "d41d8cd98f00b204e9800998ecf8427e").
		answer(relationsQuery, Row{userSchema, ordersRelation}).
		scalarAnswer("SELECT count(*)::text FROM", "17").
		scalarAnswer("string_agg(rendered", "9e107d9d372bb6826bd81d3542a419d6")
	return &recordingSQL{shared: shared, overrides: map[string]*fakeSQL{}}
}

func TestTheVerifierPassesTwoEquivalentSides(t *testing.T) {
	result, err := Verifier{SQL: equivalentSides(), Level: VerifyChecksums}.
		Verify(context.Background(), sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Equivalent() {
		t.Fatalf("two equivalent sides were reported different: %s", result.Message())
	}
	if result.SchemaFingerprintMatch == nil || !*result.SchemaFingerprintMatch {
		t.Fatal("the schema fingerprint was not reported as matching")
	}
	if result.RowCountsMatch == nil || !*result.RowCountsMatch {
		t.Fatal("the row counts were not reported as matching")
	}
	if result.ChecksumsMatch == nil || !*result.ChecksumsMatch {
		t.Fatal("the checksums were not reported as matching")
	}
}

// TestEveryChecksumOrdersUnderTheCCollation is the load-bearing property. Without COLLATE
// "C" the aggregation order is a function of the instance's glibc or ICU version, which
// makes the digest a claim about the locale rather than about the data.
func TestEveryChecksumOrdersUnderTheCCollation(t *testing.T) {
	sql := equivalentSides()
	if _, err := (Verifier{SQL: sql, Level: VerifyChecksums}).
		Verify(context.Background(), sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	for _, statement := range sql.seen {
		if !strings.Contains(statement, "string_agg") {
			continue
		}
		if !strings.Contains(statement, `COLLATE "C"`) {
			t.Fatalf("an aggregation is ordered without COLLATE \"C\": %s", statement)
		}
	}
}

// TestBothSidesArePinnedFromOneSetupString guards against the two halves drifting apart,
// which would show up either as an unexplainable mismatch or, worse, as a match that only
// held because both sides were misconfigured the same way.
func TestBothSidesArePinnedFromOneSetupString(t *testing.T) {
	sql := equivalentSides()
	if _, err := (Verifier{SQL: sql, Level: VerifyChecksums}).
		Verify(context.Background(), sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	var digests int
	for _, statement := range sql.seen {
		if !strings.Contains(statement, "string_agg") {
			continue
		}
		digests++
		if !strings.HasPrefix(strings.TrimSpace(statement), VerifierSetup) {
			t.Fatalf("a digest was taken without the shared setup string: %s", statement)
		}
	}
	if digests == 0 {
		t.Fatal("no digest was taken at all")
	}
}

func TestTheSetupStringPinsEveryTextRenderingSetting(t *testing.T) {
	for _, guc := range []string{
		"search_path", "DateStyle", "IntervalStyle", "TimeZone", "extra_float_digits",
		"bytea_output", "client_encoding", "lc_monetary", "lc_numeric", "lc_time",
		"standard_conforming_strings",
	} {
		if !strings.Contains(VerifierSetup, guc) {
			t.Fatalf("%s is not pinned, so the two sides can render the same value differently", guc)
		}
	}
}

func TestASchemaDifferenceIsReported(t *testing.T) {
	sql := equivalentSides()
	sql.overrides[targetAt.Instance] = newFakeSQL().
		scalarAnswer("string_agg(entry", "ffffffffffffffffffffffffffffffff")
	result, err := Verifier{SQL: sql, Level: VerifySchema}.Verify(context.Background(), sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if result.Equivalent() {
		t.Fatal("a schema difference was reported as equivalence")
	}
	if !strings.Contains(result.Message(), "schema fingerprint") {
		t.Fatalf("the verdict does not say what differed: %s", result.Message())
	}
}

func TestARowCountDifferenceIsReportedWithTheRelation(t *testing.T) {
	sql := equivalentSides()
	sql.overrides[targetAt.Instance] = newFakeSQL().
		scalarAnswer("string_agg(entry", "d41d8cd98f00b204e9800998ecf8427e").
		answer(relationsQuery, Row{userSchema, ordersRelation}).
		scalarAnswer("SELECT count(*)::text FROM", "16")
	result, err := Verifier{SQL: sql, Level: VerifyRowCounts}.Verify(context.Background(), sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if result.Equivalent() {
		t.Fatal("a row count difference was reported as equivalence")
	}
	if !strings.Contains(result.Message(), "public.orders") {
		t.Fatalf("the verdict does not name the relation: %s", result.Message())
	}
}

func TestAContentDifferenceIsReported(t *testing.T) {
	sql := equivalentSides()
	sql.overrides[targetAt.Instance] = newFakeSQL().
		scalarAnswer("string_agg(entry", "d41d8cd98f00b204e9800998ecf8427e").
		answer(relationsQuery, Row{userSchema, ordersRelation}).
		scalarAnswer("SELECT count(*)::text FROM", "17").
		scalarAnswer("string_agg(rendered", "00000000000000000000000000000000")
	result, err := Verifier{SQL: sql, Level: VerifyChecksums}.Verify(context.Background(), sourceAt, targetAt)
	if err != nil {
		t.Fatal(err)
	}
	if result.Equivalent() {
		t.Fatal("a content difference was reported as equivalence")
	}
}

func TestTheSchemaLevelDoesNotPayForRowCounts(t *testing.T) {
	sql := equivalentSides()
	if _, err := (Verifier{SQL: sql, Level: VerifySchema}).
		Verify(context.Background(), sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	if sql.sawFragment("SELECT count(*)::text FROM") {
		t.Fatal("the schema level counted rows, which is pause spent on evidence nobody asked for")
	}
}

func TestTheRowCountLevelDoesNotPayForChecksums(t *testing.T) {
	sql := equivalentSides()
	if _, err := (Verifier{SQL: sql, Level: VerifyRowCounts}).
		Verify(context.Background(), sourceAt, targetAt); err != nil {
		t.Fatal(err)
	}
	if sql.sawFragment("string_agg(rendered") {
		t.Fatal("the row count level checksummed every row inside the cutover pause")
	}
}

// TestTheVerdictAlwaysStatesItsLimits keeps the claim honest. Equivalence is not
// correctness, and a verdict read without its limits will be taken for more than it is.
func TestTheVerdictAlwaysStatesItsLimits(t *testing.T) {
	for _, result := range []VerificationResult{
		{Level: VerifyChecksums},
		{Level: VerifyChecksums, Differences: []Difference{{What: "row count", Relation: "public.orders"}}},
	} {
		message := result.Message()
		if !strings.Contains(message, "equivalence is not correctness") {
			t.Fatalf("a verdict was published without its limits: %s", message)
		}
		for _, limit := range []string{"ctid", "TOAST", "index physical structure", "already corrupt"} {
			if !strings.Contains(message, limit) {
				t.Fatalf("the limits do not mention %q: %s", limit, message)
			}
		}
	}
}
