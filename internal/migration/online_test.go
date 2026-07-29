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

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// stampQueryFragment is long enough to outrank the plainer pg_database fragments the other
// fakes answer, so a spec that scripts a stamp gets its own answer rather than a count.
const stampQueryFragment = "shobj_description(oid, 'pg_database')"

func TestTheSchemaCopyCommitsTheWholeSchemaAndItsStampOrNeither(t *testing.T) {
	plan := testPlan()
	plan.DumpDir = "/var/lib/postgresql/data/pgelastic-migration/shop_move"
	command := schemaCopyCommand(plan)

	if !strings.Contains(command, "--single-transaction") {
		t.Fatalf("the schema is applied statement by statement, so an interrupted copy leaves "+
			"objects a retry then fails on: %s", command)
	}
	stampAt := strings.Index(command, "COMMENT ON DATABASE")
	applyAt := strings.Index(command, "psql ")
	if stampAt < 0 {
		t.Fatalf("the copy leaves no record that it finished: %s", command)
	}
	if stampAt > applyAt {
		t.Fatalf("the stamp is written outside the transaction that applies the schema, so it "+
			"can survive an apply that did not: %s", command)
	}
	file := shellQuote(plan.DumpDir + ".schema.sql")
	if !strings.Contains(command, ">> "+file+"; psql ") || !strings.Contains(command, "--file="+file) {
		t.Fatalf("the stamp is not appended to the file psql applies: %s", command)
	}
}

// TestASecondProvisioningPassLeavesTheSchemaAlone is the defect the nightly found: a phase
// that cannot survive its own retry is not retryable, and the retry budget behind it is
// decoration.
func TestASecondProvisioningPassLeavesTheSchemaAlone(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)
	run := testRun(provisioning, online)

	if fault := engine.Step(context.Background(), run).Observation.Fault; fault != nil {
		t.Fatal(fault)
	}
	sql.scalarAnswer(stampQueryFragment, "1")

	result := engine.Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatalf("provisioning a target it had already provisioned failed: %v", result.Observation.Fault)
	}
	if !result.Observation.Provisioned {
		t.Fatal("a target that already carries the schema was not reported provisioned")
	}
	if copies := strings.Count(shell.joined(), "pg_dump --schema-only"); copies != 1 {
		t.Fatalf("the schema was copied %d times onto the same target", copies)
	}
}

// TestAnInterruptedSchemaCopyIsCopiedAgainRatherThanTakenAsDone is the other half: the
// target of a copy that did not commit is unstamped, and an unstamped target is copied onto
// from scratch rather than assumed to be half-built and left.
func TestAnInterruptedSchemaCopyIsCopiedAgainRatherThanTakenAsDone(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{err: errFake}
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)
	run := testRun(provisioning, online)

	if engine.Step(context.Background(), run).Observation.Fault == nil {
		t.Fatal("a schema copy that failed was reported as a success")
	}

	shell.err = nil
	result := engine.Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatal(result.Observation.Fault)
	}
	if copies := strings.Count(shell.joined(), "pg_dump --schema-only"); copies != 2 {
		t.Fatalf("the retry ran %d copies; an interrupted copy has to be redone in full", copies)
	}
	if !result.Observation.Provisioned {
		t.Fatal("the retry did not finish provisioning")
	}
}

func TestTheCleanupLadderTakesTheStampOffTheDatabaseItHandsOver(t *testing.T) {
	plan := testPlan()
	sql := cleanableSQL().scalarAnswer(stampQueryFragment, "1")
	if err := Cleanup(context.Background(), sql, plan, provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	if sql.ran("COMMENT ON DATABASE") < 0 {
		t.Fatal("the tenant was handed a database still carrying the migration's mark")
	}
}

func TestAnUnstampedTargetIsLeftAloneByTheCleanupLadder(t *testing.T) {
	sql := cleanableSQL().scalarAnswer(stampQueryFragment, "0")
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	if sql.ran("COMMENT ON DATABASE") >= 0 {
		t.Fatal("the ladder rewrote the comment on a database it had never stamped")
	}
}
