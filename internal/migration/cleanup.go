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
	"errors"
	"fmt"
	"strings"
)

// LadderStep names one rung of the cleanup ladder, so a failure can say which rung it
// stopped on rather than "cleanup failed".
type LadderStep string

const (
	// StepDisableSubscription stops the apply worker. Everything after it would block on a
	// worker still holding the subscription otherwise.
	StepDisableSubscription LadderStep = "DisableSubscription"
	// StepDetachSlot is SET (slot_name = NONE). Without it DROP SUBSCRIPTION tries to drop
	// the slot on the source over the subscription's own connection, which hangs whenever
	// the source is the thing that went wrong - and hanging is the one behaviour a cleanup
	// path may not have.
	StepDetachSlot LadderStep = "DetachSlot"
	// StepDropSubscription removes the subscription now that it owns nothing remote.
	StepDropSubscription LadderStep = "DropSubscription"
	// StepDropSlot removes the slot the subscription was just detached from. Until this
	// runs the slot pins the source primary's WAL.
	StepDropSlot LadderStep = "DropSlot"
	// StepDropPublication removes the publication, which holds nothing but is litter.
	StepDropPublication LadderStep = "DropPublication"
	// StepRevokeGrants closes the reads the migration opened on the source.
	StepRevokeGrants LadderStep = "RevokeGrants"
	// StepClearSchemaStamp takes the schema-copy mark off the target database, so the database
	// a successful migration hands to the tenant carries nothing of the machinery that built
	// it.
	StepClearSchemaStamp LadderStep = "ClearSchemaStamp"
)

// LadderOrder is the order the steps must run in. It is exported because the order is the
// contract: running the slot drop before the subscription is detached leaves the
// subscription referring to a slot that no longer exists, and running the subscription drop
// before the detach can block indefinitely on an unreachable source.
var LadderOrder = []LadderStep{
	StepDisableSubscription,
	StepDetachSlot,
	StepDropSubscription,
	StepDropSlot,
	StepDropPublication,
	StepRevokeGrants,
	StepClearSchemaStamp,
}

// DiscardingTarget says the caller is about to drop the target database outright, which makes
// StepClearSchemaStamp not merely pointless but harmful. See Cleanup.
type DiscardingTarget bool

// Cleanup runs the whole ladder and never stops early.
//
// A failure on one rung is recorded and the next rung still runs, because the rung that
// matters most - dropping the slot - is near the end. A ladder that aborted on the first
// error would routinely leave a slot pinning the source primary's WAL, which is the
// failure this whole path exists to prevent.
func Cleanup(ctx context.Context, sql SQL, plan Plan, role string, discarding DiscardingTarget) error {
	// After a successful cutover the source database has been fenced, and the two rungs that
	// have to run inside it cannot open a session any more. That is not a failure: the fenced
	// database is dropped wholesale when the rollback window closes, and dropping it takes
	// the publication and the grants with it. Reporting it as an error would bury the one
	// case that does matter - a slot still pinning the primary's WAL - in noise.
	sourceOpen := sourceAdmitsConnections(ctx, sql, plan.Source)

	var problems []error
	for _, step := range LadderOrder {
		if !sourceOpen && stepNeedsSourceDatabase(step) {
			continue
		}
		// The stamp is the only evidence that a target's schema was copied in full. On a path
		// that discards the database it buys nothing, because the database is going; and if the
		// drop then fails, clearing it first has converted recoverable litter into a database
		// that is fully schema'd, unstamped, and permanently un-retryable - the next migration
		// of this tenant re-runs the copy onto its own objects and dies on "already exists"
		// until its retry budget is spent. That is not hypothetical: one flaky exec is enough,
		// because ifSubscriptionExists fails closed, the ladder never stops early, and
		// DROP DATABASE is refused while a subscription is still defined in it.
		if step == StepClearSchemaStamp && discarding {
			continue
		}
		if err := runStep(ctx, sql, plan, role, step); err != nil {
			problems = append(problems, fmt.Errorf("cleanup step %s: %w", step, err))
		}
	}
	return errors.Join(problems...)
}

// stepNeedsSourceDatabase reports the rungs that open a session on the tenant's own database
// on the source. Everything else reaches the source through the postgres database or does
// not touch it at all.
func stepNeedsSourceDatabase(step LadderStep) bool {
	return step == StepDropPublication || step == StepRevokeGrants
}

func sourceAdmitsConnections(ctx context.Context, sql SQL, source Endpoint) bool {
	allowed, err := scalar(ctx, sql, source.WithDatabase("postgres"), fmt.Sprintf(
		`SELECT coalesce(bool_or(datallowconn)::text, 'false') FROM pg_database WHERE datname = %s`,
		QuoteLiteral(source.Database)))
	if err != nil {
		// An unreadable source is assumed open, so the ladder still tries every rung and
		// reports what actually failed rather than silently skipping work.
		return true
	}
	return allowed == "true"
}

func runStep(ctx context.Context, sql SQL, plan Plan, role string, step LadderStep) error {
	switch step {
	case StepDisableSubscription:
		return ifSubscriptionExists(ctx, sql, plan,
			fmt.Sprintf(`ALTER SUBSCRIPTION %s DISABLE`, QuoteIdentifier(plan.Subscription)))
	case StepDetachSlot:
		return ifSubscriptionExists(ctx, sql, plan,
			fmt.Sprintf(`ALTER SUBSCRIPTION %s SET (slot_name = NONE)`, QuoteIdentifier(plan.Subscription)))
	case StepDropSubscription:
		return ifSubscriptionExists(ctx, sql, plan,
			fmt.Sprintf(`DROP SUBSCRIPTION %s`, QuoteIdentifier(plan.Subscription)))
	case StepDropSlot:
		return DropSlot(ctx, sql, plan.Source.WithDatabase("postgres"), plan.Slot)
	case StepDropPublication:
		return sql.Exec(ctx, plan.Source,
			fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`, QuoteIdentifier(plan.Publication)))
	case StepRevokeGrants:
		return revokeSourceReads(ctx, sql, plan.Source, role)
	case StepClearSchemaStamp:
		return clearSchemaStamp(ctx, sql, plan)
	default:
		return fmt.Errorf("unknown cleanup step %q", step)
	}
}

// DropSlot removes a logical slot by name, tolerating both its absence and an active
// consumer still attached to it.
func DropSlot(ctx context.Context, sql SQL, at Endpoint, slot string) error {
	return sql.Exec(ctx, at, fmt.Sprintf(
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s AND active) THEN
		     PERFORM pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = %s;
		   END IF;
		   IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s) THEN
		     PERFORM pg_drop_replication_slot(%s);
		   END IF;
		 END $$`,
		QuoteLiteral(slot), QuoteLiteral(slot), QuoteLiteral(slot), QuoteLiteral(slot)))
}

func ifSubscriptionExists(ctx context.Context, sql SQL, plan Plan, statement string) error {
	count, err := scalarInt64(ctx, sql, plan.Target, fmt.Sprintf(
		`SELECT count(*)::text FROM pg_subscription WHERE subname = %s`, QuoteLiteral(plan.Subscription)))
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	return sql.Exec(ctx, plan.Target, statement)
}

// clearSchemaStamp removes the mark the schema copy left. An unstamped target - one this
// migration never got as far as copying onto, or one that has already been discarded - is
// left alone, so an abort from an early phase does not report a failure to unmark a database
// that was never marked.
func clearSchemaStamp(ctx context.Context, sql SQL, plan Plan) error {
	stamped, err := SchemaCopied(ctx, sql, plan)
	if err != nil || !stamped {
		return err
	}
	return sql.Exec(ctx, plan.Target,
		fmt.Sprintf(`COMMENT ON DATABASE %s IS NULL`, QuoteIdentifier(plan.Target.Database)))
}

func revokeSourceReads(ctx context.Context, sql SQL, source Endpoint, role string) error {
	if role == "" {
		return nil
	}
	schemas, err := userSchemas(ctx, sql, source)
	if err != nil {
		return err
	}
	var problems []error
	for _, schema := range schemas {
		for _, statement := range []string{
			fmt.Sprintf(`REVOKE SELECT ON ALL TABLES IN SCHEMA %s FROM %s`,
				QuoteIdentifier(schema), QuoteIdentifier(role)),
			fmt.Sprintf(`REVOKE SELECT ON ALL SEQUENCES IN SCHEMA %s FROM %s`,
				QuoteIdentifier(schema), QuoteIdentifier(role)),
			fmt.Sprintf(`REVOKE USAGE ON SCHEMA %s FROM %s`,
				QuoteIdentifier(schema), QuoteIdentifier(role)),
		} {
			if err := sql.Exec(ctx, source, statement); err != nil {
				problems = append(problems, err)
			}
		}
	}
	return errors.Join(problems...)
}

// Orphan is one physical object left behind by a migration that no longer exists.
type Orphan struct {
	Kind string
	Name string
	At   Endpoint
}

// Orphan kinds.
const (
	OrphanSlot         = "ReplicationSlot"
	OrphanPublication  = "Publication"
	OrphanSubscription = "Subscription"
)

// FindOrphans lists the migration-owned objects on one instance that no live migration
// claims.
//
// live is the set of object names belonging to migrations that still exist and have not
// reached a terminal phase. Everything else carrying one of this package's prefixes is
// litter: a migration object deleted mid-flight, or a controller that died between creating
// a slot and recording its name. The slot is the one that matters - max_slot_wal_keep_size
// bounds how much WAL an abandoned slot can pin before PostgreSQL invalidates it, but an
// invalidated slot is still an object nobody will ever drop.
//
// The three catalogs live in three different places, which is why this cannot be one query:
// pg_replication_slots is cluster-wide, pg_subscription is a shared catalog that records
// which database each subscription belongs to, and pg_publication is per-database and has
// to be looked for in every one of them.
func FindOrphans(ctx context.Context, sql SQL, at Endpoint, live map[string]bool) ([]Orphan, error) {
	postgres := at.WithDatabase("postgres")
	var orphans []Orphan
	var problems []error

	slots, err := firstColumn(ctx, sql, postgres, fmt.Sprintf(
		`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s ORDER BY 1`,
		QuoteLiteral(SlotPrefix+"%")))
	if err != nil {
		problems = append(problems, err)
	}
	for _, name := range slots {
		if !live[name] {
			orphans = append(orphans, Orphan{Kind: OrphanSlot, Name: name, At: postgres})
		}
	}

	subscriptions, err := sql.Query(ctx, postgres, fmt.Sprintf(
		`SELECT s.subname, d.datname FROM pg_subscription s JOIN pg_database d ON d.oid = s.subdbid
		 WHERE s.subname LIKE %s ORDER BY 1`, QuoteLiteral(SubscriptionPrefix+"%")))
	if err != nil {
		problems = append(problems, err)
	}
	for _, row := range subscriptions {
		if len(row) == 2 && !live[row[0]] {
			orphans = append(orphans,
				Orphan{Kind: OrphanSubscription, Name: row[0], At: at.WithDatabase(row[1])})
		}
	}

	databases, err := firstColumn(ctx, sql, postgres,
		`SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1`)
	if err != nil {
		problems = append(problems, err)
	}
	for _, database := range databases {
		in := at.WithDatabase(database)
		publications, err := firstColumn(ctx, sql, in, fmt.Sprintf(
			`SELECT pubname FROM pg_publication WHERE pubname LIKE %s ORDER BY 1`,
			QuoteLiteral(PublicationPrefix+"%")))
		if err != nil {
			problems = append(problems, err)
			continue
		}
		for _, name := range publications {
			if !live[name] {
				orphans = append(orphans, Orphan{Kind: OrphanPublication, Name: name, At: in})
			}
		}
	}

	return orphans, errors.Join(problems...)
}

// SweepOrphans drops every object FindOrphans returned, in the same order the cleanup
// ladder uses within one migration.
func SweepOrphans(ctx context.Context, sql SQL, orphans []Orphan) error {
	ordered := []string{OrphanSubscription, OrphanSlot, OrphanPublication}
	var problems []error
	for _, kind := range ordered {
		for _, orphan := range orphans {
			if orphan.Kind != kind {
				continue
			}
			if err := sweepOne(ctx, sql, orphan); err != nil {
				problems = append(problems, fmt.Errorf("sweeping %s %s: %w", orphan.Kind, orphan.Name, err))
			}
		}
	}
	return errors.Join(problems...)
}

func sweepOne(ctx context.Context, sql SQL, orphan Orphan) error {
	name := QuoteIdentifier(orphan.Name)
	switch orphan.Kind {
	case OrphanSubscription:
		for _, statement := range []string{
			`ALTER SUBSCRIPTION ` + name + ` DISABLE`,
			`ALTER SUBSCRIPTION ` + name + ` SET (slot_name = NONE)`,
			`DROP SUBSCRIPTION ` + name,
		} {
			if err := sql.Exec(ctx, orphan.At, statement); err != nil {
				return err
			}
		}
		return nil
	case OrphanSlot:
		return DropSlot(ctx, sql, orphan.At, orphan.Name)
	case OrphanPublication:
		return sql.Exec(ctx, orphan.At, `DROP PUBLICATION IF EXISTS `+name)
	default:
		return fmt.Errorf("unknown orphan kind %q", orphan.Kind)
	}
}

// LiveObjectNames is the claim set FindOrphans compares against, built from the migrations
// that still exist.
func LiveObjectNames(namespaced [][2]string) map[string]bool {
	live := make(map[string]bool, len(namespaced)*3)
	for _, key := range namespaced {
		namespace, name := key[0], key[1]
		live[SlotName(namespace, name)] = true
		live[PublicationName(namespace, name)] = true
		live[SubscriptionName(namespace, name)] = true
	}
	return live
}

// IsAlreadyGone reports an error that means the object this statement addressed does not
// exist. Cleanup treats it as success: the ladder's job is that the object is gone, not
// that this particular call is what removed it.
func IsAlreadyGone(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "does not exist")
}
