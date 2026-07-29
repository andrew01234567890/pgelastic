# Moving a live tenant between instances, with its clients queued

This is the run behind the product's headline claim: a tenant is moved from one three-node
PostgreSQL 18 instance to another **while a client is writing to it through the pool's
Service**, and that client is queued rather than dropped.

Everything below is measured. The pause is what the client waited, taken from the client's
own clock; the durability verdict is the `internal/verify` oracle replaying its ledger
against the surviving primary. Nothing here is the operator's account of itself, except
where it is explicitly labelled as such and compared with the client's.

Reproduce with `hack/demo/run.sh` (see [Running it](#running-it)).

## What was run

| | |
|---|---|
| Pool | `demo-pool`: two `PgInstance`s of three members each, one proxy fleet of two replicas |
| Tenants | `test` on `demo-a`, `test2` on `demo-b` |
| Data | 1,000,000 rows in `test.bulk`, plus the oracle's own `set` relation |
| Client | 4 connections through the pool's `ClusterIP` Service, one `INSERT` each per 20 ms |
| Move | `test` from `demo-a` to `demo-b`, online (logical replication), under load |
| Cluster | kind, single node, cert-manager 1.19.1, operator deployed from this tree |

## The numbers

**Client-visible pause: 1.53 s.** That is the longest a queued write waited at the gate,
measured by the client. Each of the four writers shows the same figure within 20 ms:

```
14:35:23.380  1525.6 ms
14:35:23.382  1523.9 ms
14:35:23.384  1521.8 ms
14:35:23.399  1507.0 ms
```

Latency either side of the cutover, from the same client:

| | p50 | p99 | samples |
|---|---|---|---|
| before the migration was created | 3.21 ms | 5.79 ms | 4,211 |
| from then until the writer stopped | 3.30 ms | 12.18 ms | 3,352 |

**Nothing was dropped.** 7,563 attempts, **0 connect errors**, and no connection was ever
closed: the same four sockets that were queued are the ones that carried on afterwards.

**The oracle's verdict: PASS.**

```
attempted=7563 committed=7362 indeterminate=0 observed=7362
COMMITTED subset of R: holds
R subset of ATTEMPTED: holds
lost=0 unexpected=0
```

No committed write was lost across the move, and nothing appeared that the client never
tried to write.

**What the operator reported about itself**, for comparison:

```
phase=Completed
pauseDurationMillis=1872
clientPauseMillis=1509
queuedClients=3
```

`clientPauseMillis` (1,509 ms) and the client's own worst write (1,525.6 ms) agree to
within 17 ms, which is the round trip the client's measurement includes and the gate's does
not. `pauseDurationMillis` (1,872 ms) is a different and larger number on purpose: it is the
controller's wall clock across the quiesced phases, so it includes the drain polling and the
reconcile latency on either side of the hold. `queuedClients=3` is the gate's own count of
transactions waiting at the instant the drain was last observed.

The sub-second target was missed. **1.5 s, and what dominates it is the operator, not
PostgreSQL.** The pause is spent on: the reconcile that observes the drain (the quiesced
phases poll at 250 ms), the row-count and schema verification the cutover runs before it
will flip, the wait for the operator's own re-render of the fleet's configuration Secret to
land (which is what makes the flip durable before it is made live — see `ProxyRouter.Route`),
and the reconcile that then decides `Completed` and resumes. Every one of those is control
plane. The proxy's own quiesce round trip, measured standalone against two containers in
`crates/pgelastic-proxy/tests/quiesce.rs`, stays under 100 ms under load.

## The one number that is not clean

201 of the 7,563 writes **failed** — not lost, not indeterminate, definitely refused — in a
2.9 s window starting the instant the queued clients were released. They are all `42501
permission denied`, and they are not caused by the move being unsafe. They are caused by
this, in `internal/migration/online.go`:

```
pg_dump --schema-only --no-owner --no-privileges
```

The online path copies the tenant's schema onto the target with ownership and every ACL
stripped, so after the flip every relation is owned by the restoring superuser and grants
nothing to anybody. The tenant's own roles lose every privilege they had. `hack/demo/run.sh`
re-grants immediately after the cutover and that is what closes the window; without the
re-grant the same run showed 2,088 failures and never recovered.

This is a defect in tenant migration and not in the cutover: the clients were held, released
and never dropped, and the oracle still passes because a refused write is recorded as a
failure and is never claimed as committed. It is called out here rather than papered over,
because a demo that hid it would be advertising a move that silently breaks the database it
moved.

`maxCommitGapMillis` in the raw report is 2,910 ms rather than 1,525 ms for the same reason:
the gap between two *successful* writes spans the pause and the permission window together.

## Two other things this run found

**An online move of a continuously written tenant never leaves Catchup by default.**
`Catchup` advances when the subscriber's lag is at or below `spec.preflight.maxSourceLagBytes`,
which defaults to zero — and a tenant that is still serving writes never reaches zero. The
first attempt at this demo sat in `Catchup` for ten minutes with a lag oscillating around
3 KB. `hack/demo/manifests/30-migration.yaml` sets the threshold to 1 MiB; whatever is still
in flight at that point is closed inside the pause, because the cutover will not flip until
the subscriber has confirmed the source's LSN as of the instant the clients were queued.

**A tenant database with no sequences could not be migrated at all.** The schema fingerprint
the verifier compares coalesces to the empty string on purpose, and `psql -tA` prints that as
a single newline — which the exec transport parsed as *no rows*, failing verification
mid-cutover with "query returned no rows". Fixed in `internal/migration/podexec.go`; the demo
found it because its tenant has no sequences and the migration e2e's does not.

## Running it

The operator must be deployed and cert-manager installed: the control listener a cutover
drives is issued by cert-manager per pool, and without it the migration falls back to the
routing table alone and queues nobody.

```sh
CONTEXT=kind-pgelastic-test-e2e NS=pgelastic-demo ./hack/demo/run.sh
```

It writes the ledger, a CSV of every attempt with its latency, the migration's final YAML
and the writer's JSON report into a directory it names on stdout.

## Where the claim is asserted rather than demonstrated

`test/e2e/migration` holds the same claim as a test: a client holding a connection open
through the pool's Service across a cutover sees a latency spike and no error, and a second
tenant on the other instance sees neither. It is mutation-tested — with nothing holding the
clients, the same spec fails with `55000 database "acme" is not currently accepting
connections` and 77 dropped writes, and the held connection is only ever served by one
instance instead of two.
