# `pgelastic-verify` — the durability oracle

An implementation of Patroni-Jepsen's `patroni-set` checker. `N` writer goroutines INSERT
monotonically increasing integers into `set(value bigint primary key)` through an endpoint —
normally the proxy, or PostgreSQL directly when calibrating the oracle itself — while a durable,
append-only, fsync'd local ledger records what was tried and what came back. After the chaos window
heals and the cluster is stable, the surviving primary is read back and the ledger is checked
against it.

This tool exists to fail a release. It is deliberately built before the HA controller is
feature-complete, because nothing else in the suite can tell you that a failover ate a committed
transaction.

## The three ledger states

Every value passes through the ledger before it reaches PostgreSQL.

| State | Written when | Meaning |
|---|---|---|
| `ATTEMPTED <v>` | **before** the INSERT is issued, and fsync'd before it is issued | the workload is about to try `v`. Any `v` in the database that was never attempted is a write nobody asked for |
| `COMMITTED <v>` | the server acknowledged the commit — a nil error and nothing else | `v` is durable. Losing it later is a durability violation |
| `INDETERMINATE <v>` | the outcome is not known | `v` may or may not be durable, and both are acceptable |

A fourth possibility — a *definite* rejection, such as `23505 unique_violation` or `42601
syntax_error` — writes no outcome record at all. The value stays `ATTEMPTED`, which is exactly the
right claim: we tried it, and we assert nothing about whether it landed.

Records are `<STATE> <value> <crc32>`; a torn trailing record left by the verifier being killed
mid-append fails its checksum and is dropped on replay. Damage anywhere earlier in the file is
refused outright rather than silently skipped, because a skipped line could be a `COMMITTED`.

## Why `INDETERMINATE` is not a failure

Because it is not a lie. A client that times out, gets `57P01 admin shutdown`, or has its connection
reset genuinely does not know whether the commit record reached disk. PostgreSQL may have flushed
the commit and been killed before the acknowledgement left the socket; the row is then durable and
correct, and calling it a failure would be reporting a fiction.

So the classifier is biased all the way over:

- only a nil error yields `COMMITTED`;
- a short allowlist of deterministic rejections yields a definite failure;
- **everything else is `INDETERMINATE`**, including errors nobody anticipated.

Getting this wrong invalidates the whole result. An outcome wrongly called committed takes the teeth
out of the durability assertion; an outcome wrongly called failed hides a real lost commit.
Indeterminate costs nothing but a line in an informational set.

## The check

With `R` the set read back from the surviving primary:

- **`COMMITTED ⊆ R`** — no lost committed transaction. This is the durability guarantee, and the
  only assertion that fails a release.
- **`R ⊆ ATTEMPTED`** — no unexpected write. Catches split-brain writes and phantom acknowledgements.
- `RECOVERED = (R ∩ ATTEMPTED) − COMMITTED` — indeterminate writes that turned out to be durable.
  **Informational.** After any real chaos this set is expected to be non-empty, and a non-empty
  RECOVERED is a sign the oracle is working, not that the cluster is broken.

`R` is read from a node that is out of recovery. Reading a replica would let replication lag
masquerade as a lost commit; `--allow-replica` exists but is unsafe and says so.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | both assertions hold |
| 1 | `COMMITTED ⊄ R` — a committed transaction was lost. **This fails the release.** |
| 2 | `R ⊄ ATTEMPTED` — an unexpected value is present, and no commit was lost |
| 3 | the check could not be completed (bad flags, unreachable endpoint, corrupt ledger) |

## Usage

```console
$ pgelastic-verify run --dsn "$DSN" --ledger run.log --writers 8 --duration 60s --check
$ pgelastic-verify check --dsn "$DSN" --ledger run.log --json
```

`make verify VERIFY_DSN=postgres://…` wraps the first form; `make verify-check` wraps the second.

Output is a machine-readable JSON report — verdict, counts, and the offending values by name — plus
a human-readable summary unless `--json` is given.

## Resumability

The ledger is the artifact, not the process. Killing the verifier mid-run loses at most the torn
trailing record; reopening the same ledger replays it, truncates the torn tail, and resumes the
value sequence above the highest value the previous run ever mentioned, so no value is ever reused.
A later, separate invocation of `check` against a ledger written by an earlier run is the normal way
to check a run that was killed on purpose — which is what the chaos harness does.
