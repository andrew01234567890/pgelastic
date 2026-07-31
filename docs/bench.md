# Benchmarking the proxy: Rust today, pgbouncer as the yardstick

The data plane is Rust and the operator is Go. Collapsing to one language is worth real money
— one toolchain, one CI, one dependency-audit story, a config schema with a shared type
instead of a hand-written renderer on one side and `serde` on the other. What nobody knew was
the performance cost, because until now the repository had no benchmark at all.

This is the apparatus that answers it, and the numbers it has produced so far.

It is arranged so that a flattering number is hard to produce. The thresholds are committed
constants in `internal/bench/criteria.go`, fixed before the Go proxy exists. The machine is
recorded in every result file. A cell whose repetitions do not agree is reported
`INCONCLUSIVE` rather than resolved.

## What was run

| | |
|---|---|
| PostgreSQL | `postgres:18`, `shared_buffers=2GB`, `max_connections=1000`, `fsync=off`, pinned to 6 physical cores |
| Pooler | one container at a time, pinned to 4 physical cores, `--memory 2g` |
| Load generator | on the host, pinned to 4 physical cores, `taskset` |
| Network | one user-defined Docker bridge; the pooler-to-PostgreSQL leg never leaves it |
| Workloads | `throughput` (closed loop), `churn` (connect + SCRAM + query + close) |
| Sweep | 1, 8, 64, 256 clients |
| Timing | 5 s warmup discarded, 20 s measured, **5 repetitions**, median reported with spread |
| Machine | AMD Ryzen 9 5950X, 16 physical / 32 logical, 15.6 GiB, WSL2, Docker 29.5.3 |

Every arm crosses the WSL2 published-port relay exactly once, on the client leg. That is
deliberate: the relay cost is then a constant shared by every arm rather than something that
differs between them.

## The arms

| Arm | What it is | Threads | Backend round trips per client query |
|---|---|---|---|
| `direct` | no pooler at all — the mandatory zero point | n/a | 1 |
| `rust` | `txn-fence-off.toml` — **what the operator ships** | 2 | 2 (query + `DISCARD ALL`) |
| `rust-reset-dirty` | `resetPolicy = "dirtyTracked"` — the true like-for-like row | 2 | 1 |
| `rust-reset-none` | `resetPolicy = "none"` — turns out not to mean what it looks like | 2 | 1, but stops reusing links |
| `rust-1worker` | `txn-fence-off.toml`, one runtime worker | 1 | 2 |
| `rust-fence-on` | `fence.verifyAtCheckout = true` — production posture | 2 | 3 (+ epoch probe) |
| `pgbouncer` | the external reference | 1 | 1 |

That last column is the one to read first. Three of these arms differ in how many times they
talk to PostgreSQL per client query, and a comparison that ignored it would be reporting
round-trip counts as though they were implementation quality.

`direct` is not optional. Almost all of a query's latency is PostgreSQL, so without it the
pooler numbers would be a ratio between two unknowns.

## Why pgbouncer is here, and why it is not a gate

Rust versus Go on its own is a **closed comparison**: it says which of the two is faster, not
whether either is any good. If both are half the speed of the pooler most people already run,
the interesting question was never which language to write it in.

pgbouncer is configured term-for-term against `txn-fence-off.toml` — transaction pooling,
pool size 100, 5000 client connections, SCRAM on the client leg, TLS off on both legs.

What cannot be matched is the work itself. pgbouncer has **no epoch fence, no tenant routing,
no capacity allocator, no per-tenant admission, no quiesce or cutover, no per-tenant backend
credentials and no control API.** It is doing strictly less. So the harness emits a
`REFERENCE` row carrying a ratio and no verdict — a pass or fail against it would be scoring
two different programs against one threshold.

It is also single-threaded, which is why `rust-1worker` exists. Comparing a 2-worker proxy to
a 1-thread pooler and calling the difference a language result would be a mistake.

## What the harness refuses to say

Three refusals, all of them first-class outcomes rather than failures to produce one:

- **A sample that does not repeat decides nothing.** If run-to-run spread exceeds 10%, the
  row is `INCONCLUSIVE`. This is measured per sample, not assumed from what kind of machine
  it is — see the run-length finding below.
- **Latency is compared as *added* microseconds over `direct`, never as a ratio of absolutes.**
  Almost all of the absolute number is PostgreSQL, and dividing one by the other would compare
  the database to itself and report a reassuring 1.01x however bad the pooler was.
- **A negative added latency produces no ratio.** Once client count exceeds pool size the
  pooler beats direct connections — that is the pooler working, and a ratio with a negative
  denominator is meaningless.

And one thing it does not yet refuse, which the reader has to hold in mind. The 10% gate is
computed over five repetitions **inside a single invocation**. Measured across invocations an
hour apart, the `direct` arm moved 8–13% — 4,052 then 3,522 ops/s at one client — while each
pass reported an internal spread of 0–2%. Within-run repeatability is therefore not the same
thing as between-run repeatability, and it is the weaker claim that the gate actually tests.

This does not touch the pooler-versus-pooler comparison, whose arms ran back to back under
the same conditions. It does mean every *proxy-versus-direct* ratio here carries roughly ten
points of slack that the quoted spreads do not show.

## What the first run found

### Run length is a Gate 0 variable, not just the machine

The gate is a 10% run-to-run spread. Direct-to-PostgreSQL, five repetitions, at two run
lengths:

| Metric | Spread @ 3 s | Spread @ 20 s |
|---|---|---|
| throughput | 1.0–6.4% | 2.4–5.8% |
| p99 | 4.9–8.3% | 3.3–6.4% |
| p99.9 | **14.2–19.0%** | **1.7–8.4%** |

p99.9 looked undecidable on this machine until the runs got longer. A three-second run simply
puts too few samples in the deep tail. **The fix for a noisy tail was more samples, not a
better machine.**

This is why the gate is keyed on each sample's measured spread rather than on what kind of
machine produced it. An earlier version of `criteria.go` refused every latency verdict on a
virtualised rig; it would have declared this box unable to decide latency, and the twenty-second
run that disproves that would never have been taken.

### The headline: we match pgbouncer's throughput and spend twice the CPU doing it

Transaction pooling, pool of 100 backends, 20 s runs, five repetitions, median:

| clients | direct | pgbouncer | pgelastic (Rust) | rust / pgbouncer |
|---|---|---|---|---|
| 1 | 3,522 | 2,306 | 2,138 | 0.93x |
| 8 | 18,546 | 11,244 | 12,202 | 1.09x |
| 64 | 54,897 | 13,685 | 13,436 | 0.98x |
| 256 | 121,552 | 12,042 | 11,925 | 0.99x |

p99 tracks just as closely — 5,767 µs against 5,687 µs at 64 clients, 23,279 µs against
22,783 µs at 256. On absolute throughput and latency the two are indistinguishable.

The cost side is not:

| | CPU at saturation | RSS | throughput per core |
|---|---|---|---|
| pgbouncer | ~103% (one core, pegged) | 8.1 MiB | ~13,700 ops/s |
| pgelastic | ~218% (both workers pegged) | 33.6 MiB | ~6,700 ops/s |
| pgelastic, 1 worker | ~100% | — | ~7,900 ops/s |

**pgbouncer is roughly 1.7–2x more CPU-efficient per core.** We reach the same absolute
number by spending two cores where it spends one, and about four times the memory — though
both are small enough that memory is unlikely to decide anything.

A correction worth recording: an earlier draft of this document reported the proxy as "not
obviously CPU-bound" on the strength of a 36.9% CPU sample. That sample was taken during the
single-client phase of a sweep, not at the ceiling. At the ceiling both poolers are pegged.
The reading was wrong, and the fix was to sample across the whole sweep rather than once.

### What the ceiling is not

Three candidate explanations were tested and eliminated, which is why the CPU explanation can
be stated with any confidence:

| Hypothesis | Test | Result |
|---|---|---|
| Docker's userspace port relay | run the load generator inside the bridge, no relay | 14,315 vs 13,436 ops/s — **not the relay** |
| The container bridge itself | direct to PostgreSQL from inside the bridge | 135,544 ops/s — **not the bridge** |
| The extra backend round trip | `dirtyTracked`, which drops the `DISCARD ALL` | 1.00–1.02x — **not round-trip count** |

That last one is worth dwelling on. The shipped configuration makes *two* backend round trips
per client transaction where pgbouncer makes one, and removing the second changes throughput
by nothing measurable. Whatever the per-operation cost is, it is not the network.

### `resetPolicy = "none"` costs 70x throughput, because it stops reusing connections

Set out to measure the proxy with the reset query switched off, to match pgbouncer. The
result was not a faster arm:

| clients | `discardAll` (shipped) | `none` |
|---|---|---|
| 1 | 2,138 ops/s | 149 ops/s |
| 8 | 12,202 ops/s | 174 ops/s |
| 64 | 13,436 ops/s | 174 ops/s |
| 256 | 11,925 ops/s | 171 ops/s |

`ResetPolicy::None` does not skip the scrub. Given any taint it returns
`Close(CloseReason::ResetDisabled)` — it closes the link instead of reusing it. So every
transaction opened a fresh backend, and ~170/s is simply the rate at which backends can be
opened. The setting that reads like "no reset overhead" turns the pooler off.

Anyone tuning this in production would reasonably expect the opposite. The policy that
actually removes the overhead is **`dirtyTracked`**, which reuses a clean link with no reset
at all and only scrubs one that has been dirtied — one backend round trip for a `SELECT`
workload, which is what pgbouncer does in transaction mode. That is the arm that belongs in
the comparison, and `txn-reset-dirty.toml` is it.

### The churn axis is measuring the test rig, not the proxy

| clients | direct conn/s | proxy conn/s | ratio | direct spread | proxy spread |
|---|---|---|---|---|---|
| 1 | 149 | 206 | 1.38x | 0.5% | 54.2% |
| 8 | 329 | 379 | 1.15x | 20.3% | 44.0% |
| 64 | 329 | 408 | 1.24x | 23.0% | 36.2% |
| 256 | 329 | 409 | 1.24x | 24.2% | 35.7% |

Both arms go flat from eight clients upward, at values close to each other, with spreads far
past the 10% gate. Every one of these rows is reported `INCONCLUSIVE`.

**It is not PostgreSQL's postmaster.** That was the appealing explanation — the postmaster
forks a backend per connection and is single-threaded, and removing that cost is exactly why
poolers exist. But the proxy reuses about a hundred pooled backends and forks no PostgreSQL
backend per client connect, so if the postmaster were the ceiling the proxy would be far
ahead of direct rather than 1.2x ahead of it.

A ceiling shared by both arms at a similar value points at what both arms share: the client
leg, which on WSL2 crosses Docker Desktop's `docker-proxy` — a single userspace process
relaying every published-port connection.

**Consequence: these numbers are a property of the test rig and must not be published as a
property of either pooler.** The axis has to be re-run with the load generator inside a
container on the bridge, where there is no relay to serialise against. Nothing incorrect has
been reported in the meantime, because the spread already put every row past the gate — which
is the gate doing its job rather than a lucky escape.

### The epoch fence costs a quarter of throughput

`fence.verifyAtCheckout` defaults to on, and it runs a `SELECT` against the backend at every
checkout — which in transaction pooling means once per client transaction.

| clients | fence off | fence on | cost |
|---|---|---|---|
| 1 | 2,138 | 1,549 | 0.72x |
| 8 | 12,202 | 9,264 | 0.76x |
| 64 | 13,436 | 10,026 | 0.75x |
| 256 | 11,925 | 9,169 | 0.77x |

Consistently around 25%, and consistent across concurrency. That is the price of the
split-brain guarantee, and it is now a number rather than an intuition. It is also the reason
every comparison here is reported with the fence off on both sides: leaving it on would tax
one arm for a feature the other does not have.

## What this means for the Rust-versus-Go question

The comparison was set up to answer whether the data plane should be rewritten in Go. Two of
its findings bear on that before a line of Go exists.

**The rewrite question is legitimate.** One of the outcomes this exercise was built to detect
was "our proxy is far behind the industry standard, so fix it before porting it". That is not
what happened: on throughput and latency we are within 7% of pgbouncer in both directions.

**But this rig cannot decide the throughput axis for Go.** Both poolers saturate their CPU at
~13k ops/s, and a Go proxy would be measured against the same wall. The axis can detect a Go
implementation that is *substantially* worse; it cannot resolve one that is slightly worse,
because there is no headroom above the ceiling to resolve it in. The axis that will actually
discriminate is **CPU per operation**, where pgbouncer already beats us 1.7–2x and where a
garbage-collected runtime has the most to lose.

That is a change to the plan's measurement strategy, and it came from the data rather than
from an argument.

## Reproduce with

```bash
make bench-doctor
```

```bash
make bench-arms
```

```bash
make bench-report
```

`bench-doctor` reports what the machine can and cannot decide before anything is measured.
`bench-arms` runs each arm in turn — one at a time, because two arms sharing the cores would
measure the contention rather than either arm — waiting for `/readyz` on the proxy and for a
real query to succeed through pgbouncer, which has no health endpoint.

## Where this is not yet honest

Recorded here rather than discovered later:

- **Only two of the five workloads have been run.** `latency` (open loop), `bulk` and
  `density` are implemented and tested but not yet measured across the arms.
- **No TLS arm.** The plan calls for client-leg and both-leg TLS. Adding it means pinning both
  implementations to the same TLS 1.3 cipher suite, since `crypto/tls` and `rustls` do not
  choose the same one by default.
- **Density is not yet measured**, because the number that matters — resident bytes per idle
  connection — has to be read from outside the process while the connections are held, and the
  driver only counts that the connections were established.
- **The sweep stops at 256 clients**, well short of the 4096 the plan calls for.

## The Go arm

A Go proxy did not exist when the arms above were measured, so those tables have no Go
column. `test/bench/cmd/goproxy/` **was** a ~330-line spike written to fill it, deleted after
round two — see the closing section for why. It was: accept, trust auth
on the client leg, a fixed pool of backends hijacked from `pgconn`, the same 64 KiB inline /
16 KiB streaming relay split as the Rust proxy, and a backend released at `ReadyForQuery`
carrying `'I'`.

**It does strictly less than either implementation it is compared with.** No TLS, no SCRAM on
the client leg, no tenant routing, no capacity allocator, no admission queue, no epoch fence,
no reset policy, no cancellation, no control API, no metrics.

That is deliberate, and it is what makes the arm worth running. Every omission removes work Go
would otherwise have to do, which makes the spike a **best case for Go**: whatever it costs per
operation is a floor, and a full port can only be worse. A spike that loses is decisive; a
spike that wins proves much less than it looks.

### Why all three were re-measured under the simple protocol

Under the extended protocol the driver caches a prepared statement per client connection, so a
pooler that hands the next transaction to a different backend has to notice and re-prepare it
there. The Rust proxy does that by rewriting statement names to content-addressed ones,
pgbouncer does it from 1.21, and a 330-line spike does neither.

Rather than credit whoever happens to have that capability, all three arms were re-run with
`--simple-protocol`, which takes prepared statements out of the comparison. Those reports carry
a `-simple` suffix so they cannot be mixed with the extended-protocol ones.

The switch costs the Rust proxy nothing measurable — 1.07x, 1.01x, 0.98x, 0.99x across the
sweep — so the simple-protocol numbers remain comparable to the extended-protocol tables above.

### The three-way result

Throughput, ops/s, identical cpusets, both proxies at two workers, pgbouncer single-threaded
by design:

| clients | direct | pgbouncer | pgelastic (Rust) | Go spike | go/rust | go/pgb | rust/pgb |
|---|---|---|---|---|---|---|---|
| 1 | 3,505 | 2,324 | 2,293 | 1,869 | 0.82x | 0.80x | 0.99x |
| 8 | 18,956 | 12,871 | 12,330 | 10,626 | 0.86x | 0.83x | 0.96x |
| 64 | 56,236 | 17,071 | 13,180 | 10,880 | 0.83x | 0.64x | 0.77x |
| 256 | 118,246 | 15,782 | 11,856 | 10,454 | 0.88x | 0.66x | 0.75x |

Against the zero point, every pooler is expensive: at 64 clients pgbouncer delivers 0.30x of
direct PostgreSQL, the Rust proxy 0.23x and the Go spike 0.19x. Pooling buys connection
density and cutover control, and it is paid for in throughput.

p99 latency, microseconds — where the three separate most:

| clients | pgbouncer | pgelastic (Rust) | Go spike | go/rust |
|---|---|---|---|---|
| 1 | 525 | 517 | 634 | 1.23x |
| 8 | 802 | 854 | 1,097 | 1.28x |
| 64 | 4,875 | 6,287 | 10,519 | **1.67x** |
| 256 | 17,839 | 24,015 | 36,575 | **1.52x** |

Cost, measured in the same run:

| | peak CPU | peak RSS | ops/s per 100% CPU @ 64 clients |
|---|---|---|---|
| pgbouncer | 112% | 5.1 MiB | **15,245** |
| pgelastic (Rust) | 227% | 17.1 MiB | 5,794 |
| Go spike | 225% | **79.0 MiB** | 4,839 |

### Against the pre-registered criteria

Applying the thresholds committed in `internal/bench/criteria.go` before any Go existed:

| Axis | Threshold | Measured | |
|---|---|---|---|
| Throughput | Go ≥ 0.75x Rust | 0.82–0.88x | **PASS** |
| CPU per operation | Go ≤ 1.33x Rust | 1.20x | **PASS** |
| p99 latency | Go ≤ 1.25x Rust | 1.23–1.67x | **FAIL** at 64 and 256 clients |
| Memory | Go ≤ 1.25x Rust | 4.6x | **FAIL** |

**Two of four fail — and they fail as a best case.** The spike has no TLS, no SCRAM, no tenant
routing, no capacity allocator, no admission queue, no epoch fence, no reset policy and no
control API. Every one of those would add allocation and work to the Go side. The real port
cannot do better than this and would very likely do worse.

The tail behaviour and the memory figure are consistent with a garbage-collected runtime under
a high allocation rate, which is what one would predict — but this measurement does not prove
the mechanism, only the outcome. Confirming it would mean `GODEBUG=gctrace=1` or a pprof
profile, neither of which has been run.

### What is not yet established

The spike is 330 lines against roughly 20,000 in the Rust data plane, so it is evidence about
the *floor* of a Go implementation rather than a verdict on one. A serious port would be
written differently — buffer pools, fewer allocations per frame, `GOGC` tuning — and could
close some of the gap. What it cannot do is add features and get faster.

## Summary table

Simple protocol, 64 clients, identical cpusets — the row every arm was measured on:

| | pgbouncer | pgelastic (Rust) | Go spike |
|---|---|---|---|
| Throughput | **17,071 ops/s** | 13,180 ops/s | 10,880 ops/s |
| p99 | **4,875 µs** | 6,287 µs | 10,519 µs |
| Peak CPU | **112%** | 227% | 225% |
| Peak RSS | **5.1 MiB** | 17.1 MiB | 79.0 MiB |
| Ops/s per 100% CPU | **15,245** | 5,794 | 4,839 |
| Features | pooling | pooling + epoch fence + tenant routing + capacity + quiesce + control API | pooling only |

Read the last row before any of the others. pgbouncer and the spike are both doing
substantially less work than the Rust proxy, which is why neither comparison is a pass or a
fail on its own — and why the spike's losses matter more than its wins would have.

## Round two: after fixing both proxies

Two changes, one per implementation, each measured on its own.

### Rust: metrics were taking the pool lock on the hot path

`publish_budget()` refreshed the budget gauges by reading the ledger, which needs the
`PoolManager` mutex — and the transaction state machine called it from **fifteen separate
points**. On a two-worker runtime that put a contended acquisition on almost every state
transition.

Now throttled to one refresh per 5 ms behind an atomic compare-exchange, with
`publish_budget_now()` kept for config reloads and route changes where a stale gauge would be
misread as a fact rather than as lag.

| clients | before | after | gain |
|---|---|---|---|
| 8 | 12,330 | 13,266 | 1.08x |
| 64 | 13,180 | 14,652 | 1.11x |
| 256 | 11,856 | 13,371 | 1.13x |

Modest, and concentrated at high concurrency — which is where contention was predicted.

### Go: the spike had no write buffer, and that was most of its cost

Every frame was written with a syscall for its header and another for its body. A `SELECT`
answers with four frames, so a trivial query cost about eight writes where it should cost one.
The Rust relay has coalesced since it was written ([session.rs:413](../crates/pgelastic-proxy/src/session.rs)),
so the spike was being measured against a buffer it did not have.

| clients | before | after | gain |
|---|---|---|---|
| 8 | 10,626 | 13,257 | 1.25x |
| 64 | 10,880 | 24,745 | **2.27x** |
| 256 | 10,454 | 22,969 | **2.20x** |

**This invalidates the first Go verdict.** The earlier report — Go failing two of four
pre-registered axes — was measuring a missing write buffer, not a language.

### The corrected three-way

| clients | pgbouncer | pgelastic (Rust) | Go spike | go/rust | go/pgb |
|---|---|---|---|---|---|
| 1 | 2,324 | 2,252 | 2,148 | 0.95x | 0.92x |
| 8 | 12,871 | 13,266 | 13,257 | 1.00x | 1.03x |
| 64 | 17,071 | 14,652 | **24,745** | 1.69x | 1.45x |
| 256 | 15,782 | 13,371 | **22,969** | 1.72x | 1.46x |

p99, microseconds — Go is now ahead here too:

| clients | pgbouncer | Rust | Go spike | go/rust |
|---|---|---|---|---|
| 64 | 4,875 | 5,263 | 4,743 | 0.90x |
| 256 | 17,839 | 20,239 | 15,887 | 0.78x |

Cost at 64 clients:

| | peak CPU | peak RSS | ops/s per 100% CPU |
|---|---|---|---|
| pgbouncer | 112% | **5.1 MiB** | **15,242** |
| pgelastic (Rust) | 226% | 16.1 MiB | 6,489 |
| Go spike | 228% | 91.2 MiB | 10,837 |

### Against the pre-registered criteria, corrected

| Axis | Threshold | Measured | |
|---|---|---|---|
| Throughput | Go ≥ 0.75x Rust | 1.69x | **PASS** |
| CPU per operation | Go ≤ 1.33x Rust | 0.60x | **PASS** |
| p99 latency | Go ≤ 1.25x Rust | 0.78–0.90x | **PASS** |
| Memory | Go ≤ 1.25x Rust | **5.7x** | **FAIL** |

Only memory fails now, and it fails by a lot.

## What this actually says

**The gap is architectural, not linguistic.** A 350-line Go program with no TLS, no SCRAM, no
tenant routing, no capacity allocator, no admission control, no epoch fence, no quiesce and no
control API outruns the Rust proxy by 1.69x — and pgbouncer, in C, is more CPU-efficient than
either. The Rust proxy is not slow because it is Rust. It is slow because it does roughly six
mutex acquisitions per transaction on structures both runtime workers share, and pgbouncer's
single-threaded event loop does none.

That points somewhere other than a rewrite. At 6,489 ops/s per core against pgbouncer's
15,242, the Rust proxy has something like 2.3x of headroom available from its own design, and
the first 11% of it came from deleting one lock acquisition. A Go port that reproduced the
same per-transaction bookkeeping would inherit the same ceiling — the spike is fast partly
because it has no bookkeeping to do.

The remaining candidates, in expected-value order, are recorded in the task list: the routing
table's mutex on every checkout (`arc-swap` is already a workspace dependency), the
`InstanceId` and `CancelTarget` string clones per checkout, the quiesce gates' lock on their
open fast path, and memoising the tripwire scan.

## Round three: closing the gap with pgbouncer

The Go spike was deleted after round two. Its result stands as the reason: a 350-line program
with none of the features outran the Rust proxy, which said the ceiling was architectural
rather than linguistic. That made the useful next move fixing the architecture, not changing
language.

### What pgbouncer does differently

Read from source at `13a344f` (2026-07-10).

**It never copies a payload.** `IOBuf` is one flat buffer per connection with three cursors —
`done_pos` (sent), `parse_pos` (parsed, pending send), `recv_pos` (received). Packets are
parsed in place, `iobuf_tag_send()` merely advances a cursor, and `sbuf_send_pending_iobuf()`
writes straight out of the receive buffer (`include/iobuf.h`, `src/sbuf.c`). One read, one
write, no intermediate buffer.

**It takes no locks.** A single-threaded libevent loop needs none. Our proxy took roughly six
mutex acquisitions per transaction on structures both runtime workers share, which is what put
CPU at 227% while throughput sat at 13k.

### The changes, measured

| clients | baseline | after | gain | vs pgbouncer before | after |
|---|---|---|---|---|---|
| 1 | 2,293 | 2,344 | 1.02x | 0.99x | 1.01x |
| 8 | 12,330 | 13,499 | 1.09x | 0.96x | **1.05x** |
| 64 | 13,180 | 14,984 | 1.14x | 0.77x | 0.88x |
| 256 | 11,856 | 13,628 | 1.15x | 0.75x | 0.86x |

Three edits, none of them clever:

1. **Metrics stopped taking the pool lock.** `publish_budget()` refreshed gauges by reading the
   ledger under the `PoolManager` mutex, and the transaction state machine called it from
   fifteen places. Now one refresh per 5 ms behind an atomic compare-exchange, with
   `publish_budget_now()` kept for reloads and route changes. **Worth about 11%.**
2. **The routing table became lock-free to read.** It was a `Mutex<HashMap>` read at every
   checkout and written only by a cutover — `ArcSwap` with read-copy-update on the write side.
   **Worth about 2%.**
3. **`BackendConn.address` became `Arc<str>`**, removing a heap allocation per checkout, since
   it is cloned into a `CancelTarget` every time a backend is taken.

We now beat pgbouncer at eight clients and reach 86–88% of it at high concurrency, from
75–77%. The remaining gap is still lock traffic: both quiesce gates and the `PoolManager`
itself are taken per transaction. Those are recorded as next steps, with the safety analysis
for the gate already done — it is the mechanism behind live migration, and it is not a change
to make casually.

### Round four: the quiesce gates

Both gates — tenant and instance — took `queue.lock()` on every transaction to evaluate
`open && waiting.is_empty() && !running`, a condition that is true for every transaction
outside a cutover. That is two contended acquisitions per transaction to discover that nothing
is happening.

`TenantGate` now carries an `AtomicBool` mirroring the condition, recomputed under the lock at
every site that changes it. `admit()` reads the atomic first and returns without locking.

Safe to read stale in either direction, and this is the part that had to be argued rather than
assumed. Stale-false costs one lock and the authoritative check behind it. Stale-true admits a
transaction that a quiesce closing at that exact instant would have queued — which is the same
race a caller already loses by passing the *locked* check one instruction before the close.
Admission registers nothing: the fast path returns `Baton { gate: None }`, and what a drain
waits for is `in_flight`, which `hold()` bumps after checkout and `drainStatus` polls. A test
pins the direction that matters — a quiesced gate is never fast.

### Where the Rust proxy ended up

| clients | baseline | v3 | v4 | total | pgbouncer | verdict |
|---|---|---|---|---|---|---|
| 1 | 2,293 | 2,344 | 2,355 | 1.03x | 2,324 | tied |
| 8 | 12,330 | 13,499 | **14,111** | 1.14x | 12,871 | **+9.6%, resolved** |
| 64 | 13,180 | 14,984 | **16,683** | 1.27x | 17,071 | tied |
| 256 | 11,856 | 13,628 | **15,234** | 1.28x | 15,782 | −3.5%, resolved |

"Tied" and "resolved" are the harness's own rule: two samples whose run-to-run ranges overlap
did not separate, and reporting a winner from them would be reporting noise.

**27–28% faster than where this started, and now level with pgbouncer** — ahead of it at eight
clients, indistinguishable at one and sixty-four, 3.5% behind at 256 where the pool is
oversubscribed and the wait queue is doing real work.

### Why parity is roughly the expected ceiling here

At 16,683 ops/s across two saturated cores each operation costs about 120 µs of CPU;
pgbouncer's is about 66 µs on one. Both numbers are far larger than the work involved, because
both poolers make the same four syscalls per query — read client, write backend, read backend,
write client — and on this virtualised network stack a syscall is expensive. Having removed
the lock traffic that separated them, the two converge on what the rig charges for syscalls.

Going faster than pgbouncer here would mean making fewer syscalls than it does, not writing
tighter code between them. That is the zero-copy relay lever, and it pays on large result sets
rather than on `SELECT 1`.

## Round five: measuring what an operation costs, instead of estimating it

Every CPU and memory figure above this line was derived by hand from `docker stats` read at
some remembered moment. Round five replaces the instrument, and the replacement immediately
contradicted two things this document asserted.

### The instrument

`internal/bench/probe.go` reads cgroup v2 pseudo-files directly, at 4 Hz, for the length of a
sweep; each cell attributes its own window afterwards through `Segment(from, to)`. Nobody has
to remember to sample at the right moment, which is how the earlier CPU claim came to be taken
during the single-client phase and reported as though it described the sweep.

`cpu.stat`'s `usage_usec` is a cumulative counter, so consumption over a window is a
subtraction with no sampling error at all. That is the reason for reading the files rather
than shelling out: `docker stats` reports CPU as a percentage, from which an exact core-seconds
total cannot be recovered, and it costs a fork per sample on the machine being measured.

Under Docker Desktop the usual `docker inspect .State.Pid` → `/proc/<pid>/cgroup` route does
not work — the engine runs in its own PID namespace, so `cgroup.procs` reads 0. The path is
resolved from the container ID string instead.

**Calibrated against `docker stats` before being trusted: 3.0% disagreement**, with `docker
stats` reading high because it samples the ramp outside the measured window. Had they disagreed
materially the probe would have been the thing at fault, not the reference.

### The ceiling was `TOKIO_WORKER_THREADS=2`, not the architecture

Round four concluded that parity with pgbouncer was "roughly the expected ceiling here",
because both poolers make the same four syscalls per query and the rig charges heavily for
each. The first thing the probe reported was that the proxy was using 2.18 of its 8 pinned
cores. Raising the worker count, identical load, 64 clients:

| workers | ops/s | CPU µs/op | cores peak | p99 |
|---|---|---|---|---|
| pgbouncer (1 thread) | 15,175 | **52.9** | 1.11 | 4,831 µs |
| 2 | 15,505 | 103.7 | 2.21 | 4,583 µs |
| 4 | 23,516 | 134.8 | 4.38 | 4,759 µs |
| 8 | 31,530 | 145.1 | 6.58 | 4,259 µs |

Throughput scales roughly linearly with worker threads to **2.08x pgbouncer**. The syscall
argument in round four was not wrong about syscalls being expensive; it was wrong to conclude
that a ceiling had been reached, when what had been reached was the thread count the benchmark
happened to configure.

So the honest statement of round four's result is narrower than the one it made: **the proxy
reached throughput parity with pgbouncer using two threads against pgbouncer's one, by
spending 1.96x the CPU per operation.** pgbouncer remains close to twice as efficient per
operation, and cannot scale past one thread; the proxy is less efficient and can. Which of
those matters depends on whether cores are the scarce resource.

The hand-derived estimates in round four — 120 µs against 66 µs — were close. They were also
unfalsifiable, which is the actual problem the probe fixes.

Note that efficiency *degrades* as threads are added: 103.7 → 134.8 → 145.1 µs/op. Parallelism
is being bought, not gained.

### Memory: 11x pgbouncer per idle connection, and it is not returned

The density axis previously divided a working-set figure by a client count and always passed.
It now measures resident bytes per idle connection against a floor taken before the sweep
opened anything. Five repetitions at each point, connections established and held:

| clients | pgbouncer | rust, first repetition → fifth |
|---|---|---|
| 256 | 2.4 MB (flat) | 9.9 → 14.1 MB |
| 1024 | 4.6 MB (flat) | 34.5 → 46.8 MB |
| 4096 | 13.4 MB (flat) | 122.4 → 175.6 MB |

Against the sweep floor that is ~2.8 KB per connection for pgbouncer and **~30 KB for the
proxy**. Two distinct problems, and they must not be fixed together:

1. **Footprint.** `relay.rs:27` reserves a 16 KiB `READ_CHUNK` and `wire_io.rs:30` an 8 KiB
   pre-startup buffer, both eagerly, both retained while the connection sits idle with no
   backend attached. pgbouncer's `pkt_buf` is 2 KiB.
2. **Memory that is never returned.** pgbouncer is flat to three significant figures across
   every repetition. The proxy climbs monotonically and plateaus about 40% above its first
   repetition. The growth tracks client count, so it is per-connection rather than a fixed
   leak — most likely glibc arena retention rather than a true leak, but RSS is what a
   container limit measures.

Tracked as one task, to be established in that order.

### A harness bug the measurement found

The first density run reported **−13,490 bytes per connection**. The floor was being read at
the start of each cell — moments after the previous repetition closed 256 connections, when
the pages are freed but not yet reclaimed — so it landed *above* the steady state that
followed and the subtraction went negative.

A negative is obvious. The same contamination shrinks a positive figure silently, which would
have handed the density axis to whichever arm had the dirtiest baseline. The floor is now taken
once, before the first cell, and a cell sitting below its floor reports no per-connection cost
at all rather than a number.

## Round six: the run boundary, and the change it nearly mis-scored

Round five's figures were all taken inside single invocations. This round makes the boundary
between invocations something the harness knows about, because the first measurement taken
across one produced a confident and completely wrong answer.

### The wrong answer, in full

The `attach` change below was measured against a baseline captured half an hour earlier. It read
as a clear regression:

| clients | baseline, 13:04 | after, 13:35 |
|---|---|---|
| 8 | 14,573 | 13,113 |
| 64 | 16,476 | 13,815 |

Nothing about the code changed between those two readings except thirty minutes. Re-measured
interleaved — before, after, before, after, three rounds — the same change is **inconclusive**:

| clients | round | before | after | delta |
|---|---|---|---|---|
| 8 | 1 | 12,600 | 14,296 | +13.5% |
| 8 | 2 | 14,564 | 14,457 | −0.7% |
| 8 | 3 | 14,457 | 14,602 | +1.0% |
| 64 | 1 | 15,821 | 16,539 | +4.5% |
| 64 | 2 | 16,547 | 16,436 | −0.7% |
| 64 | 3 | 16,610 | 14,700 | −11.5% |

The sign flips in both sweeps. A block design — three runs of one arm, then three of the other —
would have put every run of one arm on one side of the drift and reported whichever ordering it
happened to use. Alternating makes the drift fall on both arms equally, and what is left is that
**the change is smaller than this rig can resolve.**

`pgebench drift` on those same six invocations, which is the number this round exists to produce:

```
before   FAIL  3 invocations at 8 clients spread 13.6% (12600 to 14564)
before   PASS  3 invocations at 64 clients spread 4.8% (15821 to 16610)
after    PASS  3 invocations at 8 clients spread 2.1% (14296 to 14602)
after    FAIL  3 invocations at 64 clients spread 11.2% (14700 to 16539)
after    FAIL  3 invocations at 64 clients, p99, spread 16.1% (4575 to 5315)
```

Real rows fail. That was predicted before the threshold was applied and is the correct output,
not a calibration error.

### What the harness now does about it

`Report` carries a `RunID` and `StartedAt`, shared by every arm of one `run-arms.sh` invocation.
`Compare` then knows something it could not before:

- Arms from different invocations still compare — the reference arm is often measured once and
  kept — but **every row carries the warning**.
- The **latency axes are withheld outright.** They subtract the direct arm from the pooled ones,
  and across a run boundary that subtraction crosses drift larger than the added latency being
  measured; the difference would be mostly the gap between two invocations, reported as a
  property of the proxy.
- A report predating run identity is **flagged rather than assumed** to belong with the others.
  Silence there would assert exactly what cannot be established.

The threshold is `MaxP99SpreadRatio`, **reused rather than chosen**. The drift was already
measured, so any new number would be picked in view of the result it had to produce — the
failure the pre-registration comment in `criteria.go` exists to prevent.

Per-invocation copies go to `docs/bench/runs/<id>/` and are not committed: the answer is
machine-specific, so one rig's copies would only grow the repository.

## Round seven: the last two levers

### The `PoolManager` lock, fused rather than counted

`attach` took the manager lock twice — once for `sever_superseded`, once to claim — and
`check_in` and `try_lease` take it once each. The sever is now done inside the lock the claim
already holds.

**The reason to do it is not the lock count.** `check_in` parks a link without consulting the
epoch, so the gap between the two acquisitions is a window in which a link superseded while it
was checked out gets parked just after the scan walked past it, and the claim then takes it.
With `verify_at_checkout` set that is caught one round trip later and costs a wasted connection.
**With it off there is no second check**, and the client is handed a link to a demoted primary —
the whole failure the fence exists to prevent. One lock closes the window by construction rather
than leaving it to a backstop that configuration can switch off.

Its throughput effect is the inconclusive result above. It ships as a correctness change with
one fewer acquisition, and **no performance claim**, because the rig could not resolve one.

That leaves three acquisitions per transaction — `try_lease`, the fused sever-and-claim, and
`check_in` — where the plan set a floor of two, by also fusing `try_lease` into the claim.
**That second fusion was not done, and the reason is the result above.** The first lock removal
produced no effect this rig could resolve, so there is no evidence that the second would either,
and it is a more invasive change to the checkout path than the first. Reducing lock traffic was
justified by a performance argument; the measurement withdrew the argument. Reopen it with a rig
that can resolve a few percent, or with a profile showing the manager lock is contended — not on
the reasoning that fewer acquisitions must be faster.

### Zero-copy relay: measured against the traffic, and dropped

The plan carried this as the remaining lever: the read side is already zero-copy, the copies are
on write assembly, so keep the frames as `Bytes` and `writev` them.

It does not pay, and the reason is the traffic rather than the implementation.
`DEFAULT_INLINE_FRAME_BYTES` is 64 KiB, so `Relayed::Opaque` — the only path holding a large
contiguous payload — is reached only by frames above that. A PostgreSQL result set is not one
large frame; it is thousands of small ones. The bulk workload's own query,
`SELECT repeat('x', 512) FROM generate_series(1, 20000)`, produces 20,000 `DataRow` frames of
about 525 bytes, every one of them below the threshold.

`RawFrame` holds its 5-byte header separately from its body, so relaying those without copying
means two `IoSlice`s per frame: 40,000 slices, and `writev` takes at most `IOV_MAX`. That is
roughly 600 syscalls where the coalesced write makes a handful. Trading a `memcpy` at memory
bandwidth for hundreds of syscalls on a rig whose syscalls round four already identified as
expensive is the wrong direction.

Vectored writing would pay on a workload dominated by frames above 64 KiB — large `bytea`, wide
text columns, `COPY`. It is not what `SELECT` traffic looks like, and the harness measures
`SELECT` traffic. Recorded here rather than attempted, so the next person does not re-derive it.

## Round eight: per-connection memory, and why the first three answers were wrong

Round five found the proxy holding ~30 KB per idle connection against pgbouncer's ~2.8 KB. This
round cuts that by 28%. Most of the work went into finding out what the memory actually was,
because the obvious answer was wrong three times over.

### Wrong answer one: the read buffer

`relay.rs` reserved a 16 KiB `READ_CHUNK` per connection, and `read_target()` is evaluated on
every iteration of the select loop, so the reservation happens before the client has sent a byte.
Sixteen kilobytes times four thousand connections is the whole gap. Obvious, and nearly wrong:
rebuilding with a 2 KiB chunk and re-measuring saved **2,790 bytes**, not 14,336.

Resident memory is touched pages, not reserved bytes. An idle client writes a startup packet into
that buffer and nothing else, so exactly one page of the sixteen is ever faulted in. The reserved
remainder costs address space, which nothing measures and nobody pays for.

That also explains why 2 KiB helps at all: an allocation smaller than a page shares one with its
neighbours, where a 16 KiB one does not. pgbouncer's `pkt_buf` default is 2048 bytes.

### Wrong answer two: the measurement

The first attempt at that ablation used the density report's `bytesPerConnection` and produced
`-352`, then `+1620`, then `-880` bytes across interleaved rounds — the sign flipped. The figure
subtracts a floor captured once at sweep start, and that floor was observed ranging **4.0 to 10.7
MB for the same build**, which is larger than the effect being measured.

Reading `anon` from the cgroup before and after the connections are opened, inside one proxy
process, removes the floor from the comparison entirely. The same quantity then repeats to within
3%:

```
mem-before  [24716, 24248, 24316]  median 24316 B/conn
mem-after   [17148, 17712, 17628]  median 17628 B/conn
```

The density axis is still the right thing to publish — it is what an operator experiences — but
it is not sharp enough to attribute a change to a line of code. Two instruments, two jobs.

### Wrong answer three: the inventory

A static inventory of the allocations came to 39,013 bytes per connection. The measurement said
24,316. The inventory was not wrong about the allocations; it was wrong that allocating and
paying are the same thing. Every entry has to be asked whether it is ever *written to*.

### What actually changed

| | bytes | what it was |
|---|---|---|
| Handshake buffer released | ~8 KiB allocated | `MessageBuffer` held 8 KiB for the whole session. The backend link already released its own at `server.rs:688`; the client side simply never did, on either pooling path. |
| Adaptive read chunk | 2,790 | Starts at 2 KiB and doubles to 16 KiB only once a read comes back full. An idle connection never grows; a connection relaying a result set reaches the large chunk within a few reads. Growth is one-way, so a busy link going briefly quiet does not pay a reallocation. |
| `acquire` boxed | ~2,500 | A coroutine frame is sized by its largest suspend point, so the entire dial-connect-SCRAM-TLS chain — 4,848 bytes of it — sat inside every client's task, including the thousands that never check out a backend. |
| `greeting` dropped | ~2,240 | Already written to the client, but still live across the `txn::run` await, so it occupied the frame for the life of the connection. |

The per-connection task future went from **9,144 to 6,656 bytes**, and the whole footprint from
**24,316 to 17,628** — 28% off, against pgbouncer at 8.7x before and 6.3x after.

### The retention half turned out not to be a second defect

The task these four changes came from named two problems and insisted they be established
separately: the footprint, and memory that is never returned. A `mimalloc` experiment on the
unfixed build looked like the answer to the second — working set 22% smaller (34.9 MB against
44.9 MB at 1024 connections) and growth across repetitions cut from +32.3% to +11.6%.

Measuring growth again after the footprint fix, interleaved, two rounds:

| build | rep0 | rep4 | growth |
|---|---|---|---|
| before | 30.6 MB | 46.9 MB | +53.2% |
| before | 40.7 MB | 49.2 MB | +21.0% |
| after | 22.7 MB | 26.9 MB | +18.6% |
| after | 32.1 MB | 29.5 MB | −8.2% |

Median **+37.1% before, +5.2% after** — better than `mimalloc` achieved on the unfixed build, from
changes that touch no allocator at all. The growth was the allocator holding freed per-connection
blocks, so allocating less per connection is most of the cure; there was no independent retention
bug to find.

**So the allocator is not being swapped.** A residual +5.2% whose sign flips between rounds is
below what this rig resolves, and replacing the global allocator to chase it would be a large,
process-wide change justified by noise. Keeping the two halves separate is what made that
answerable — had they been fixed together, `mimalloc` would have shipped and been credited with a
result the buffer changes produced.

### Something else the search turned up

`GlobalStatementCache` (`stmt.rs:149`) has `insert` and `get` and **no removal path of any kind**.
Its key holds the query bytes, so every distinct (query text, parameter types) pair the proxy has
ever seen is retained for the life of the process. `maxServerStatements` does not bound it — that
bounds the per-link view.

It is invisible to both benchmarks, which is why it survived this long: density issues no queries
and throughput issues exactly one. It needs a workload with many distinct query texts, which is
the ordinary case for any client that inlines literals instead of binding parameters. Filed
separately, because it is proportional to query variety where this round's defect is proportional
to connections, and conflating them would produce a fix that cannot be measured.
