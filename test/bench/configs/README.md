# Benchmark proxy configurations

One file per cell of the symmetry table. They exist so that the two proxies are never
configured by hand at the point of measurement, and so that the difference between two runs
is a diff rather than a recollection.

Every file here is rendered against the same PostgreSQL (`pgebench-pg` on the `pgebench`
Docker bridge) with the same credentials, the same pool size and the same frame limits. The
only thing that varies between files is the one axis named in the filename.

Every file pins `resetPolicy` explicitly rather than taking the default, and now has to: the
shipped default moved from `discardAll` to `dirtyTracked`, and the numbers published in
`docs/bench.md` were all taken under `discardAll`. Pinning keeps them reproducible. A sweep of
the new default belongs in a fresh set of arms, not in a silent redefinition of the old ones.

| File | Pool mode | `fence.verifyAtCheckout` | Role |
|---|---|---|---|
| `txn-fence-off.toml` | transaction | `false` | **Primary decision number.** Proxy overhead in isolation. |
| `txn-fence-on.toml` | transaction | `true` | What production runs. Reported alongside, never instead. |
| `session-fence-off.toml` | session | `false` | Session-mode overhead, where checkout is per connection. |
| `txn-reset-none.toml` | transaction | `false` | `resetPolicy = "none"`. The like-for-like row against pgbouncer — see below. |
| `pgbouncer.ini` + `userlist.txt` | transaction | n/a | **External reference.** Never a gate — see below. |

## Why the reset policy gets its own file

`ResetPolicy::DiscardAll` issues a `DISCARD ALL` on **every release**, and in transaction mode
a release happens per transaction. The operator ships `resetPolicy = "discardAll"`, so the
shipped proxy makes **two** backend round trips per client transaction: the client's query,
and the scrub.

pgbouncer's `server_reset_query_always` defaults to `0`, which means it runs no reset query in
transaction mode at all — **one** round trip per transaction.

Comparing those two directly would be comparing one round trip against two and reporting the
gap as an implementation difference. `txn-reset-none.toml` is the row that matches pgbouncer
term for term; `txn-fence-off.toml` remains the row that matches production. Both are
reported, for exactly the reason both fence settings are.

## Why pgbouncer is here

Rust versus Go on its own is a closed comparison: it says which of the two is faster, not
whether either is any good. If both are half the speed of the pooler most people already run,
the interesting question was never which language to write it in.

pgbouncer is configured term-for-term against `txn-fence-off.toml` — transaction pooling,
pool size 100, 5000 client connections, SCRAM on the client leg, TLS off on both. What cannot
be matched is the work itself: pgbouncer has no epoch fence, no tenant routing, no capacity
allocator, no quiesce, no per-tenant backend credentials and no control API. It is doing
strictly less, which is why it produces a `REFERENCE` row carrying a ratio and no verdict.
A pass or fail against it would be scoring two different programs against one threshold.

It is also single-threaded, where the proxy runs at `TOKIO_WORKER_THREADS=2` by operator
default. The report states the core count each arm actually used rather than implying they
were equal.

## Why the fence gets its own axis

`PoolManager::verify_epoch` runs on **every checkout**, on parked links as well as fresh
ones, and `fence.verifyAtCheckout` defaults to `true`. The probe is a real `Query` round trip
to PostgreSQL, drained to its own `ReadyForQuery`.

In transaction mode a checkout happens per transaction, so with the default the proxy makes
**two** backend round trips per client transaction rather than one. A proxy that skipped the
fence would therefore measure roughly twice as fast for reasons that have nothing to do with
the language it is written in. Both arms must always be set the same way, and both settings
must always be reported, because publishing only the fence-on number understates the proxy's
own cost and publishing only fence-off overstates its effect on production.

There is no middle setting: `server.rs` spawns the background prober if and only if
`verify_at_checkout` is set, so "checkout probe without background prober" is not a
configuration that exists.

## Why `stall.enabled = false` everywhere

The write-stall monitor polls `pg_stat_replication` on a timer. Against a single
non-replicating PostgreSQL it has nothing to find, so leaving it on would add a periodic
query that is noise in the measurement rather than a property of the proxy. It is disabled
identically in every file, which is what keeps that a symmetry rather than a thumb on the
scale.

## Why there is no TLS file yet

TLS is an axis the plan calls for and this directory does not cover yet. Adding it means
generating a certificate and key, mounting them, and setting `listen.tls` plus
`backend.tls.mode` — and doing it identically for both proxies, pinned to the same TLS 1.3
cipher suite, since `crypto/tls` and `rustls` do not pick the same one by default.
