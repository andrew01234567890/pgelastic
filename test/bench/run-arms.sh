#!/usr/bin/env bash
# Runs the benchmark arms, one at a time, and writes one report per arm and workload.
#
# Sequential on purpose. Two arms running at once would contend for the same cores and the
# same PostgreSQL, and the numbers would describe the contention rather than either arm.
#
# Every arm is measured through the same client-leg hop: the load generator runs on the host
# and reaches PostgreSQL, the proxy and pgbouncer through a published port. Under WSL2 there
# is no docker0 and a published port crosses a userspace relay, so what matters is that the
# relay is crossed exactly once by every arm. The pooler-to-PostgreSQL leg stays on the
# bridge, which is why arms that differ in backend round-trip count - fence on versus off -
# are still comparable.
#
# Select arms with ARMS, e.g. ARMS=pgbouncer ./test/bench/run-arms.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

OUT="${BENCH_DIR:-docs/bench}"
DURATION="${BENCH_DURATION:-20s}"
WARMUP="${BENCH_WARMUP:-5s}"
REPS="${BENCH_REPETITIONS:-5}"
CONCURRENCY="${BENCH_CONCURRENCY:-1,8,64,256}"
WORKLOADS="${BENCH_WORKLOADS:-throughput churn}"
# Offered rate for the latency workload, which is refused without one. Held constant across
# the concurrency sweep and set well below the measured ceiling: offering anything near
# saturation makes every cell an overrun, and the percentiles then describe a queue rather
# than the pooler - which is the closed-loop measurement this workload exists to escape.
RATE="${BENCH_RATE:-5000}"
# A suffix keeps a simple-protocol sweep from overwriting an extended-protocol one; the two
# are not comparable and must not land in the same file.
SIMPLE="${BENCH_SIMPLE:-}"
SUFFIX="${BENCH_SUFFIX:-${SIMPLE:+-simple}}"
ARMS="${ARMS:-direct rust rust-fence-on rust-session rust-1worker pgbouncer}"

LOADGEN_CPUS="${BENCH_LOADGEN_CPUS:-10-13,26-29}"
POOLER_CPUS="${BENCH_PROXY_CPUS:-6-9,22-25}"
WORKERS="${BENCH_WORKERS:-2}"
PGBOUNCER_IMG="${PGBOUNCER_IMG:-edoburu/pgbouncer:latest}"

PG_DSN="postgres://bench:bench@localhost:15432/bench?sslmode=disable"
POOLER_DSN="postgres://bench:bench@localhost:16432/bench?sslmode=disable"

mkdir -p "$OUT"
go build -trimpath -o bin/pgebench ./test/bench/cmd/pgebench

stop_poolers() {
	docker rm -f pgebench-proxy pgebench-pgbouncer >/dev/null 2>&1 || true
}

# start_proxy waits for /readyz rather than sleeping a guessed interval, so a slow start
# lengthens the setup instead of silently measuring a proxy that is not serving yet.
start_proxy() {
	local config="$1"
	local workers="${2:-$WORKERS}"
	stop_poolers
	docker run -d --name pgebench-proxy --network pgebench \
		--cpuset-cpus "$POOLER_CPUS" --memory 2g \
		-e TOKIO_WORKER_THREADS="$workers" -e RUST_LOG=warn \
		-v "$ROOT/test/bench/configs:/etc/pgelastic/proxy:ro" \
		-p 16432:6432 -p 19127:9127 \
		pgelastic/proxy:latest --config "/etc/pgelastic/proxy/$config" >/dev/null
	local attempt
	for attempt in $(seq 1 60); do
		if [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:19127/readyz)" = "200" ]; then
			return 0
		fi
		sleep 1
	done
	echo "the proxy never reported ready; logs follow" >&2
	docker logs pgebench-proxy >&2 2>&1 || true
	return 1
}

# start_pgbouncer has no readiness endpoint to wait on, so readiness is established the only
# way that actually proves it: by running a query through it.
start_pgbouncer() {
	stop_poolers
	docker run -d --name pgebench-pgbouncer --network pgebench \
		--cpuset-cpus "$POOLER_CPUS" --memory 2g \
		-v "$ROOT/test/bench/configs/pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini:ro" \
		-v "$ROOT/test/bench/configs/userlist.txt:/etc/pgbouncer/userlist.txt:ro" \
		-p 16432:6432 \
		"$PGBOUNCER_IMG" >/dev/null

	local attempt
	for attempt in $(seq 1 60); do
		if bin/pgebench probe --dsn "$POOLER_DSN" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	echo "pgbouncer never served a query; logs follow" >&2
	docker logs pgebench-pgbouncer >&2 2>&1 || true
	return 1
}

measure() {
	local target="$1" dsn="$2" workload="$3" label="$4" pooler="${5:-}"
	echo "=== $label / $workload ==="
	taskset -c "$LOADGEN_CPUS" bin/pgebench run \
		--target "$target" --dsn "$dsn" ${pooler:+--pooler "$pooler"} \
		--workload "$workload" --concurrency "$CONCURRENCY" \
		--duration "$DURATION" --warmup "$WARMUP" --repetitions "$REPS" \
		${SIMPLE:+--simple-protocol} \
		$([ "$workload" = latency ] && echo "--rate $RATE") \
		--out "$OUT/$label$SUFFIX-$workload.json"
}

run_arm() {
	local arm="$1" target dsn pooler=""
	case "$arm" in
	direct)
		stop_poolers
		target=direct dsn="$PG_DSN"
		;;
	rust)
		start_proxy txn-fence-off.toml
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	rust-fence-on)
		start_proxy txn-fence-on.toml
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	rust-session)
		start_proxy session-fence-off.toml
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	rust-reset-dirty)
		# The real like-for-like row against pgbouncer. dirtyTracked reuses a clean link
		# with no reset at all, so a SELECT workload costs one backend round trip - which
		# is what pgbouncer does in transaction mode.
		start_proxy txn-reset-dirty.toml
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	rust-reset-none)
		# The other like-for-like row against pgbouncer. Our resetPolicy = "discardAll"
		# issues a DISCARD ALL on every release, which in transaction mode is one extra
		# backend round trip per client transaction. pgbouncer runs no reset query in
		# transaction mode by default, so comparing the two without this arm would be
		# comparing two round trips against one and calling the gap an implementation.
		start_proxy txn-reset-none.toml
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	rust-1worker)
		# The like-for-like row against pgbouncer, which is single-threaded by design.
		# Without it the reference comparison silently credits us with twice the runtime
		# threads and reads as a language result.
		start_proxy txn-fence-off.toml 1
		target=rust dsn="$POOLER_DSN" pooler=pgebench-proxy
		;;
	pgbouncer)
		start_pgbouncer
		target=pgbouncer dsn="$POOLER_DSN" pooler=pgebench-pgbouncer
		;;
	*)
		echo "unknown arm $arm" >&2
		return 1
		;;
	esac

	local workload
	for workload in $WORKLOADS; do
		measure "$target" "$dsn" "$workload" "$arm" "$pooler"
	done
}

for arm in $ARMS; do
	run_arm "$arm"
done

stop_poolers
echo "wrote reports to $OUT"
