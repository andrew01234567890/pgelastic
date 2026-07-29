#!/usr/bin/env bash
# The live-migration demo, driven end to end.
#
# It stands up a pool with two three-node PostgreSQL 18 instances and a two-replica proxy
# fleet, puts a million rows in the tenant, runs a writer *through the pool's Service*, and
# migrates that tenant onto the other instance while the writer keeps writing.
#
# What it reports is measured rather than claimed: the pause is the longest gap between two
# successful writes as the client saw it, and the verdict is the internal/verify oracle
# replaying its ledger against the surviving primary.
#
# The operator must already be deployed in the cluster and cert-manager installed. The
# control listener the cutover drives is issued by cert-manager per pool, and without it the
# migration falls back to the routing table alone and queues nobody.
set -euo pipefail

NS="${NS:-pgelastic-demo}"
CONTEXT="${CONTEXT:-docker-desktop}"
TENANT="${TENANT:-test}"
TARGET="${TARGET:-demo-b}"
ROWS="${ROWS:-1000000}"
# The writer runs until the migration is over rather than for a fixed window: a run that
# stopped writing before the cutover would report a flawless pause it was not present for.
MAX_DURATION="${MAX_DURATION:-30m}"
BASELINE="${BASELINE:-30s}"
WRITERS="${WRITERS:-4}"
OUT="${OUT:-$(mktemp -d /tmp/pgelastic-demo.XXXXXX)}"
LOCAL_PORT="${LOCAL_PORT:-16432}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
kube() { kubectl --context="$CONTEXT" "$@"; }
psql_on() { kube -n "$NS" exec "$1" -c postgres -- psql -U postgres -d "$2" -tAqc "$3"; }
say() { printf '\n=== %s\n' "$*"; }

mkdir -p "$OUT"
say "output directory: $OUT"

say "applying the pool, the instances and the tenants into $NS"
kube create namespace "$NS" --dry-run=client -o yaml | kube apply -f -
kube apply -n "$NS" -f "$here/manifests/00-pool.yaml"
kube apply -n "$NS" -f "$here/manifests/10-instances.yaml"

say "waiting for both instances to become Ready"
for instance in demo-a "$TARGET"; do
  for _ in $(seq 1 120); do
    phase="$(kube -n "$NS" get pginstance "$instance" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$phase" = "Ready" ] && break
    sleep 10
  done
  [ "$(kube -n "$NS" get pginstance "$instance" -o jsonpath='{.status.phase}')" = "Ready" ] ||
    { echo "instance $instance never became Ready" >&2; exit 1; }
done

kube apply -n "$NS" -f "$here/manifests/20-tenants.yaml"

say "waiting for both tenant databases to exist"
for _ in $(seq 1 60); do
  ready="$(kube -n "$NS" get pgtenant -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' |
    grep -c '^Ready$' || true)"
  [ "$ready" -ge 2 ] && break
  sleep 5
done

primary="$(kube -n "$NS" get pginstance demo-a -o jsonpath='{.status.currentPrimary}')"
say "seeding $ROWS rows into $TENANT on $primary"
# The ops role is the identity the proxy's backend leg authenticates as, and the writer
# reaches PostgreSQL only through the proxy. It therefore needs to be able to create and
# write its own relation: PostgreSQL 15 and later do not grant CREATE on public to PUBLIC.
psql_on "$primary" "$TENANT" "GRANT CREATE, USAGE ON SCHEMA public TO pgelastic_ops"
# The oracle's relation is created here rather than by the writer, and granted explicitly.
# A relation the writer creates through the proxy is owned by the ops role, and the schema
# copy recreates it on the target owned by the tenant - after which the ops role cannot read
# back what it wrote, and the check fails on a permission rather than on a lost row.
psql_on "$primary" "$TENANT" "
  CREATE TABLE IF NOT EXISTS \"set\" (value bigint PRIMARY KEY);
  GRANT SELECT, INSERT, UPDATE, DELETE ON \"set\" TO pgelastic_ops;
  CREATE TABLE IF NOT EXISTS bulk (id bigint PRIMARY KEY, payload text NOT NULL);
  INSERT INTO bulk SELECT g, repeat('x', 64) FROM generate_series(1, $ROWS) g
  ON CONFLICT DO NOTHING;"
psql_on "$primary" "$TENANT" "SELECT count(*) FROM bulk"

say "waiting for the proxy fleet"
kube -n "$NS" rollout status deployment/demo-pool-proxy --timeout=10m

say "forwarding the pool's Service to 127.0.0.1:$LOCAL_PORT"
kube -n "$NS" port-forward "service/demo-pool-proxy" "$LOCAL_PORT:5432" >"$OUT/forward.log" 2>&1 &
forward=$!
trap 'kill $forward ${writer:-} 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do
  grep -q "Forwarding from" "$OUT/forward.log" && break
  sleep 1
done
grep -q "Forwarding from" "$OUT/forward.log" ||
  { echo "the port-forward never came up: $(cat "$OUT/forward.log")" >&2; exit 1; }

dsn="host=127.0.0.1 port=$LOCAL_PORT user=pgelastic_ops dbname=$TENANT sslmode=disable"

say "starting the writer through the pool Service"
# Built rather than `go run`, so the process this script signals is the writer itself: go run
# does not pass a terminating signal on, and a writer that outlived the script would go on
# holding connections to a pool nobody is watching any more.
(cd "$repo" && go build -o "$OUT/demo-writer" ./hack/demo/writer)
"$OUT/demo-writer" \
  --dsn "$dsn" \
  --ledger "$OUT/ledger.log" \
  --journal "$OUT/attempts.csv" \
  --writers "$WRITERS" \
  --duration "$MAX_DURATION" \
  --baseline "$BASELINE" >"$OUT/writer.json" &
writer=$!

say "letting the baseline settle for $BASELINE"
sleep "$BASELINE"

say "migrating $TENANT onto $TARGET while the writer keeps writing"
kube apply -n "$NS" -f "$here/manifests/30-migration.yaml"
for _ in $(seq 1 180); do
  phase="$(kube -n "$NS" get pgtenantmigration move-test -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in Completed|Failed|Aborted) break ;; esac
  sleep 5
done
kube -n "$NS" get pgtenantmigration move-test -o yaml >"$OUT/migration.yaml"
say "migration finished in phase $(kube -n "$NS" get pgtenantmigration move-test -o jsonpath='{.status.phase}')"

# Re-granting after the move, and it should not be necessary. The online path copies the
# schema with `pg_dump --schema-only --no-owner --no-privileges`, so every table on the
# target is owned by the restoring superuser with no ACL at all: the tenant's own roles lose
# every grant they had. Until that is fixed the writes that arrive between the flip and this
# line are refused with 42501, and they show up in the report below as failures rather than
# as anything the pause did. See docs/demo.md.
target_primary="$(kube -n "$NS" get pginstance "$TARGET" -o jsonpath='{.status.currentPrimary}')"
psql_on "$target_primary" "$TENANT" \
  "GRANT SELECT, INSERT, UPDATE, DELETE ON \"set\" TO pgelastic_ops" || true

# A little longer than the cutover, so the window the report calls "during" contains the
# recovery as well as the pause.
sleep 15
kill -TERM "$writer" 2>/dev/null || true
wait "$writer"
say "what the client measured"
cat "$OUT/writer.json"

say "what the operator reported about itself"
kube -n "$NS" get pgtenantmigration move-test \
  -o jsonpath='{.status.phase}{"\n"}pauseDurationMillis={.status.pauseDurationMillis}{"\n"}clientPauseMillis={.status.clientPauseMillis}{"\n"}queuedClients={.status.queuedClients}{"\n"}'

say "the oracle's verdict, read back through the pool Service"
(cd "$repo" && go run ./cmd/verify check --dsn "$dsn" --ledger "$OUT/ledger.log") ||
  { echo "the durability oracle refused this run" >&2; exit 1; }

say "artefacts are in $OUT"
