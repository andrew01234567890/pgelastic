#!/usr/bin/env bash
# Continuous writer for the live-migration demo.
#
# Every attempt is journalled before it is made and only marked committed on a definite
# success, so a row that vanishes cannot be mistaken for a row that was never written.
# Anything ambiguous is recorded as indeterminate and is never counted as either.
set -uo pipefail

NS="${NS:-pgelastic-demo}"
POD="${POD:-demo-a-1}"
DB="${DB:-test}"
LEDGER="${LEDGER:-/tmp/pgelastic-demo-ledger.txt}"
LATENCY="${LATENCY:-/tmp/pgelastic-demo-latency.txt}"
START_ID="${START_ID:-2000000}"

: >"$LEDGER"
: >"$LATENCY"

id=$START_ID
while [ -f /tmp/pgelastic-demo-writer.run ]; do
  id=$((id + 1))
  echo "ATTEMPTED $id" >>"$LEDGER"
  t0=$(date +%s%N)
  out=$(kubectl -n "$NS" exec "$POD" -c postgres -- \
        psql -U postgres -d "$DB" -v ON_ERROR_STOP=1 -tAc \
        "INSERT INTO orders (id, customer, amount) VALUES ($id, 'live-writer', 1.00);" 2>&1)
  rc=$?
  t1=$(date +%s%N)
  ms=$(( (t1 - t0) / 1000000 ))
  echo "$(date +%s%N) $ms $rc" >>"$LATENCY"
  if [ $rc -eq 0 ] && [ "$out" = "INSERT 0 1" ]; then
    echo "COMMITTED $id" >>"$LEDGER"
  else
    # Not a failure: a write whose fate is unknown must never be claimed either way.
    echo "INDETERMINATE $id -- $out" >>"$LEDGER"
  fi
done
