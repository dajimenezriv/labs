#!/usr/bin/env bash
#
# The same 5M rows, the same index, the same page size. Only the way the page
# is addressed changes.
#
#   offset   ORDER BY created_at DESC, id DESC LIMIT 20 OFFSET n
#   keyset   WHERE (created_at, id) < (cursor) ORDER BY ... LIMIT 20
#
# Both plans are an index scan. The difference is what the index scan is asked
# to do: offset walks n entries and throws them away before the first row
# counts, keyset descends the btree straight to the cursor. So the cost of a
# page is a function of how deep it is for one and not for the other.
#
# Timing comes from pg_stat_statements rather than a single EXPLAIN, because
# one execution of a millisecond query is mostly noise. pg_stat_statements
# normalises literals, so every depth would collapse into one row -- hence the
# reset before each measurement.
set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql -h localhost -p 5555 -U postgres -d db -qtAX -v ON_ERROR_STOP=1)
readonly DEPTHS=(0 1000 10000 100000 1000000 4000000)
readonly PAGE=20
readonly WARMUP=2
readonly RUNS=10
readonly EXPLAINS=explain.txt

export PGPASSWORD=postgres

sql() { "${PSQL[@]}" -c "$1"; }

if ! sql 'SELECT 1 FROM lab.events LIMIT 1' >/dev/null 2>&1; then
  echo "no lab.events: psql ... -f seed.sql (the stack must be up)" >&2
  exit 1
fi

sql 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' >/dev/null

offset_q() { echo "SELECT * FROM lab.events ORDER BY created_at DESC, id DESC LIMIT $PAGE OFFSET $1"; }
keyset_q() {
  echo "SELECT * FROM lab.events WHERE (created_at, id) < ('$1'::timestamptz, $2::bigint)
        ORDER BY created_at DESC, id DESC LIMIT $PAGE"
}

# Mean execution time and buffer counts per call, over RUNS executions of one
# query shape, with the cache already warm.
measure() {
  local q=$1 i
  for ((i = 0; i < WARMUP; i++)); do sql "$q" >/dev/null; done
  sql 'SELECT pg_stat_statements_reset()' >/dev/null
  for ((i = 0; i < RUNS; i++)); do sql "$q" >/dev/null; done
  sql "SELECT round(mean_exec_time::numeric, 3) || '|' ||
              round(shared_blks_hit::numeric / calls, 1) || '|' ||
              round(shared_blks_read::numeric / calls, 1)
       FROM pg_stat_statements
       WHERE query LIKE '%lab.events%' AND query NOT LIKE '%pg_stat_statements%'
       ORDER BY calls DESC LIMIT 1"
}

: > "$EXPLAINS"
printf '%-9s  %-7s  %9s  %11s  %11s\n' depth strategy 'mean ms' 'blks hit' 'blks read'
printf '%-9s  %-7s  %9s  %11s  %11s\n' --------- ------- --------- ----------- -----------

for d in "${DEPTHS[@]}"; do
  # The cursor a client would have been handed by the previous page. Taken with
  # OFFSET here only because the lab needs to start at an arbitrary depth; a
  # real client already holds it and never pays for this.
  IFS='|' read -r ts id < <(sql "SELECT created_at || '|' || id FROM lab.events
                                 ORDER BY created_at DESC, id DESC OFFSET $d LIMIT 1")

  for strategy in offset keyset; do
    case $strategy in
      offset) q=$(offset_q "$d") ;;
      keyset) q=$(keyset_q "$ts" "$id") ;;
    esac

    { echo "== $strategy depth $d"; sql "EXPLAIN (ANALYZE, BUFFERS) $q"; echo; } >> "$EXPLAINS"

    IFS='|' read -r ms hit read_ <<< "$(measure "$q")"
    printf '%-9s  %-7s  %9s  %11s  %11s\n' "$d" "$strategy" "$ms" "$hit" "$read_"
  done
done

echo
echo "plans in $(pwd)/$EXPLAINS"
