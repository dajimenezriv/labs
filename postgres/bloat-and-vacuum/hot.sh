#!/usr/bin/env bash
#
# Why one table doubles under a workload that leaves another one flat.
#
# A HOT (Heap-Only Tuple) update keeps the new row version on the same page as
# the old one and writes no index entries at all -- the indexes still point at
# the original line pointer, which now forwards to the new version. Nothing has
# to be added to any index, and the dead version can be reclaimed by an
# ordinary page access later, without waiting for a vacuum.
#
# Two conditions, and both have to hold:
#
#   1. no indexed column changed VALUE. Not "the column is unindexed" --
#      SET status = status on an indexed status is still HOT-eligible.
#   2. the new version fits on the same page. That is what fillfactor buys:
#      at the default 100 an INSERT packs the page full and leaves an update
#      nowhere to go.
#
# Each trial builds its own small table so the fillfactor can differ, and runs
# the same five full-table update passes over it.
set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql -h localhost -p 5555 -U postgres -d db -qtAX -v ON_ERROR_STOP=1)
readonly ROWS=200000
readonly PASSES=5

export PGPASSWORD=postgres
export PGOPTIONS='-c client_min_messages=warning'

sql() { "${PSQL[@]}" -c "$1"; }

# autovacuum_enabled=off for the duration of a trial. This is not the lab
# pretending autovacuum does not exist -- bloat.sh runs it on stock settings.
# It is here so the only difference between two rows of the table below is HOT
# eligibility, rather than whether a background worker happened to wake up.
trial() { # fillfactor, SET clause (PASS is substituted per pass), label
  local ff=$1 set_clause=$2 label=$3

  sql "DROP TABLE IF EXISTS lab.hot;
       CREATE TABLE lab.hot (
         id bigint PRIMARY KEY,
         sensor_id int NOT NULL,
         status text NOT NULL,
         value numeric(10, 2) NOT NULL
       ) WITH (fillfactor = $ff, autovacuum_enabled = off);
       INSERT INTO lab.hot
         SELECT g, g % 5000, 'ok', (g % 1000)::numeric
         FROM generate_series(1, $ROWS) AS g;
       CREATE INDEX hot_status ON lab.hot (status);" >/dev/null
  sql 'VACUUM ANALYZE lab.hot' >/dev/null

  local heap0 idx0
  heap0=$(sql "SELECT pg_relation_size('lab.hot')")
  idx0=$(sql "SELECT pg_indexes_size('lab.hot')")

  local p
  for ((p = 1; p <= PASSES; p++)); do
    sql "UPDATE lab.hot SET ${set_clause//PASS/$p}" >/dev/null
  done

  sql 'SELECT pg_stat_force_next_flush()' >/dev/null
  local out
  out=$(sql "SELECT round($heap0 / 1048576.0, 1) || '|' ||
                    round(pg_relation_size('lab.hot') / 1048576.0, 1) || '|' ||
                    round(pg_relation_size('lab.hot')::numeric / $heap0, 2) || '|' ||
                    round(pg_indexes_size('lab.hot')::numeric / $idx0, 2) || '|' ||
                    round(100.0 * n_tup_hot_upd / n_tup_upd, 1)
             FROM pg_stat_user_tables WHERE relname = 'hot'")

  IFS='|' read -r h0 h1 hg ig hot <<< "$out"
  printf '%-23s  %4s  %8s  %8s  %9s  %8s  %7s\n' "$label" "$ff" "$h0" "$h1" "${hg}x" "${ig}x" "${hot}%"
}

if ! sql 'SELECT 1' >/dev/null 2>&1; then
  echo "postgres is not answering on 5555" >&2
  exit 1
fi
sql 'CREATE SCHEMA IF NOT EXISTS lab' >/dev/null

echo "$ROWS rows, $PASSES full-table update passes each"
echo
printf '%-23s  %4s  %8s  %8s  %9s  %8s  %7s\n' \
  'what the UPDATE sets' ff 'heap MB' 'heap MB' heap idx HOT
printf '%-23s  %4s  %8s  %8s  %9s  %8s  %7s\n' \
  '' '' before after growth growth ''
printf '%-23s  %4s  %8s  %8s  %9s  %8s  %7s\n' \
  ---------------------- ---- -------- -------- --------- -------- -------

trial 100 "status = 'st-PASS'"    'indexed col, new value'
trial  70 "status = 'st-PASS'"    'indexed col, new value'
trial  70 "status = status"       'indexed col, same value'
trial 100 "value = value + 1"     'unindexed col'
trial  90 "value = value + 1"     'unindexed col'
trial  70 "value = value + 1"     'unindexed col'

sql 'DROP TABLE IF EXISTS lab.hot' >/dev/null
