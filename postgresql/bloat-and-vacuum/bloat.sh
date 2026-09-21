#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql postgres://postgres:postgres@localhost:5555/db -qtAX -v ON_ERROR_STOP=1)
readonly PASSES=5

export PGOPTIONS='-c client_min_messages=warning'

sql() { "${PSQL[@]}" -c "$1"; }

if ! sql 'SELECT 1 FROM lab.readings LIMIT 1' >/dev/null 2>&1; then
  echo "no lab.readings: psql ... -f seed.sql (the stack must be up)" >&2
  exit 1
fi

row() {
  # Backends accumulate their stats locally and flush them on a timer. Force the flush.
  sql 'SELECT pg_stat_force_next_flush()' >/dev/null
  sql "SELECT pg_relation_size('lab.readings') / 1048576 || '|' ||
              pg_indexes_size('lab.readings') / 1048576 || '|' ||
              n_live_tup || '|' || n_dead_tup || '|' ||
              n_tup_hot_upd || '|' || vacuum_count || '|' || autovacuum_count
       FROM pg_stat_user_tables WHERE relname = 'readings'"
}

emit() {
  IFS='|' read -r heap idx live dead hot vac avac <<< "$(row)"
  printf '%-16s  %7s  %7s  %10s  %10s  %8s  %7s  %7s\n' \
    "$1" "$heap" "$idx" "$live" "$dead" "$hot" "$vac" "$avac"
}

header() {
  printf '%-16s  %7s  %7s  %10s  %10s  %8s  %7s  %7s\n' \
    step 'heap MB' 'idx MB' n_live_tup n_dead_tup hot_upd vacuums autovacs
  printf '%-16s  %7s  %7s  %10s  %10s  %8s  %7s  %7s\n' \
    ---------------- ------- ------- ---------- ---------- -------- ------- -------
}

# Rewrites every row. status is indexed, so none of these can be a HOT update
# and every one of them lands on a new page -- see hot.sh for the other case.
churn() { sql "UPDATE lab.readings SET status = 'pass-$1', updated_at = now()" >/dev/null; }

echo "== 1. repeated full-table updates, autovacuum on stock settings =="
header
emit baseline
for ((p = 1; p <= PASSES; p++)); do
  churn "$p"
  emit "update pass $p"
done

# Reclaims dead versions into the free space map. The file does not shrink:
# space below the high water mark becomes reusable, not returned.
echo
echo "== 2. VACUUM =="
header
t0=$(date +%s%3N)
sql 'VACUUM lab.readings' >/dev/null
echo "took $(( $(date +%s%3N) - t0 )) ms"
emit 'after VACUUM'

# The point of part 2: that reclaimed space is a budget the next passes spend
# instead of extending the file.
echo
echo "== 3. the same updates again, now that there is free space to reuse =="
header
for ((p = 1; p <= PASSES; p++)); do
  churn "reuse-$p"
  emit "update pass $p"
done

# Rewrites the table into a new file and swaps it in. Returns the space, and
# takes an ACCESS EXCLUSIVE lock for the whole rewrite -- no reads, no writes,
# and it needs room for a second copy of the table on disk while it runs.
#
# Watch n_dead_tup in this last row: it still reads 5000000 against a table
# that was just rewritten and has none. VACUUM FULL does not reset the
# counter, so the one column people monitor for bloat goes stale exactly when
# the bloat is gone. A plain VACUUM afterwards is what zeroes it.
echo
echo "== 4. VACUUM FULL =="
header
t0=$(date +%s%3N)
sql 'VACUUM FULL lab.readings' >/dev/null
echo "took $(( $(date +%s%3N) - t0 )) ms"
emit 'after VACUUM FULL'
