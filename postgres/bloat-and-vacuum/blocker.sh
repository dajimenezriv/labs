#!/usr/bin/env bash
#
# VACUUM cannot remove a dead row version that somebody might still need to
# see. "Somebody" is defined by the xmin horizon: the oldest transaction id any
# session could still be looking at. One session holding that horizon back
# freezes cleanup for the whole cluster -- not just for the table it touched,
# and not just for its own database.
#
# The folklore blames "idle in transaction". That is close enough to be
# dangerous, because it is wrong in both directions: a read-committed session
# can sit idle in a transaction all day without holding the horizon, and a
# session that is not idle at all -- a long-running analytics query -- holds it
# exactly the same way. What matters is whether the session holds
#
#   backend_xmin   a snapshot, so it can still see old versions, or
#   backend_xid    an unfinished write, so its own versions are not yet settled
#
# Three holders below, all of them "idle in transaction", with different
# answers.
set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql -h localhost -p 5555 -U postgres -d db -qtAX -v ON_ERROR_STOP=1)
readonly ROWS=200000
readonly FIFO=$(mktemp -u)

export PGPASSWORD=postgres
export PGOPTIONS='-c client_min_messages=warning'

sql() { "${PSQL[@]}" -c "$1"; }
holder_pid=

# A psql fed from a fifo, so the transaction stays open between commands
# instead of ending when the client exits.
open_holder() { # SQL to run inside the transaction
  mkfifo "$FIFO"
  psql -h localhost -p 5555 -U postgres -d db -qtAX < "$FIFO" >/dev/null 2>&1 &
  holder_pid=$!
  exec 9>"$FIFO"
  echo "$1" >&9
  sleep 2 # let it reach idle-in-transaction before anything is measured
}

close_holder() {
  [[ -n $holder_pid ]] || return 0
  echo 'COMMIT;' >&9
  exec 9>&-
  wait "$holder_pid" 2>/dev/null || true
  holder_pid=
  rm -f "$FIFO"
}
trap 'close_holder; rm -f "$FIFO"' EXIT

reset_table() {
  sql "DROP TABLE IF EXISTS lab.blocked;
       CREATE TABLE lab.blocked (id bigint PRIMARY KEY, v int NOT NULL)
         WITH (autovacuum_enabled = off);
       INSERT INTO lab.blocked SELECT g, g FROM generate_series(1, $ROWS) AS g;" >/dev/null
  sql 'VACUUM ANALYZE lab.blocked' >/dev/null
}

# What the holder looks like from the outside, which is all a DBA gets.
horizon() {
  sql "SELECT coalesce(backend_xmin::text, '-') || '|' || coalesce(backend_xid::text, '-')
       FROM pg_stat_activity
       WHERE state = 'idle in transaction' AND datname = 'db'
       LIMIT 1"
}

# VACUUM VERBOSE says outright how many dead versions it had to leave behind.
# n_dead_tup does not: the counter drops either way.
vacuum_report() {
  "${PSQL[@]}" -c 'VACUUM (VERBOSE) lab.blocked' 2>&1 |
    sed -n 's/.*tuples: \([0-9]*\) removed.*[^0-9]\([0-9]*\) are dead but not yet removable.*/\1|\2/p' |
    head -1
}

case_() { # label, holder SQL
  reset_table
  open_holder "$2"
  IFS='|' read -r xmin xid <<< "$(horizon)"
  # id > 1 because holder 3 has row 1 locked; a full-table UPDATE would block
  # on it and this script would hang rather than measure anything.
  sql "UPDATE lab.blocked SET v = v + 1 WHERE id > 1" >/dev/null
  IFS='|' read -r removed stuck <<< "$(vacuum_report)"
  printf '%-34s  %9s  %9s  %9s  %11s\n' "$1" "$xmin" "$xid" "$removed" "$stuck"
  close_holder
}

if ! sql 'SELECT 1' >/dev/null 2>&1; then
  echo "postgres is not answering on 5555" >&2
  exit 1
fi
sql 'CREATE SCHEMA IF NOT EXISTS lab' >/dev/null

echo "== who actually holds the xmin horizon =="
echo "all three holders are 'idle in transaction'; $ROWS rows are updated under each"
echo
printf '%-34s  %9s  %9s  %9s  %11s\n' \
  'the open transaction' backend_ backend_ 'tuples' 'dead, not'
printf '%-34s  %9s  %9s  %9s  %11s\n' \
  '' xmin xid removed removable
printf '%-34s  %9s  %9s  %9s  %11s\n' \
  ---------------------------------- --------- --------- --------- -----------

case_ 'repeatable read, read-only' \
      'BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM lab.blocked;'
case_ 'read committed, read-only' \
      'BEGIN ISOLATION LEVEL READ COMMITTED; SELECT count(*) FROM lab.blocked;'
case_ 'read committed, has written' \
      'BEGIN ISOLATION LEVEL READ COMMITTED; UPDATE lab.blocked SET v = v WHERE id = 1;'

# Same table, same dead rows, nothing changed except that the holder is gone.
echo
echo "== the blocked case again, then the holder commits =="
reset_table
open_holder 'BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM lab.blocked;'
sql "UPDATE lab.blocked SET v = v + 1" >/dev/null

# The production question is not "is a vacuum running" but "how far behind is
# the horizon", which is this, and it grows for as long as the holder sits there.
age_query="SELECT coalesce(max(age(backend_xmin))::text, '-')
           FROM pg_stat_activity WHERE backend_xmin IS NOT NULL"

for i in 1 2 3; do
  sql "UPDATE lab.blocked SET v = v + 1 WHERE id > 1" >/dev/null
  IFS='|' read -r removed stuck <<< "$(vacuum_report)"
  printf '  vacuum %d, holder still open:  %6s removed, %7s not removable, heap %5s MB, horizon %s xids behind\n' \
    "$i" "$removed" "$stuck" \
    "$(sql "SELECT round(pg_relation_size('lab.blocked') / 1048576.0, 1)")" \
    "$(sql "$age_query")"
done

close_holder
IFS='|' read -r removed stuck <<< "$(vacuum_report)"
printf '  vacuum 4, holder committed:   %6s removed, %7s not removable, heap %5s MB\n' \
  "$removed" "$stuck" "$(sql "SELECT round(pg_relation_size('lab.blocked') / 1048576.0, 1)")"

echo
echo "== the other things that hold the horizon =="
# All of these are the same mechanism wearing a different hat, and none of them
# show up as a session someone can spot in pg_stat_activity.
sql "SELECT '  replication slots:     ' ||
            coalesce(string_agg(slot_name || ' (xmin ' || coalesce(xmin::text, 'null') || ')', ', '), 'none')
     FROM pg_replication_slots"
sql "SELECT '  prepared transactions: ' ||
            coalesce(string_agg(gid, ', '), 'none') FROM pg_prepared_xacts"
sql "SELECT '  idle_in_transaction_session_timeout = ' || setting || ' (0 = no limit)'
     FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'"

sql 'DROP TABLE IF EXISTS lab.blocked' >/dev/null
