#!/usr/bin/env bash
# Part 3: somebody ran a DELETE with the wrong WHERE clause and it committed.
# The data directory is fine. Recover the database to the instant before it.

cd "$(dirname "$0")"
source ./lib.sh
require_seed

hr() { printf '%s\n' "------------------------------------------------------------"; }

# With recovery_target_action=pause the server stops at the target and waits,
# still in recovery, so the target can be checked before it is made permanent.
wait_paused() {
  local t0; t0=$(ms)
  while (( ($(ms) - t0) < 300000 )); do
    [[ "$(rsql 'SELECT pg_get_wal_replay_pause_state()' 2>/dev/null || true)" == "paused" ]] && return 0
  done
  return 1
}

echo "== 1. base backup, then normal traffic =="
hr
take_backup /backups/pitr
for i in $(seq 1 20); do
  sql "INSERT INTO lab.orders (customer_id, status, amount)
       SELECT g % 1000, 'paid', 3.00 FROM generate_series(1, 2000) g" >/dev/null
done
before_rows=$(sql 'SELECT count(*) FROM lab.orders')
victim_rows=$(sql 'SELECT count(*) FROM lab.orders WHERE customer_id < 100')
echo "rows:                      $before_rows"
echo "rows for customer_id<100:  $victim_rows"
echo "relfilenode of lab.orders: $(sql "SELECT pg_relation_filenode('lab.orders')")"

echo
echo "== 2. the mistake =="
hr
seg_before=$(sql "SELECT pg_walfile_name(pg_current_wal_lsn())")
# pg_current_xact_id() assigns this transaction its xid and returns it. In a
# real incident nobody has this -- step 3 goes and finds it instead. It is
# recorded here only so the discovered value can be checked against it.
truth=$(sql "BEGIN;
             SELECT pg_current_xact_id();
             DELETE FROM lab.orders WHERE customer_id < 100;
             COMMIT;" | head -1)
bad_time=$(sql 'SELECT now()')
echo "deleted:                   $victim_rows rows"
echo "rows now:                  $(sql 'SELECT count(*) FROM lab.orders')"
echo "(ground truth xid:         $truth)"

echo
echo "== 3. traffic keeps flowing, because nobody has noticed yet =="
hr
for i in $(seq 1 10); do
  sql "INSERT INTO lab.orders (customer_id, status, amount)
       SELECT 500 + g % 100, 'paid', 4.00 FROM generate_series(1, 2000) g" >/dev/null
done
after_rows=$(sql 'SELECT count(*) FROM lab.orders')
good_after=$(( after_rows - before_rows + victim_rows ))
echo "rows written after the DELETE: $good_after   <- these are good writes"
echo "rows now:                      $after_rows"

# An open segment is not in the archive, and the mistake is in the open
# segment. Nothing can be recovered past the last archived segment, so this
# is the first move in the runbook, before anything else.
seg_after=$(sql "SELECT pg_walfile_name(pg_current_wal_lsn())")
sql 'SELECT pg_switch_wal()' >/dev/null
for _ in $(seq 1 200); do
  dexec "test -f /archive/$seg_after" 2>/dev/null && break
done
dexec "test -f /archive/$seg_after" || { echo "segment $seg_after never archived"; exit 1; }
echo "forced a WAL switch; $seg_after is now in the archive"

echo
echo "== 4. finding the transaction in the WAL =="
hr
node=$(sql "SELECT pg_relation_filenode('lab.orders')")
db=$(sql "SELECT oid FROM pg_database WHERE datname = 'db'")

# pg_waldump takes a START and an END segment, not a list of files. Handed a
# glob it silently reads the first two and reports on those -- which looks
# exactly like "no DELETEs found", and the empty target that produces is how a
# PITR quietly turns into a restore-to-end that replays the mistake.
echo "scanning $seg_before .. $seg_after for DELETEs against rel 1663/$db/$node"
found=$(dexec "pg_waldump -p /archive $seg_before $seg_after 2>/dev/null \
  | grep 'desc: DELETE' | grep 'rel 1663/$db/$node' \
  | sed -n 's/.*tx: *\([0-9]*\),.*/\1/p' | sort -un | tail -1")
[[ -n $found ]] || { echo "no DELETE found -- refusing to restore without a target"; exit 1; }
echo "transaction found:         $found"
echo "matches ground truth:      $([[ "$found" == "$truth" ]] && echo yes || echo "NO ($truth)")"

echo
echo "== 5. recover to just before it =="
hr
stop_restored
# recovery_target_inclusive=false means stop BEFORE this transaction commits.
# Left at its default of true, recovery replays the DELETE and the restore
# produces exactly the database you were trying to escape.
t0=$(ms)
restore_into /backups/pitr \
  "recovery_target_xid = '$found'" \
  "recovery_target_inclusive = false" \
  "recovery_target_action = 'pause'"
wait_paused || { echo "never paused"; startup_log | tail -20; exit 1; }
echo "paused at target after $(( $(ms) - t0 )) ms, still in recovery"
echo
printf '%-32s %12s\n' 'rows, paused at target'       "$(rsql 'SELECT count(*) FROM lab.orders')"
printf '%-32s %12s\n' '  of which customer_id<100'   "$(rsql 'SELECT count(*) FROM lab.orders WHERE customer_id < 100')"
echo
startup_log | grep -E 'starting point-in-time|recovery stopping|consistent recovery' | sed 's/^.*UTC \[[0-9]*\] //'

echo
echo "== 6. accept it =="
hr
rsql 'SELECT pg_wal_replay_resume()' >/dev/null
wait_out_of_recovery 120 || { echo "did not promote"; exit 1; }
restored_rows=$(rsql 'SELECT count(*) FROM lab.orders')
printf '%-32s %12s\n' 'before the DELETE'      "$before_rows"
printf '%-32s %12s\n' 'after the DELETE'       "$(( before_rows - victim_rows ))"
printf '%-32s %12s\n' 'live now'               "$after_rows"
printf '%-32s %12s\n' 'restored'               "$restored_rows"
echo
echo "victim rows recovered:     $(rsql 'SELECT count(*) FROM lab.orders WHERE customer_id < 100') / $victim_rows"
echo "good writes lost with it:  $good_after   <- everything after the target, mistake or not"
echo
echo "timeline: $(startup_log | grep -o 'selected new timeline ID: [0-9]*' | tail -1)"
