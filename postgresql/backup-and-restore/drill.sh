#!/usr/bin/env bash
# Part 4: the drill. Restore the latest backup into a throwaway instance,
# check it, report, tear it down. Non-zero exit on any failure.
#
# This is the part that makes the other three worth having. A backup is a
# claim; this is the test of the claim, and it is the only thing that
# distinguishes a backup system from a directory that files accumulate in.

cd "$(dirname "$0")"
source ./lib.sh

readonly RTO_BUDGET_S=60    # how long the business says a restore may take
readonly RPO_BUDGET_S=300   # how much data loss the business says is allowed

fails=0
step=0
check() {
  local name=$1 ok=$2 detail=${3:-}
  step=$(( step + 1 ))
  if [[ $ok == 0 ]]; then
    printf '  [%d/6] %-42s PASS  %s\n' "$step" "$name" "$detail"
  else
    printf '  [%d/6] %-42s FAIL  %s\n' "$step" "$name" "$detail"
    fails=$(( fails + 1 ))
  fi
}

echo "restore drill $(date -u '+%Y-%m-%d %H:%M:%SZ')"
echo "------------------------------------------------------------"

require_seed
take_backup /backups/drill

# A backup that cannot be read is a backup that does not exist, and the
# manifest is the cheapest way to find that out. This catches the truncated
# upload and the half-written file, and it catches them without a restore.
verify_out=$(dexec "pg_verifybackup -n /backups/drill 2>&1") && rc=0 || rc=1
check "backup verifies against its manifest" "$rc" "$(echo "$verify_out" | tail -1)"

stop_restored
t0=$(ms)
restore_into /backups/drill
extract_ms=$(( $(ms) - t0 ))
rexec "test -f $RDATA/PG_VERSION" >/dev/null 2>&1 && rc=0 || rc=1
check "restores into a clean data directory" "$rc" "${extract_ms} ms"

t1=$(ms)
wait_out_of_recovery "$RTO_BUDGET_S" && rc=0 || rc=1
replay_ms=$(( $(ms) - t1 ))
check "reaches end of recovery and promotes" "$rc" "${replay_ms} ms"

rto_ms=$(( extract_ms + replay_ms ))
(( rto_ms / 1000 <= RTO_BUDGET_S )) && rc=0 || rc=1
check "RTO within ${RTO_BUDGET_S}s budget" "$rc" "$(awk -v m=$rto_ms 'BEGIN{printf "%.1f s", m/1000}')"

# "It started" is not "it is correct". amcheck walks every btree and confirms
# each index entry has the heap tuple it claims to, which is the class of
# damage a row count cannot see.
amcheck_out=$(rexec "pg_amcheck -U postgres -d db --heapallindexed --no-dependent-indexes 2>&1") && rc=0 || rc=1
check "structural check (pg_amcheck)" "$rc" "$(echo "$amcheck_out" | grep -c . | xargs -I{} echo '{} lines of complaint')"

# Freshness, measured against the clock rather than against the live database
# -- live is a moving target and will never match a restore exactly. What the
# business actually bought is a bound on how far behind the restore lands.
fresh=$(rsql "SELECT round(extract(epoch FROM now() - max(created_at))) FROM lab.orders" 2>/dev/null || echo 999999)
(( fresh <= RPO_BUDGET_S )) && rc=0 || rc=1
check "freshness within ${RPO_BUDGET_S}s budget" "$rc" "${fresh}s behind"

echo "------------------------------------------------------------"
printf '  %-20s %s\n' 'restored rows' "$(rsql 'SELECT rows FROM lab.fingerprint' 2>/dev/null || echo '-')"
printf '  %-20s %s\n' 'RTO' "$(awk -v m=$rto_ms 'BEGIN{printf "%.1f s", m/1000}')"
printf '  %-20s %s\n' 'RPO' "${fresh} s"
echo

stop_restored
rexec "rm -rf $RDATA /backups/drill"

if (( fails == 0 )); then
  echo "DRILL PASSED"
else
  echo "DRILL FAILED ($fails checks)"
fi
exit $(( fails > 0 ))
