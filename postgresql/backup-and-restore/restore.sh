#!/usr/bin/env bash
# Part 2: the host is gone. Restore from the base backup plus the WAL archive,
# and measure the two numbers nobody has until they have done this once:
# how long it took (RTO) and how much was lost (RPO).

cd "$(dirname "$0")"
source ./lib.sh
require_seed

readonly LOG=/tmp/restore-lab-writer.log
readonly SECONDS_OF_WRITES=20

hr() { printf '%s\n' "------------------------------------------------------------"; }

echo "== 1. base backup =="
hr
t0=$(ms)
take_backup /backups/base
echo "pg_basebackup: $(( $(ms) - t0 )) ms, $(rexec 'du -sh /backups/base | cut -f1')"
echo "archive at backup time: $(sql 'SELECT last_archived_wal FROM pg_stat_archiver')"

# Every line is written only after COMMIT returned, so this file is the list
# of transactions the application was told were durable. That promise is the
# thing the restore is measured against.
echo
echo "== 2. ${SECONDS_OF_WRITES}s of committed writes after the backup =="
hr
: > "$LOG"
(
  end=$(( $(ms) + SECONDS_OF_WRITES * 1000 ))
  while (( $(ms) < end )); do
    id=$(sql "WITH ins AS (
                INSERT INTO lab.orders (customer_id, status, amount)
                SELECT g % 1000, 'paid', 2.00 FROM generate_series(1, 2000) g
                RETURNING id)
              SELECT max(id) FROM ins" 2>/dev/null) || break
    [[ -n $id ]] && echo "$(ms) $id" >> "$LOG"
  done
) &
writer=$!
wait "$writer" 2>/dev/null || true

committed_rows=$(sql 'SELECT count(*) FROM lab.orders')
committed_max=$(awk 'END{print $2}' "$LOG")
committed_at=$(awk 'END{print $1}' "$LOG")
echo "commits acknowledged:      $(wc -l < "$LOG")"
echo "highest acknowledged id:   $committed_max"
echo "rows in the table:         $committed_rows"
echo "segments archived:         $(sql 'SELECT archived_count FROM pg_stat_archiver')"
echo "still in the open segment: $(sql "SELECT pg_walfile_name(pg_current_wal_lsn())")"

# SIGKILL, not a shutdown. A graceful stop is not the failure worth drilling;
# power loss is. Nothing gets flushed, nothing gets archived on the way out.
echo
echo "== 3. the primary dies =="
hr
docker compose kill postgres >/dev/null 2>&1
echo "SIGKILL sent at $(date -u '+%H:%M:%S')"
echo
echo "The restore container mounts /backups and /archive and nothing else --"
echo "it has no access to the primary's volume, so whatever comes back came"
echo "back from the backup."

echo
echo "== 4. restore =="
hr
stop_restored
t0=$(ms)
restore_into /backups/base
extract_ms=$(( $(ms) - t0 ))

t1=$(ms)
wait_out_of_recovery 300 || { echo "recovery did not finish"; startup_log | tail -30; exit 1; }
replay_ms=$(( $(ms) - t1 ))

printf '%-38s %8s ms\n' 'lay down data dir + reach consistency' "$extract_ms"
printf '%-38s %8s ms\n' 'replay archive to end + promote'       "$replay_ms"
printf '%-38s %8s ms\n' 'RTO (total)'                           "$(( extract_ms + replay_ms ))"

echo
echo "== 5. what came back =="
hr
restored_max=$(rsql 'SELECT max_id FROM lab.fingerprint')
restored_rows=$(rsql 'SELECT rows FROM lab.fingerprint')
lost_rows=$(( committed_rows - restored_rows ))

printf '%-26s %12s %12s\n' '' acknowledged restored
printf '%-26s %12s %12s\n' 'rows'   "$committed_rows" "$restored_rows"
printf '%-26s %12s %12s\n' 'max id' "$committed_max"  "$restored_max"
echo
echo "rows lost:                 $lost_rows"

# The acknowledged commit that did not survive tells you the RPO in the only
# unit that matters: how far back in time the restore threw you.
if (( lost_rows > 0 )); then
  cutoff=$(awk -v m="$restored_max" '$2 <= m {t=$1} END{print t}' "$LOG")
  echo "last surviving commit:     $(awk -v m="$restored_max" '$2 <= m {print $2} ' "$LOG" | tail -1)"
  echo "RPO (wall clock):          $(awk -v a="$committed_at" -v b="$cutoff" 'BEGIN{printf "%.1f s", (a-b)/1000}') of acknowledged commits"
else
  echo "nothing lost -- the tail happened to land on a segment boundary."
fi

echo
echo "== 6. the control: the primary's own crash recovery =="
hr
docker compose up -d postgres >/dev/null 2>&1
for _ in $(seq 1 60); do sql 'SELECT 1' >/dev/null 2>&1 && break; done
echo "rows after crash recovery: $(sql 'SELECT count(*) FROM lab.orders')"
echo
echo "The volume survived here, so local WAL replay got everything back. That"
echo "is a different guarantee from the one above, and it is the one you do"
echo "NOT have when the hardware is what failed."
