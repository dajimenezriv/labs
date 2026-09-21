#!/usr/bin/env bash
# Part 1: what a base backup actually is, what it costs, and what the archive
# is accumulating next to it.

cd "$(dirname "$0")"
source ./lib.sh
require_seed

hr() { printf '%s\n' "------------------------------------------------------------"; }

echo "== 1. what the defaults give you =="
hr
printf '%-24s %s\n' 'wal_level'      "$(sql 'SHOW wal_level')"
printf '%-24s %s\n' 'archive_mode'   "$(sql 'SHOW archive_mode')  (default: off)"
printf '%-24s %s\n' 'archive_timeout' "$(sql 'SHOW archive_timeout')  (0 = never force a switch)"
printf '%-24s %s\n' 'summarize_wal'  "$(sql 'SHOW summarize_wal')  (default: off, needed for -i incrementals)"

# A writer running for the whole backup. pg_basebackup takes no lock that
# blocks writes -- it reads files while the database keeps changing them, and
# WAL replay is what makes the inconsistent copy consistent later.
echo
echo "== 2. taking the backup, with writes in flight =="
hr
before=$(sql 'SELECT count(*) FROM lab.orders')
( for i in $(seq 1 60); do
    sql "INSERT INTO lab.orders (customer_id, status, amount)
         SELECT g % 1000, 'paid', 1.00 FROM generate_series(1, 2000) g" >/dev/null 2>&1 || true
  done ) &
writer=$!

t0=$(ms)
take_backup /backups/base
backup_ms=$(( $(ms) - t0 ))

kill "$writer" 2>/dev/null || true
wait "$writer" 2>/dev/null || true
after=$(sql 'SELECT count(*) FROM lab.orders')

echo "rows before backup:        $before"
echo "rows after backup:         $after"
echo "committed during backup:   $(( after - before ))   <- none of them blocked"
echo "backup took:               ${backup_ms} ms"

echo
echo "== 3. what is in it =="
hr
rexec "ls -l /backups/base" | awk '/^-/ {printf "%-20s %10.1f MB\n", $9, $5/1048576}'
db_bytes=$(sql "SELECT pg_database_size('db')")
bk_bytes=$(rexec "du -sb /backups/base | cut -f1")
printf '%-20s %10.1f MB\n' '(live database)' "$(echo "$db_bytes" | awk '{print $1/1048576}')"
echo
echo "compression:               $(awk -v d="$db_bytes" -v b="$bk_bytes" 'BEGIN{printf "%.1fx", d/b}')"
echo "start LSN:                 $(rexec "grep -m1 START_WAL_LOCATION $RDATA/backup_label 2>/dev/null" || rexec "tar xzOf /backups/base/base.tar.gz backup_label 2>/dev/null | grep -m1 'START WAL LOCATION'")"

echo
echo "== 4. the archive, which is the other half of the backup =="
hr
IFS='|' read -r archived last failed <<< "$(sql "SELECT archived_count || '|' || last_archived_wal || '|' || failed_count FROM pg_stat_archiver")"
printf '%-24s %s\n' 'segments archived'  "$archived"
printf '%-24s %s\n' 'last archived'      "$last"
printf '%-24s %s\n' 'failed'             "$failed"
printf '%-24s %s\n' 'archive on disk'    "$(dexec 'du -sh /archive | cut -f1')"
printf '%-24s %s\n' 'current segment'    "$(sql "SELECT pg_walfile_name(pg_current_wal_lsn())")"
echo
echo "The current segment is NOT in the archive and will not be until it fills"
echo "16 MB or something forces a switch. Everything committed into it is"
echo "outside the backup -- that gap is measured in restore.sh."
