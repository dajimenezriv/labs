#!/usr/bin/env bash
# Shared plumbing. Three of the four scripts perform the same restore with
# different recovery targets, so the procedure lives here once.

set -euo pipefail

readonly LIVE=postgres://postgres:postgres@localhost:5556/db
readonly BACK=postgres://postgres:postgres@localhost:5557/db
readonly RDATA=/var/lib/postgresql/restore   # PGDATA inside the restore container

# -qtAX: no headers, no alignment, no psqlrc. Every caller parses the output.
sql()  { psql "$LIVE" -qtAX -v ON_ERROR_STOP=1 -c "$1"; }
rsql() { psql "$BACK" -qtAX -v ON_ERROR_STOP=1 -c "$1"; }

ms() { date +%s%3N; }

# Both as uid 70. pg_basebackup writes its output 0600, so a backup taken as
# root inside the container is one the restore host cannot read -- a backup
# that fails only at restore time, which is the whole theme of this lab.
dexec()  { docker compose exec -T -u postgres postgres sh -c "$1"; }
rexec()  { docker compose exec -T -u postgres restore  sh -c "$1"; }

require_seed() {
  sql 'SELECT 1 FROM lab.orders LIMIT 1' >/dev/null 2>&1 && return 0
  echo "no lab.orders: psql $LIVE -f seed.sql (the stack must be up)" >&2
  exit 1
}

# A base backup is a physical copy of the data directory plus enough WAL to
# make that copy self-consistent. It is not a snapshot of a moment: files are
# copied over a span of time while the database keeps writing, and it is WAL
# replay from the backup start LSN that reconciles them. That is why a base
# backup is useless without its WAL, and why -X matters.
#
#   -c fast  checkpoint immediately instead of spreading it out; otherwise the
#            backup waits for the next scheduled checkpoint.
#   -Xs      stream the WAL generated *during* the backup alongside it, so the
#            backup can reach consistency on its own.
#   -Ft -z   tar + gzip, which is what the size numbers in part 1 compare.
take_backup() {
  local dest=$1
  rexec "rm -rf $dest && mkdir -p $dest"
  dexec "pg_basebackup -U postgres -D $dest -Ft -z -Xs -c fast"
}

# Lay the backup down as a fresh data directory and start a server on it in
# recovery. Extra recovery settings are passed as additional arguments and
# appended verbatim to postgresql.auto.conf.
#
# Everything read here comes from /backups and /archive. The restore container
# has no access to the primary's volume at all, which is the only way a
# restore drill proves anything.
restore_into() {
  local backup=${1:?backup}
  shift
  local extra
  extra=$(printf '%s\n' "$@")

  rexec "
    set -e
    rm -rf $RDATA
    mkdir -p $RDATA/pg_wal
    chmod 700 $RDATA
    tar xzf $backup/base.tar.gz   -C $RDATA
    tar xzf $backup/pg_wal.tar.gz -C $RDATA/pg_wal
  "

  # restore_command is how recovery pulls segments it does not have. It is a
  # shell command, and Postgres trusts its exit code completely: a command
  # that exits 0 without producing a file ends recovery early and quietly.
  rexec "cat >> $RDATA/postgresql.auto.conf <<'EOF'
restore_command = 'cp /archive/%f %p'
$extra
EOF"

  # The file that turns a data directory into a recovery. Without it the
  # server starts as an ordinary primary on a torn copy and refuses, or worse,
  # comes up on data that never reached consistency.
  rexec "touch $RDATA/recovery.signal"
  rexec "rm -f $RDATA/startup.log; pg_ctl -D $RDATA -l $RDATA/startup.log -o '-p 5432' -w -t 5 start" >/dev/null 2>&1 || true
}

# hot_standby is on by default, so the server answers read-only queries while
# it is still replaying. "It accepted a connection" is not "the restore is
# done" -- pg_is_in_recovery() going false is.
wait_out_of_recovery() {
  local timeout=${1:-120} t0
  t0=$(ms)
  while :; do
    if [[ "$(rsql 'SELECT pg_is_in_recovery()' 2>/dev/null || echo t)" == "f" ]]; then
      return 0
    fi
    (( ($(ms) - t0) / 1000 >= timeout )) && return 1
  done
}

stop_restored() {
  rexec "pg_ctl -D $RDATA -m immediate -w stop" >/dev/null 2>&1 || true
}

startup_log() { rexec "cat $RDATA/startup.log" 2>/dev/null || true; }
