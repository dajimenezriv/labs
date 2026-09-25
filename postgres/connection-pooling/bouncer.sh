#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:6432/db
readonly BIN=$(mktemp -d)/loadgen

export PGPASSWORD=postgres

d() { psql -h localhost -p 5555 -U postgres -d db -qtAX "$@"; }  # straight to postgres
b() { psql -h localhost -p 6432 -U postgres -d db -qtAX "$@"; }  # through pgbouncer

if ! b -c 'SELECT 1 FROM lab.accounts LIMIT 1' >/dev/null 2>&1; then
  echo "pgbouncer is not answering on 6432, or lab.accounts is missing" >&2
  exit 1
fi

go build -o "$BIN" .
run() { "$BIN" -dsn "$DSN" -workers 100 -pool 100 -duration 5s "$@"; }

# Client backends only: pg_stat_activity also lists the checkpointer, the
# walwriter and friends, which do not come out of max_connections.
backends() { d -c "select count(*) from pg_stat_activity
                   where datname = 'db' and backend_type = 'client backend'"; }

echo "== 1. what 100 application connections cost =="
echo "  idle: $(backends) backends"
"$BIN" -workers 100 -pool 16 -duration 8s >/dev/null &
sleep 4
echo "  direct, pgxpool MaxConns=16:     $(backends) backends"
wait
run -duration 8s >/dev/null &
sleep 4
echo "  pgbouncer, pgxpool MaxConns=100: $(backends) backends"
psql -h localhost -p 6432 -U postgres -d pgbouncer -qtAX -c 'SHOW POOLS' |
  awk -F'|' '$1 == "db" { print "    pgbouncer holds cl_waiting=" $4 " clients over sv_idle=" $7 " server connections" }'
wait

echo
echo "== 2. pgx query exec modes through transaction pooling, 5s each =="
# cache_statement  pgx's default. PARSE under a generated name, BIND to that
#                  name later. pgbouncer >= 1.21 tracks those names itself and
#                  replays the PARSE, so this is correct now.
# cache_describe   caches the description client side, executes unnamed.
# describe_exec    describes, then executes, in two separate exchanges.
# exec             unnamed prepared statement, one round trip.
# simple           no parameters on the wire; pgx interpolates.
{
  run -header
  for m in cache_statement cache_describe describe_exec exec simple; do
    run -exec "$m"
  done
} | column -t -s $'\t'

echo
echo "== 3. the session state no setting fixes =="
# A pooled server connection is handed to the next transaction in whatever
# state the last one left it. Prepared statements got solved; these did not.
docker compose restart pgbouncer >/dev/null 2>&1
sleep 3
locks() { d -c "select coalesce(string_agg(objid::text, ','), 'none') from pg_locks where locktype = 'advisory'"; }

echo "  advisory locks before anything:                    $(locks)"
d -c "select pg_advisory_lock(43)" >/dev/null
echo "  after a direct client took 43 and disconnected:    $(locks)"
b -c "select pg_advisory_lock(42)" >/dev/null
echo "  after a pgbouncer client took 42 and disconnected: $(locks)"

b -c "set work_mem = '64MB'" >/dev/null
echo "  an unrelated later client through pgbouncer sees work_mem = $(b -c 'show work_mem')"
echo "  a client on its own backend sees work_mem                 = $(d -c 'show work_mem')"

docker compose restart pgbouncer >/dev/null 2>&1
