#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql -h localhost -p 5555 -U postgres -d db -qtAX -v ON_ERROR_STOP=1)
readonly TIMEOUT=100
readonly CHURN=20000

export PGPASSWORD=postgres
export PGOPTIONS='-c client_min_messages=warning'

sql() { "${PSQL[@]}" -c "$1"; }

if ! sql 'SELECT 1 FROM lab.readings LIMIT 1' >/dev/null 2>&1; then
  echo "no lab.readings: psql ... -f seed.sql (the stack must be up)" >&2
  exit 1
fi

echo "== the knobs, as shipped =="
sql "SELECT '  ' || rpad(name, 38) || setting || coalesce(' ' || unit, '')
     FROM pg_settings
     WHERE name IN ('autovacuum_vacuum_threshold', 'autovacuum_vacuum_scale_factor',
                    'autovacuum_vacuum_max_threshold', 'autovacuum_naptime',
                    'autovacuum_vacuum_cost_delay', 'autovacuum_max_workers')
     ORDER BY name"

# The same arithmetic Postgres does, against the planner's row estimate rather
# than a count -- which is also what autovacuum uses, so a stale reltuples
# moves the trigger point.
echo
echo "== what that means for this table =="
sql "SELECT '  reltuples estimate: ' || reltuples::bigint || E'\n' ||
            '  vacuum threshold:   ' ||
            least(current_setting('autovacuum_vacuum_threshold')::bigint +
                  current_setting('autovacuum_vacuum_scale_factor')::float8 * reltuples,
                  current_setting('autovacuum_vacuum_max_threshold')::bigint)::bigint ||
            ' dead tuples'
     FROM pg_class WHERE oid = 'lab.readings'::regclass"

# Dirties a FIXED number of rows -- the same modest churn under both settings --
# and waits to see whether the launcher ever comes for it. Sizing the churn to
# each threshold instead would guarantee both fire and measure nothing but
# naptime.
measure() { # label, scale factor or 'default', rows to dirty
  local label=$1 sf=$2 churn=$3 threshold before t0 waited fired

  if [[ $sf == default ]]; then
    sql 'ALTER TABLE lab.readings RESET (autovacuum_vacuum_scale_factor)' >/dev/null
  else
    sql "ALTER TABLE lab.readings SET (autovacuum_vacuum_scale_factor = $sf)" >/dev/null
  fi

  sql 'VACUUM lab.readings' >/dev/null   # start each measurement from zero dead
  threshold=$(sql "SELECT least(50 + coalesce((SELECT option_value::float8
                                               FROM pg_options_to_table(reloptions)
                                               WHERE option_name = 'autovacuum_vacuum_scale_factor'),
                                              current_setting('autovacuum_vacuum_scale_factor')::float8)
                                     * reltuples,
                                current_setting('autovacuum_vacuum_max_threshold')::bigint)::bigint
                   FROM pg_class WHERE oid = 'lab.readings'::regclass")

  before=$(sql "SELECT autovacuum_count FROM pg_stat_user_tables WHERE relname = 'readings'")
  sql "UPDATE lab.readings SET value = value + 1 WHERE id <= $churn" >/dev/null

  t0=$(date +%s)
  waited="never (${TIMEOUT}s)"
  fired=no
  local now
  while (( $(date +%s) - t0 < TIMEOUT )); do
    sleep 2
    sql 'SELECT pg_stat_force_next_flush()' >/dev/null
    # Default to $before if psql hands back nothing, so a transient blip is a
    # retry rather than an arithmetic error inside [[ ]].
    now=$(sql "SELECT autovacuum_count FROM pg_stat_user_tables WHERE relname = 'readings'")
    if (( ${now:-$before} > before )); then
      waited="$(( $(date +%s) - t0 ))s"
      fired=yes
      break
    fi
  done

  printf '%-22s  %8s  %11s  %9s  %7s  %14s\n' \
    "$label" "$sf" "$threshold" "$churn" "$fired" "$waited"
}

echo
echo "== the same churn under two settings =="
echo "$CHURN rows dirtied each time, ${TIMEOUT}s of patience"
echo
printf '%-22s  %8s  %11s  %9s  %7s  %14s\n' \
  'setting' 'scale' threshold 'rows' 'auto' 'waited'
printf '%-22s  %8s  %11s  %9s  %7s  %14s\n' \
  '' factor '' dirtied vacuum ''
printf '%-22s  %8s  %11s  %9s  %7s  %14s\n' \
  ---------------------- -------- ----------- --------- ------- --------------

measure 'stock'              default "$CHURN"
measure 'per-table override' 0.01    "$CHURN"

sql 'ALTER TABLE lab.readings RESET (autovacuum_vacuum_scale_factor)' >/dev/null
echo
echo "table reset to stock settings"
