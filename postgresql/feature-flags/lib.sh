#!/usr/bin/env bash
# Shared plumbing. Every script drives the same program against the same
# stack, so the pieces live here once.

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:5555/db

export PGPASSWORD=postgres

# -qtAX: no headers, no alignment, no psqlrc. Every caller parses the output.
d() { psql "$DSN" -qtAX -v ON_ERROR_STOP=1 "$@"; }

backends() { d -c "SELECT count(*) FROM pg_stat_activity
                   WHERE datname = 'db' AND backend_type = 'client backend'"; }

rule() { printf '\n== %s ==\n' "$*"; }
