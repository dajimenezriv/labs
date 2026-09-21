#!/usr/bin/env bash
# Shared plumbing. Every script drives the same binary against the same
# stack, so the pieces live here once.

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:5555/db
readonly BIN=./feature-flags

export PGPASSWORD=postgres

# -qtAX: no headers, no alignment, no psqlrc. Every caller parses the output.
d() { psql "$DSN" -qtAX -v ON_ERROR_STOP=1 "$@"; }

build() {
  if ! d -c 'SELECT 1 FROM lab.flags LIMIT 1' >/dev/null 2>&1; then
    echo "no lab.flags: psql $DSN -f seed.sql (the stack must be up)" >&2
    exit 1
  fi
  go build -o "$BIN" .
}

backends() { d -c "SELECT count(*) FROM pg_stat_activity
                   WHERE datname = 'db' AND backend_type = 'client backend'"; }

rule() { printf '\n== %s ==\n' "$*"; }
