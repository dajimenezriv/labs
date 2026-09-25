#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:5555/db
readonly BIN=$(mktemp -d)/loadgen

export PGPASSWORD=postgres

if ! psql $DSN -qtAX -c 'SELECT 1 FROM lab.accounts LIMIT 1' >/dev/null 2>&1; then
  echo "no lab.accounts: psql $DSN -f seed.sql (the stack must be up)" >&2
  exit 1
fi

go build -o "$BIN" .
run() { "$BIN" -dsn "$DSN" -workers 100 "$@"; }

{
  run -header
  for n in 0 1 2 4 6 8 12 16 50 100 150; do run -pool "$n" -duration 5s; done
} | column -t -s $'\t'
