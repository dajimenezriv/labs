#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:5555/db

# -qtAX: no headers, no alignment, no psqlrc. Every caller parses the output.
d() { psql "$DSN" -qtAX -v ON_ERROR_STOP=1 "$@"; }

rule() { printf '\n== %s ==\n' "$*"; }
