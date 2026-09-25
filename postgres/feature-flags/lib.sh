#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly DSN=postgres://postgres:postgres@localhost:5555/db

rule() { printf '\n== %s ==\n' "$*"; }
