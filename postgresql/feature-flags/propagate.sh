#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

psql $DSN -f seed.sql

rule "1. propagation to 20 instances, 30 changes 1s apart"
{
  go run . -header
  go run . -mode poll -interval 5s
  go run . -mode listen
} | column -t -s $'\t'

rule "2. cost at rest: 20 instances, 30s, no flag changes"
# Nothing to propagate. Everything counted here is what the mechanism spends
# to learn that nothing happened, which is the state a flag table is in
# essentially all of the time.
{
  printf 'mode\t| every\t| queries\t| backends\n'
  printf 'baseline (stack idle)\t| -\t| -\t| %s\n' "$(backends)"
  idle() {
    local mode=$1 every=$2 label=$3 out; out=$(mktemp)
    go run . -mode "$mode" $every -idle -settle 30s >"$out" &
    local pid=$!
    sleep 15
    local bk; bk=$(backends)
    wait $pid
    printf '%s\t| %s\t| %s\t| %s\n' "$mode" "$label" \
      "$(cut -d'|' -f7 "$out" | tr -d ' \t')" "$bk"
    rm -f "$out"
  }
  idle poll   "-interval 1s"   1s
  idle poll   "-interval 5s"   5s
  idle listen ""               -
} | column -t -s $'\t'
