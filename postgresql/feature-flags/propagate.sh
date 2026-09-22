#!/usr/bin/env bash
# How long a flag change takes to reach every instance, and what each way of
# finding out costs while nothing is changing.

source "$(dirname "$0")/lib.sh"
build

readonly N=20

rule "1. propagation to $N instances, 30 changes 1s apart"
# The poll intervals are the ones people actually pick. resync is listen plus
# a watchdog; on this path it should be indistinguishable from listen, and
# that is the result being established -- §2 is where they diverge.
{
  "$BIN" -header
  for i in 15s 5s 1s; do
    "$BIN" -mode poll -interval "$i" -instances $N -flips 30 -gap 1s
  done
  "$BIN" -mode listen -instances $N -flips 30 -gap 1s
  "$BIN" -mode resync -watchdog 10s -instances $N -flips 30 -gap 1s
} | column -t -s $'\t'

rule "2. cost at rest: $N instances, 30s, no flag changes"
# Nothing to propagate. Everything counted here is what the mechanism spends
# to learn that nothing happened, which is the state a flag table is in
# essentially all of the time.
{
  printf 'mode\t| every\t| queries\t| backends\n'
  printf 'baseline (stack idle)\t| -\t| -\t| %s\n' "$(backends)"
  idle() {
    local mode=$1 every=$2 label=$3 out; out=$(mktemp)
    "$BIN" -mode "$mode" $every -instances $N -flips 0 -settle 30s >"$out" &
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
  idle resync "-watchdog 10s"  10s
} | column -t -s $'\t'
