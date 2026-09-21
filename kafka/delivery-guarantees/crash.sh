#!/usr/bin/env bash
#
# One topic, one crash point, three commit placements. The consumer always dies
# at the same instant -- immediately after making record 5300's effect durable
# -- so the only thing that varies is where the committed offset happens to be
# when it does.

set -euo pipefail
cd "$(dirname "$0")"

readonly N=${N:-20000}
readonly CRASH_AT=${CRASH_AT:-5300}
readonly BIN=$(mktemp -d)/dg
readonly OUT=out
readonly TOPIC=seq-crash

go build -o "$BIN" .
mkdir -p "$OUT"

"$BIN" topic -topic "$TOPIC" -rf 3 -min-isr 2 >&2
"$BIN" produce -topic "$TOPIC" -acks all -n "$N" -out "$OUT/$TOPIC.produced"

variant() {
  local label=$1 mode=$2
  local group="$TOPIC-$mode-$RANDOM"
  local sink="$OUT/$TOPIC.$mode.consumed"
  rm -f "$sink"

  # Run 1: dies at CRASH_AT with no clean exit and no leave-group.
  # 200us per record is what makes the poll loop take long enough for an
  # autocommit interval to land inside it, which is the only reason the three
  # modes can disagree at all.
  "$BIN" consume -topic "$TOPIC" -group "$group" -sink "$sink" \
    -commit "$mode" -per-record 200us -crash-after "$CRASH_AT" >/dev/null 2>&1 || true

  # A member that was killed does not leave the group, so the coordinator
  # keeps its assignment reserved until the session expires. Until then a new
  # member joins a group that is still waiting for a process that no longer
  # exists, and gets nothing. This wait is not lab scaffolding -- it is the
  # recovery time of every crashed consumer, and SessionTimeout is what sets it.
  sleep 8

  # Run 2: same group, new member, resumes from whatever was committed.
  "$BIN" consume -topic "$TOPIC" -group "$group" -sink "$sink" \
    -commit "$mode" -idle 8s >/dev/null 2>&1

  "$BIN" verify -label "$label" -produced "$OUT/$TOPIC.produced" -consumed "$sink"
}

{
  "$BIN" verify -header
  variant "greedy autocommit (java default)" greedy
  variant "franz-go default autocommit"      default
  variant "commit after processing"          manual
} | column -t -s $'\t'
