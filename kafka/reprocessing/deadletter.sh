#!/usr/bin/env bash
#
# A deploy with a bug dead-letters 1% of records; the fix ships; the DLQ is
# merged back through retry.1 by the same consumer group -- twice, because
# merge does not consume.

set -euo pipefail
cd "$(dirname "$0")"
source lib.sh

readonly N=6000 KEYS=30
readonly SINK="$OUT/deadletter.$RANDOM" GROUP="g-$RANDOM"

"$BIN" topics
# The buggy deploy: every 100th record carries a field it cannot parse.
"$BIN" consume -mode tiered -group "$GROUP" -sink "$SINK" -idle 5s -bug-every 100 &
consumer=$!
sleep 2
"$BIN" produce -n $N -rate 1000 -keys $KEYS
wait $consumer

"$BIN" dlq list | sed -n '1p;$p' >&2

row() { "$BIN" verify -view dlq -label "$1" -sink "$SINK" -n $N -keys $KEYS >&3; }
exec 3> >(table)
"$BIN" verify -view dlq -header >&3
row "buggy deploy"

# The fixed deploy: same group, so the live topic resumes where it was and
# only what is merged gets handled.
"$BIN" consume -mode tiered -group "$GROUP" -sink "$SINK" -idle 10s &
consumer=$!
sleep 3
"$BIN" dlq merge >&2
sleep 5
row "fixed deploy + merge"
"$BIN" dlq merge >&2
wait $consumer
row "second merge, no purge"
"$BIN" dlq purge >&2
"$BIN" dlq list | tail -1 >&2
exec 3>&-
wait
