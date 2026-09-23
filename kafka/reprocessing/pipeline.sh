#!/usr/bin/env bash
#
# Same traffic, same failures, two consumers: one that retries in place and
# one that hands failures to the retry topics. 6000 records at 200/s over 30
# keys; every 500th fails twice before it works.

set -euo pipefail
source lib.sh

readonly N=6000 RATE=200 KEYS=30
readonly FLAKY="-flaky-every 500 -flaky-times 2"

variant() {
  local label=$1 mode=$2
  shift 2
  local sink="$OUT/pipeline.$mode.$RANDOM"

  "$BIN" topics
  # Consumer first, so it is waiting when the first record lands and latency
  # measures the consumer rather than when it happened to start.
  "$BIN" consume -mode "$mode" -group "g-$RANDOM" -sink "$sink" "$@" &
  local consumer=$!
  sleep 2
  "$BIN" produce -n $N -rate $RATE -keys $KEYS
  wait $consumer
  "$BIN" verify -label "$label" -sink "$sink" -n $N -keys $KEYS >&3
}

exec 3> >(table)
"$BIN" verify -header >&3
variant "blocking, flaky"          blocking $FLAKY
variant "tiered, flaky"            tiered   $FLAKY
# The poison pill is id 3000, half way through. Retryable, so neither consumer
# knows it will never work: the blocking one tries forever, the tiered one
# walks the ladder. The deadline is what ends the blocking run.
variant "blocking, flaky + poison" blocking $FLAKY -poison 3000 -deadline 90s
variant "tiered, flaky + poison"   tiered   $FLAKY -poison 3000
exec 3>&-
wait
