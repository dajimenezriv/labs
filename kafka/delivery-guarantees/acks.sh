#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly N=${N:-2000000}
readonly TRIALS=${TRIALS:-3}
readonly BIN=$(mktemp -d)/dg
readonly OUT=out

go build -o "$BIN" .
mkdir -p "$OUT"

compose() { docker compose "$@"; }

wait_healthy() {
  for _ in {1..60}; do
    if [[ $(compose ps --format '{{.Health}}' kafka1 kafka2 kafka3 | sort -u) == healthy ]]; then return; fi
    sleep 2
  done
  echo "brokers did not come back healthy" >&2
  exit 1
}

variant() {
  local label=$1 acks=$2 minisr=$3 topic=$4

  "$BIN" topic -topic "$topic" -rf 3 -min-isr "$minisr" >&2
  rm -f "$OUT/$topic".*

  "$BIN" produce -topic "$topic" -acks "$acks" -n "$N" -out "$OUT/$topic.produced" &
  local producer=$!

  # Long enough that the log is being written to and the followers are behind
  # by whatever they are behind by, short enough that the producer is nowhere
  # near done.
  sleep 2
  local leader
  leader=$("$BIN" leader -topic "$topic")
  echo "-- killing kafka$leader (leader of $topic-0)" >&2
  compose kill -s SIGKILL "kafka$leader" >/dev/null 2>&1

  wait $producer

  compose start "kafka$leader" >/dev/null 2>&1
  wait_healthy

  "$BIN" consume -topic "$topic" -group "$topic-g" -sink "$OUT/$topic.consumed" >/dev/null 2>&1
  "$BIN" verify -label "$label" -produced "$OUT/$topic.produced" -consumed "$OUT/$topic.consumed"
}

echo "== 1. the leader dies mid-produce ($TRIALS trials each) =="
{
  "$BIN" verify -header
  variant "acks=1" one 2 "seq-acks1"
  variant "acks=all" all 2 "seq-acksall"
} | column -t -s $'\t'

# Part 1 scores min.insync.replicas 1 and 2 identically, because with three
# live brokers the ISR is full and acks=all waits for all three either way.
# min.insync.replicas is not a setting about the healthy path. It is the number
# the broker checks BEFORE accepting an acks=all write, and it only ever
# changes the answer once the ISR has already shrunk.
#
# So shrink it: one broker down, ISR 3 -> 2, and three topics that disagree
# about whether that is still enough. Only one broker is killed because the
# controller quorum is these same three nodes -- killing two would leave no
# quorum, and the failure would be the cluster having no controller rather than
# the topic refusing a write.
echo
echo "== 2. what min.insync.replicas refuses, with one broker down =="

for isr in 1 2 3; do
  "$BIN" topic -topic "seq-isr$isr" -rf 3 -min-isr "$isr" >&2
done

victim=$("$BIN" leader -topic seq-isr2)
echo "-- stopping kafka$victim: 3 brokers -> 2" >&2
compose kill -s SIGKILL "kafka$victim" >/dev/null 2>&1
# Long enough for replica.lag.time.max.ms to expire and the controller to
# actually shrink the ISR. Until it does, the topic still believes it has three.
sleep 35

{
  printf 'topic\t| sent\t| acked\t| failed\t| first error\n'
  for isr in 1 2 3; do
    "$BIN" produce -topic "seq-isr$isr" -acks all -n 10000 -retries 3 \
      -out "$OUT/seq-isr$isr.produced" -row "rf=3, min.isr=$isr" 2>/dev/null
  done
} | column -t -s $'\t'

compose start "kafka$victim" >/dev/null 2>&1
wait_healthy
