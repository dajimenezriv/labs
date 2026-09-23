#!/usr/bin/env bash
#
# Replay is bounded by retention. retention.ms is scaled down from 7 days to
# 20s, and the broker's sweep interval with it (compose.yaml); segment.ms is
# left at its 7-day default.

set -euo pipefail
cd "$(dirname "$0")"
source lib.sh

readonly SINK="$OUT/retention.$RANDOM" GROUP="g-$RANDOM"

"$BIN" topics -retention-ms 20000
"$BIN" consume -mode blocking -group "$GROUP" -sink "$SINK" -idle 5s &
consumer=$!
sleep 2
start=$(date +%s)
"$BIN" produce -n 1000 -rate 1000
wait $consumer

{
  printf 'seconds since produce\t| log start offset\t| log end offset\n'
  for _ in {1..8}; do
    read -r first next < <("$BIN" offsets)
    printf '%d\t| %d\t| %d\n' $(($(date +%s) - start)) "$first" "$next"
    sleep 5
  done
} | table

when=$(date -u -d "@$start" +%Y-%m-%dT%H:%M:%S.000)
echo "-- rewinding $GROUP to $when, before the first record" >&2
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group "$GROUP" --topic readings \
  --reset-offsets --to-datetime "$when" --execute >&2

# Zero records never start the idle clock, so the deadline ends it.
"$BIN" consume -mode blocking -group "$GROUP" -sink "$SINK.replay" -idle 5s -deadline 10s
echo "replayed: $(wc -l < "$SINK.replay") records"
