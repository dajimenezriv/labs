#!/usr/bin/env bash
#
# A deploy that writes wrong results for ids 2001..3000 and errors nowhere.
# Found later, fixed, and the group rewound with the stock CLI to the moment
# the bug went out.

set -euo pipefail
cd "$(dirname "$0")"
source lib.sh

readonly N=6000 KEYS=30
readonly SINK="$OUT/replay.$RANDOM" GROUP="g-$RANDOM"

"$BIN" topics
"$BIN" consume -mode blocking -group "$GROUP" -sink "$SINK" -idle 5s -bad-from 2001 -bad-to 3000 &
consumer=$!
sleep 2
"$BIN" produce -n $N -rate 200 -keys $KEYS
wait $consumer

# "The bug went out at 14:05": the produce time of the first wrong record.
ms=$(awk -F, '$1 == 2001 { print $4; exit }' "$SINK")
when=$(date -u -d "@${ms:0:-3}.${ms: -3}" +%Y-%m-%dT%H:%M:%S.%3N)
echo "-- rewinding $GROUP to $when" >&2

# Without --execute this is a dry run that prints the plan and changes
# nothing. The group must have no live members, or the broker refuses.
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group "$GROUP" --topic readings \
  --reset-offsets --to-datetime "$when" --execute >&2

exec 3> >(table)
"$BIN" verify -view replay -header >&3
"$BIN" verify -view replay -label "before replay" -sink "$SINK" >&3

# The fixed deploy, same group: it resumes from the rewound offsets.
"$BIN" consume -mode blocking -group "$GROUP" -sink "$SINK" -idle 5s
"$BIN" verify -view replay -label "after replay" -sink "$SINK" >&3
exec 3>&-
wait
