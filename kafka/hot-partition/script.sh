#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

readonly BIN=$(mktemp -d)/hp
go build -o "$BIN" .

variants() {
  local suffix=$1
  shift
  "$BIN" run -report header
  "$BIN" run -name "hot-device$suffix" -key device -members 0s=6 -report row -label "key=t00/device" "$@"
  "$BIN" run -name "hot-salt$suffix" -key salt -members 0s=6 -report row -label "key=t00#salt" "$@"
  "$BIN" run -name "hot-route$suffix" -key route -members 0s=2 -report row -label "route t00 to own topic" "$@"
}

echo
echo "== 1. skewed: tenant t00 sends 80%, then scale 3 -> 6 =="
"$BIN" run -name hot-skew -whale 0.8 -members 0s=3,30s=6 -duration 60s

echo
echo "== 2. steady: 80% whale, 1000 records/s, 30s each =="
variants "" -duration 30s | column -t -s $'\t'

echo
echo "== 3. the whale spikes 4x between 30s and 60s, 120s each =="
variants "-spike" -duration 120s -spike-from 30s -spike-to 60s -spike 4 | column -t -s $'\t'
