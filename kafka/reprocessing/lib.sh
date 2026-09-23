# Shared by every script in this lab.

readonly BIN=$(mktemp -d)/reprocessing
readonly OUT=out
go build -o "$BIN" .
mkdir -p "$OUT"

table() { column -t -s $'\t'; }
