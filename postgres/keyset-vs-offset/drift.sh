#!/usr/bin/env bash
#
# The half a benchmark cannot show: offset pages are wrong when the table
# changes underneath them.
#
# OFFSET n means "skip n rows of the result as it is now". Insert a row that
# sorts above the current page and every row below it shifts down one, so the
# row that was last on page 1 is first on page 2 and the client sees it twice.
# Delete one and the shift goes the other way: a row is never shown at all.
#
# A keyset cursor names a row rather than a position, so rows arriving above it
# do not move it. Same insert, no duplicate.
set -euo pipefail
cd "$(dirname "$0")"

readonly PSQL=(psql -h localhost -p 5555 -U postgres -d db -qtAX -v ON_ERROR_STOP=1)
readonly PAGE=20

export PGPASSWORD=postgres

sql() { "${PSQL[@]}" -c "$1"; }

cleanup() { sql "DELETE FROM lab.events WHERE payload = 'INTERLEAVED'" >/dev/null; }
trap cleanup EXIT

cleanup

# Sorts above everything: the seed stops in 2025, this lands now.
interleave() {
  sql "INSERT INTO lab.events (created_at, device_id, payload)
       VALUES (now(), 0, 'INTERLEAVED')" >/dev/null
}

echo "offset"
page1=$(sql "SELECT id FROM lab.events ORDER BY created_at DESC, id DESC LIMIT $PAGE OFFSET 0")
interleave
page2=$(sql "SELECT id FROM lab.events ORDER BY created_at DESC, id DESC LIMIT $PAGE OFFSET $PAGE")
dupes=$(comm -12 <(sort <<< "$page1") <(sort <<< "$page2") | tr '\n' ' ')
echo "  page 1 last:  $(tail -1 <<< "$page1")"
echo "  page 2 first: $(head -1 <<< "$page2")"
echo "  seen twice:   ${dupes:-none}"

cleanup

echo "keyset"
page1=$(sql "SELECT id FROM lab.events ORDER BY created_at DESC, id DESC LIMIT $PAGE")
IFS='|' read -r ts id < <(sql "SELECT created_at || '|' || id FROM lab.events
                               ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET $((PAGE - 1))")
interleave
page2=$(sql "SELECT id FROM lab.events
             WHERE (created_at, id) < ('$ts'::timestamptz, $id::bigint)
             ORDER BY created_at DESC, id DESC LIMIT $PAGE")
dupes=$(comm -12 <(sort <<< "$page1") <(sort <<< "$page2") | tr '\n' ' ')
echo "  page 1 last:  $(tail -1 <<< "$page1")"
echo "  page 2 first: $(head -1 <<< "$page2")"
echo "  seen twice:   ${dupes:-none}"
