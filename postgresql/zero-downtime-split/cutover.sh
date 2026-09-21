#!/usr/bin/env bash
#
#   ./cutover.sh            the runbook: freeze, drain, flip, thaw
#   ./cutover.sh naive-seq  the same flip without advancing the sequence
#   ./cutover.sh naive-lag  the same flip without freezing or draining
#
# One rig, three cutovers, three failure signatures. The service stays up and
# under load throughout all of them; what changes is only the order of five
# statements, and that order is the entire difference between a 400 ms blip
# and an outage.
source "$(dirname "$0")/lib.sh"

readonly MODE=${1:-safe}
case "$MODE" in safe|naive-seq|naive-lag) ;; *) echo "usage: $0 [safe|naive-seq|naive-lag]" >&2; exit 2 ;; esac

mkdir -p out
require_seed
teardown_replication
reset_monolith
create_new_schema

trap stop_service EXIT
start_service

readonly ACKED=out/$MODE-acked.txt
first=$(( $(m "SELECT coalesce(max(order_id), 200000) FROM lab.payments") + 1 ))
LOAD_T0=$(ms)
"$BIN" load -duration 70s -acked "$ACKED" >"out/$MODE-load.tsv" 2>&1 &
LOAD=$!
sleep 3

# Backfill. Covered by backfill.sh; here it is just the state the cutover
# starts from.
m "CREATE PUBLICATION payments_pub FOR TABLE lab.payments" >/dev/null
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null
while [[ "$(sub_state)" != "r" ]]; do :; done

rule "1. shadow reads: what cutting reads over today would have returned"

# Reads keep being served by the monolith. The new database is queried
# alongside, purely to be compared. This is the only way to find out whether
# the new database is ready to serve without finding out in production.
ctl 'reads=shadow&reset=1'
sleep 10
mismatch=$(stat_of mismatch)
ctl 'reads=monolith'

printf '  writes acknowledged    %s\n' "$(stat_of ok)"
printf '  shadow mismatches      %s\n' "$mismatch"
printf '  replay lag             %s\n' "$(lag_time)"

rule "2. the cutover ($MODE)"

do_cutover "$MODE"

printf '  quiesce in-flight      %5s ms\n' "$quiesce"
printf '  drain to caught up     %5s ms\n' "$drain"
printf '  drop subscription      %5s ms\n' "$dropsub"
printf '  advance sequence       %5s ms%s\n' "$seqfix" "$([[ $MODE == naive-seq ]] && echo '   <- SKIPPED')"
printf '  reverse replication    %5s ms\n' "$reverse"
printf '  flip routing           %5s ms\n' "$flip"
printf '  ------------------------------\n'
printf '  writes held for        %5s ms%s\n' "$total" "$([[ $MODE == naive-lag ]] && echo '   <- nothing was held')"
printf '  WAL still unconfirmed  %5s bytes at the moment rows agreed\n' "$outstanding"

wait "$LOAD"
sleep 2   # let the last in-flight writes land before counting anything

rule "3. what the clients saw"

# The header, the seconds around the cutover, and the totals. The whole
# timeline is in out/$MODE-load.tsv.
cut_s=$(( (t0 - LOAD_T0) / 1000 ))
sed -n "1p;$((cut_s - 1)),$((cut_s + 5))p" "out/$MODE-load.tsv" | sed "s/^$cut_s\t/$cut_s*\t/"
tail -n 6 "out/$MODE-load.tsv"

rule "4. what the new database ended up with"

sort -n "$ACKED" > out/$MODE-acked.sorted
missing=$(psql "$PAY" -qtAX -v ON_ERROR_STOP=1 <<SQL
CREATE TEMP TABLE acked (order_id bigint);
\copy acked FROM 'out/$MODE-acked.sorted'
SELECT count(*) FROM acked a
WHERE NOT EXISTS (SELECT 1 FROM lab.payments p WHERE p.order_id = a.order_id);
SQL
)
printf '  acknowledged writes    %s\n' "$(wc -l < "$ACKED")"
printf '  absent from new db     %s   <- acknowledged and lost\n' "$missing"
printf '  orders paid but open   %s   <- the second commit that did not happen\n' \
  "$(m "SELECT count(*) FROM lab.orders o WHERE o.status = 'pending'
        AND EXISTS (SELECT 1 FROM lab.payments p WHERE p.order_id = o.id)")"
