#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

mkdir -p out

psql $MONO -f seed.sql

teardown_replication
create_new_schema

trap stop_service EXIT
start_service

readonly ACKED=out/cutover-acked.txt
LOAD_T0=$(ms)
go run . load -duration 30s -acked "$ACKED" >"out/cutover-load.tsv" 2>&1 &
LOAD=$!
sleep 3

m "CREATE PUBLICATION payments_pub FOR TABLE lab.orders, lab.payments" >/dev/null
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null

while (( $(tables_syncing) > 0 )); do :; done
finish_new_schema

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

rule "2. the cutover"

do_cutover

printf '  quiesce in-flight      %5s ms\n' "$quiesce"
printf '  drain to caught up     %5s ms\n' "$drain"
printf '  drop subscription      %5s ms\n' "$dropsub"
printf '  advance sequence       %5s ms\n' "$seqfix"
printf '  reverse replication    %5s ms\n' "$reverse"
printf '  flip routing           %5s ms\n' "$flip"
printf '  ------------------------------\n'
printf '  writes held for        %5s ms\n' "$total"
printf '  WAL still unconfirmed  %5s bytes at the moment rows agreed\n' "$outstanding"

wait "$LOAD"
sleep 2   # let the last in-flight writes land before counting anything

rule "3. what the clients saw"

# The header, the seconds around the cutover, and the totals. The whole
# timeline is in out/cutover-load.tsv.
cut_s=$(( (t0 - LOAD_T0) / 1000 ))
sed -n "1p;$((cut_s - 1)),$((cut_s + 5))p" "out/cutover-load.tsv" | sed "s/^$cut_s\t/$cut_s*\t/"
tail -n 6 "out/cutover-load.tsv"

rule "4. what the new database ended up with"

sort -n "$ACKED" > out/cutover-acked.sorted
missing=$(psql "$PAY" -qtAX -v ON_ERROR_STOP=1 <<SQL
CREATE TEMP TABLE acked (order_id bigint);
\copy acked FROM 'out/cutover-acked.sorted'
SELECT count(*) FROM acked a
WHERE NOT EXISTS (SELECT 1 FROM lab.payments p WHERE p.order_id = a.order_id);
SQL
)
printf '  acknowledged writes    %s\n' "$(wc -l < "$ACKED")"
printf '  absent from new db     %s   <- acknowledged and lost\n' "$missing"
