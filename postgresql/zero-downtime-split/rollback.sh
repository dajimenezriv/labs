#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

mkdir -p out

psql $MONO -f seed.sql

teardown_replication
create_new_schema

trap stop_service EXIT
start_service

LOAD_T0=$(ms)
"$BIN" load -duration 70s -acked out/rollback-acked.txt >out/rollback-load.tsv 2>&1 &
LOAD=$!
sleep 3

m "CREATE PUBLICATION payments_pub FOR TABLE lab.orders, lab.payments" >/dev/null
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null

while (( $(tables_syncing) > 0 )); do :; done
finish_new_schema

rule "1. out (the same cutover, quietly)"
do_cutover
printf '  writes held for        %5s ms\n' "$total"
printf '  now serving from       the new database\n'

sleep 20
printf '  written there since    %s rows\n' \
  "$(p "SELECT count(*) FROM lab.orders WHERE id > 200000")"

rule "2. what the monolith looks like from behind"

# Rows have been arriving in the monolith through rollback_sub for the last
# 20 seconds, carrying ids the new database generated. An arriving row does
# not touch the sequence that would have produced it, so the monolith's
# sequence is now stranded far behind its own table. Nothing is wrong yet --
# the monolith is not inserting. It becomes wrong the instant it is asked to
# again, which is what rolling back means.
mono_max=$(m "SELECT max(id) FROM lab.payments")
mono_seq=$(m "SELECT last_value FROM lab.payments_id_seq")
printf '  max(id) in its table   %s\n' "$mono_max"
printf '  its sequence           %s\n' "$mono_seq"
printf '  inserts that would     %s   <- every one a duplicate key\n' "$((mono_max - mono_seq))"

rule "3. back"

t0=$(ms)
ctl 'freeze=on'

# Drain the reverse stream this time. Same rule, same direction of
# reasoning: the target has to stop moving before "caught up" is a fact.
while (( $(rows_mono) < $(rows_pay) )); do :; done
drain=$(( $(ms) - t0 ))

m "DROP SUBSCRIPTION rollback_sub" >/dev/null
p "DROP PUBLICATION rollback_pub" >/dev/null

# The step §2 was about.
m "SELECT setval('lab.orders_id_seq',   (SELECT max(id) + 1000 FROM lab.orders));
   SELECT setval('lab.payments_id_seq', (SELECT max(id) + 1000 FROM lab.payments))" >/dev/null

# Forward replication again, so the monolith is once more the source and the
# next attempt does not start from a backfill.
m "CREATE PUBLICATION payments_pub FOR TABLE lab.orders, lab.payments" >/dev/null 2>&1 || true
p "DROP SCHEMA IF EXISTS lab CASCADE" >/dev/null
create_new_schema
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null

ctl 'writes=monolith&reads=monolith'
ctl 'freeze=off'
total=$(( $(ms) - t0 ))

printf '  drain reverse stream   %5s ms\n' "$drain"
printf '  writes held for        %5s ms\n' "$total"

wait "$LOAD"
sleep 2

rule "4. what the clients saw"
cut_s=$(( (t0 - LOAD_T0) / 1000 ))
sed -n "1p;$((cut_s - 1)),$((cut_s + 3))p" out/rollback-load.tsv
tail -n 6 out/rollback-load.tsv

rule "5. nothing left behind"
sort -n out/rollback-acked.txt > out/rollback-acked.sorted
printf '  acknowledged writes    %s\n' "$(wc -l < out/rollback-acked.txt)"
printf '  absent from monolith   %s\n' "$(psql "$MONO" -qtAX -v ON_ERROR_STOP=1 <<SQL
CREATE TEMP TABLE acked (order_id bigint);
\copy acked FROM 'out/rollback-acked.sorted'
SELECT count(*) FROM acked a WHERE NOT EXISTS (SELECT 1 FROM lab.payments p WHERE p.order_id = a.order_id);
SQL
)"
