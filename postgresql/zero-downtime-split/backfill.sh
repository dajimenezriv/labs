#!/usr/bin/env bash

source "$(dirname "$0")/lib.sh"

mkdir -p out

psql $MONO -f seed.sql

teardown_replication
create_new_schema

trap stop_service EXIT
start_service
go run . load -duration 20s -acked out/backfill-acked.txt >out/backfill-load.tsv 2>&1 &
LOAD=$!
sleep 4

rule "1. the copy, while the table is being written to"

before_mono=$(rows_mono)
t0=$(ms)

m "CREATE PUBLICATION payments_pub FOR TABLE lab.payments" >/dev/null
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null

# 'r' is ready-to-stream. The initial COPY is done and the subscription is
# now following the publisher, which is a different thing from being at the
# head of it.
while [[ "$(sub_state)" != "r" ]]; do :; done
copy_ms=$(( $(ms) - t0 ))

after_mono=$(rows_mono)
printf '  rows before copy       %s\n' "$before_mono"
printf '  rows after copy        %s\n' "$after_mono"
printf '  written during copy    %s   <- none of them blocked\n' "$((after_mono - before_mono))"
printf '  initial COPY           %s ms\n' "$copy_ms"

rule "2. lag while it follows, under the same write rate"

# Three different answers to "how far behind is it", and they do not agree.
# Rows and seconds are what the product cares about. Bytes of unconfirmed
# WAL is what the monolith cares about, because that is disk it cannot
# reclaim -- and it is also the one that will not go to zero here, because
# the subscriber only reports its position every wal_receiver_status_interval
# (10s by default) and there is always something in flight under load.
printf '  %10s  %12s  %12s\n' rows bytes time
for _ in 1 2 3 4 5; do
  printf '  %10s  %12s  %12s\n' "$(rows_behind)" "$(lag_bytes)" "$(lag_time)"
  sleep 1
done

rule "3. what did not come across"

printf '  sequence on monolith   %s\n' "$(m "SELECT last_value FROM lab.payments_id_seq")"
printf '  sequence on new db     %s   <- every insert here collides\n' "$(p "SELECT last_value FROM lab.payments_id_seq")"
printf '  indexes                %s on monolith, %s on new db\n' \
  "$(m "SELECT count(*) FROM pg_indexes WHERE tablename='payments'")" \
  "$(p "SELECT count(*) FROM pg_indexes WHERE tablename='payments'")"
printf '  replica identity       %s (default: the primary key)\n' \
  "$(m "SELECT relreplident FROM pg_class WHERE oid='lab.payments'::regclass")"

wait "$LOAD"
rule "the workload that ran through all of it"
tail -n 8 out/backfill-load.tsv
