#!/usr/bin/env bash
#
# Copy a live table into another database while it is being written to, and
# find out what the copy does not bring with it.
#
# The copy is the part everyone worries about and it is the part that takes
# care of itself. CREATE SUBSCRIPTION does an initial COPY and then follows
# the publisher, both without blocking a single write on the monolith. What
# it does not do is anything else: no table, no index, no constraint, no
# sequence position, no DDL from that point on. Every one of those is a
# manual step, and the sequence is the one that takes the service down at
# cutover if you miss it.
source "$(dirname "$0")/lib.sh"

mkdir -p out
require_seed
teardown_replication
reset_monolith
create_new_schema

trap stop_service EXIT
start_service
"$BIN" load -duration 75s -first-order 200001 -acked out/backfill-acked.txt >out/backfill-load.tsv 2>&1 &
LOAD=$!
sleep 4

rule "1. the copy, while the table is being written to"

before_mono=$(rows_mono)
t0=$(ms)

# The publication is the publisher-side declaration of what is exported. It
# takes no lock worth the name and costs nothing until something subscribes.
m "CREATE PUBLICATION payments_pub FOR TABLE lab.payments" >/dev/null

# The subscription is where the work happens: it opens a replication slot on
# the publisher, COPYs the table as of the slot's snapshot, and then applies
# everything the slot has been holding since. The COPY and the stream cannot
# miss each other -- that handoff is the reason to use this rather than a
# pg_dump and a prayer.
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
printf '  indexes                %s on monolith, %s on new db (created by hand)\n' \
  "$(m "SELECT count(*) FROM pg_indexes WHERE tablename='payments'")" \
  "$(p "SELECT count(*) FROM pg_indexes WHERE tablename='payments'")"
printf '  replica identity       %s (default: the primary key)\n' \
  "$(m "SELECT relreplident FROM pg_class WHERE oid='lab.payments'::regclass")"

wait "$LOAD"
rule "the workload that ran through all of it"
tail -n 8 out/backfill-load.tsv
