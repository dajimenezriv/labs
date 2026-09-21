#!/usr/bin/env bash
#
# What the split costs after it has succeeded.
#
# The cutover is a day. This is the rest of the time. Two tables that used to
# be joined by a foreign key and written by one transaction are now in two
# databases, and both of those guarantees are gone -- not degraded, gone.
# Nothing in the cutover runbook gives them back.
source "$(dirname "$0")/lib.sh"

mkdir -p out
require_seed
teardown_replication
reset_monolith
create_new_schema

trap stop_service EXIT
start_service

# Does the service accept a payment for an order that does not exist?
orphan() {
  curl -s -o /dev/null -w '%{http_code}' -XPOST "$SVC/pay?order_id=999999999&amount=1000"
}

rule "1. the constraint that is gone"

printf '  payment for a non-existent order, monolith   HTTP %s\n' "$(orphan)"

LOAD_T0=$(ms)
"$BIN" load -duration 45s -acked out/atomicity-acked.txt >out/atomicity-load.tsv 2>&1 &
LOAD=$!
sleep 3

m "CREATE PUBLICATION payments_pub FOR TABLE lab.payments" >/dev/null
p "CREATE SUBSCRIPTION payments_sub
   CONNECTION 'host=monolith port=5432 user=postgres password=postgres dbname=db'
   PUBLICATION payments_pub" >/dev/null
while [[ "$(sub_state)" != "r" ]]; do :; done
do_cutover safe >/dev/null

printf '  the same request, new database               HTTP %s   <- accepted\n' "$(orphan)"

rule "2. the transaction that is gone"

# Paying an order is now an insert here and an update there. Between the two
# the process can die, and 3% of the time it does. This is not a setting that
# can be turned off; it is what two commits in two databases means.
ctl 'halfwrite=3'
sleep 20
ctl 'halfwrite=0'
printf '  writes that stopped halfway   %s of %s\n' "$(stat_of halved)" "$(stat_of ok)"

wait "$LOAD"
sleep 2

rule "3. the reconciliation you now have to write"

# The query that would have found these is a join, and the join no longer
# exists: the payments are in one database and the orders are in another.
# What replaces it is a program -- read the ids out of one, stream them into
# the other, compare there. Cheap here with 30k rows and a fixed window; the
# real version needs a watermark, a schedule, and somewhere to put what it
# finds.
psql "$PAY" -qtAX -c \
  "COPY (SELECT order_id FROM lab.payments WHERE provider_ref LIKE 'svc-%') TO STDOUT" \
  > out/paid-ids.txt

psql "$MONO" -qtAX -v ON_ERROR_STOP=1 <<'SQL' | while IFS='|' read -r label n; do printf '  %-30s%s\n' "$label" "$n"; done
CREATE TEMP TABLE paid (order_id bigint);
\copy paid FROM 'out/paid-ids.txt'
CREATE INDEX ON paid (order_id);
SELECT 'payments recorded', count(*) FROM paid;
SELECT 'orders still open', count(*) FROM paid p
  JOIN lab.orders o ON o.id = p.order_id WHERE o.status = 'pending';
SELECT 'payments with no order', count(*) FROM paid p
  WHERE NOT EXISTS (SELECT 1 FROM lab.orders o WHERE o.id = p.order_id);
SQL
