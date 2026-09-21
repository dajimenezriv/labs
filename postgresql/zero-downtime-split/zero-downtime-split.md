# Zero-downtime Database Split

- [The setup](#the-setup)
- [1. The copy is the easy part](#1-the-copy-is-the-easy-part)
  - [What does not come across](#what-does-not-come-across)
- [2. Shadow reads](#2-shadow-reads)
- [3. The cutover](#3-the-cutover)
  - [The runbook](#the-runbook)
  - [Without the sequence](#without-the-sequence)
  - [Without the freeze](#without-the-freeze)
- [4. Going back](#4-going-back)
- [5. What is gone for good](#5-what-is-gone-for-good)
- [What this costs you](#what-this-costs-you)
- [Notes](#notes)

`wal_level` determines how much information is written to the WAL. Default is `replica`, which supports archiving and replication. `minimal` writtes just the information to recover from a crash or immediate shutdown. `logical` supports logical decoding.

1. Decouple in code first:
   1. `Joins`. Application-level composition.
   2. `Foreign keys`.
   3. `Transactions that span the table and others.`. The pattern for this is an outbox.
2. Create the target and replicate:
   1. `Dual writes` in the app (write to old, then to new) look easy, but they're the worst option. Writes aren't atomic so updates can arrive in different orders which will silently diverge the copies.
   2. `Native Postgres logical replication`: simplest and most reliable. `DDL` is not replicated. `Sequences` are not replicated.

```sql
-- On the OLD database (requires wal_level = logical, restart if you change it)
CREATE PUBLICATION invoices_pub FOR TABLE invoices;

-- On the NEW database (schema must already exist)
CREATE SUBSCRIPTION invoices_sub
  CONNECTION 'host=old-db dbname=monolith user=replicator password=...'
  PUBLICATION invoices_pub
  WITH (copy_data = true);
```

3. Verify:

```sql
SELECT id / 10000 AS bucket,
       count(*),
       md5(string_agg(md5(t::text), '' ORDER BY id))
FROM lab.payments t
GROUP BY 1 ORDER BY 1;
```

Moving a table out of a monolith's database is a copy, a flip, and a list of
things that do not get copied. The copy is the part everyone plans for and
the only part that takes care of itself.

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5558/db -f seed.sql
```

```bash
./backfill.sh            # copy a live table into another database
./cutover.sh             # freeze, drain, flip, thaw -- measured
./cutover.sh naive-seq   # the same flip, without advancing the sequence
./cutover.sh naive-lag   # the same flip, without freezing or draining
./rollback.sh            # and back again
./atomicity.sh           # what the split costs after it has worked
```

Every script resets the monolith to the seed and tears the replication down
before it starts, so they can be run in any order and repeated.

## The setup

The monolith owns two tables joined by a foreign key:

```sql
lab.orders    500 000 rows   id, customer_id, amount_cents, status, created_at
lab.payments  200 000 rows   id, order_id -> lab.orders(id), amount_cents, ...
```

`payments` is what moves. The thing that makes it hard is not the size, it is
that paying an order touches both tables in one transaction:

```sql
BEGIN;
  INSERT INTO lab.payments (order_id, amount_cents, provider_ref) VALUES (...);
  UPDATE lab.orders SET status = 'paid' WHERE id = $1;
COMMIT;
```

The seam runs through a constraint and through a commit, and neither of them
survives the split. §5 is about what that costs.

The service (`go run . serve`) is an HTTP API with two connection pools and a
routing switch that can be moved while it is running, which is the only
honest way to do this: the migration has to happen to a service that is
already up. The workload (`go run . load`) pays 400 orders a second, and
after each write it immediately reads that write back through the same API.
Every order is paid exactly once, so an id it was given a 2xx for and that is
not in the database afterwards is a write the service acknowledged and lost.

The only non-default Postgres setting is `wal_level = logical` on both, and
it is the one that has to be decided months in advance. It ships as
`replica`, which is enough for streaming replication and PITR but carries no
row images, so a publication on a stock cluster produces nothing. Raising it
needs a restart of the primary — the first step of the zero-downtime
migration is a downtime, and it is not one you can take on the day.

The second database has it too, for the same reason: rollback is a
subscription pointing the other way, and you cannot create it after the thing
you need it for has already happened.

## 1. The copy is the easy part

```bash
./backfill.sh
```

`CREATE PUBLICATION` on the monolith, `CREATE SUBSCRIPTION` on the new
database, and 200 000 rows move while the table is being written to:

```
  rows before copy       201617
  rows after copy        202022
  written during copy    405   <- none of them blocked
  initial COPY           945 ms
```

Nothing was locked, nothing queued, and no write was lost between the
snapshot and the stream. That handoff is the whole reason to use logical
replication instead of a `pg_dump` and a maintenance window: the subscription
opens a replication slot _first_, copies the table as of the slot's snapshot,
and then applies everything the slot has been holding since. There is no gap
for a write to fall into.

Then it follows. Three different answers to "how far behind is it":

```
        rows         bytes          time
          29         29472  00:00:00.000215
          24         44280  00:00:00.000245
          26          8216  00:00:00.000346
          21          4200  00:00:00.000271
          27         23392  00:00:00.000134
```

They do not agree, and the disagreement is the useful part:

- **time** is `pg_stat_replication.replay_lag`, computed by Postgres on the
  publisher. 200 microseconds. Replication is not the bottleneck in this
  migration and will not be the reason anything goes wrong.
- **rows** is two `count(*)`s against two databases, and at 400 writes/s the
  ~60 ms between them is worth ~25 rows on its own. It cannot resolve a lag
  this small; what it is measuring here is mostly itself.
- **bytes** is WAL the publisher has produced and the subscriber has not
  confirmed, and it never reaches zero under load — the subscriber only
  reports its position every `wal_receiver_status_interval`, 10 s by default.
  This is the number the _monolith_ cares about, because unconfirmed WAL is
  disk it may not reclaim, and it is the wrong number to gate a cutover on.

### What does not come across

```
  sequence on monolith   204520
  sequence on new db     1   <- every insert here collides
  foreign keys           1 on monolith, 0 on new db
  indexes                2 on monolith, 2 on new db (created by hand)
  replica identity       d (default: the primary key)
```

Logical replication copies rows. Not tables, not indexes, not constraints,
not sequences, and no DDL from that point on:

- **The table** has to exist on the subscriber before the subscription can
  copy a single row, with matching column names and types. It is written by
  hand, which is where the foreign key quietly disappears — `orders` is not
  in this database and never will be.
- **The indexes** are written by hand too. Without the `(order_id)` index
  every read in the new service is a sequential scan, and the number only
  becomes visible when reads cut over.
- **The sequence** is the one that ends the outage debate. 200 000 rows
  arrived carrying ids up to 204 520, and `payments_id_seq` on the new
  database is still at **1**. It is not broken, and nothing will report it as
  a problem, because nothing is inserting there yet. It becomes a problem in
  the first millisecond after the flip.
- **DDL** is not replicated at all. A migration that adds a column to
  `lab.payments` between the backfill and the cutover breaks the subscription
  and it stays broken until someone applies the same DDL on the subscriber by
  hand. The freeze on schema changes starts at `CREATE SUBSCRIPTION`.

## 2. Shadow reads

Reads keep being served by the monolith. The new database is queried
alongside, purely so the answers can be compared:

```
  writes acknowledged    4014
  shadow mismatches      0
  replay lag             00:00:00.000112
```

Zero, and that is the finding, not a disappointment. It is worth having
because it is the only way to learn it without learning it in production —
but it means replication lag is not what makes cutting reads over risky here.
What makes it risky is everything in the list above that the comparison
cannot see: an index that was not created, a column type that does not match,
a constraint that no longer exists. A shadow read compares answers, and two
databases can agree on every answer and still not be interchangeable.

## 3. The cutover

Same rig, same load, three orderings of the same five statements:

| cutover            | writes held | errors | acknowledged and absent | read-after-write misses |
| ------------------ | ----------: | -----: | ----------------------: | ----------------------: |
| the runbook        |      530 ms |      0 |                       0 |                       0 |
| without `setval`   |      458 ms | 22 249 |                       0 |                       0 |
| without the freeze |        0 ms |      0 |                  **87** |                       1 |

### The runbook

```bash
./cutover.sh
```

```
  quiesce in-flight          9 ms
  drain to caught up       125 ms
  drop subscription         86 ms
  advance sequence          52 ms
  reverse replication      157 ms
  flip routing              17 ms
  ------------------------------
  writes held for          530 ms
  WAL still unconfirmed  38752 bytes at the moment rows agreed
```

```
t	ok/s	err/s	rawmiss/s	p99ms
12	401	0	0	3
13	399	0	0	3
14*	401	0	0	3
15	206	0	0	526
16	400	0	0	4

acknowledged        27804
errors              0
read-after-write    0 misses
p50 / p99 / max     3 / 4 / 543 ms
```

Half a second of requests taking half a second, and nothing else. No errors,
no lost writes, no client that failed to read back what it had just written.
That is what "without downtime" is allowed to mean, and it is a claim about
the client's timeout, not about the database: the writes were **held, not
rejected**. A request that waits 530 ms is slow. A request that gets a 503 is
downtime. The loader's client timeout is 3 s, and the entire runbook fits
inside it with room to spare.

The quiesce is one `sync.RWMutex`. Writers hold it for reading for the
duration of their write; the freeze takes it for writing, which blocks new
writes _and_ does not return until the in-flight ones have committed. Both
halves of a quiesce in one primitive, and the second half is the one that
matters: after `freeze=on` returns, the monolith's `payments` table is a
fixed target, which is the only condition under which "caught up" means
anything at all.

Then the drain, and the trap that the last line of the output is about. When
the two row counts agreed there were still **38 KB of WAL unconfirmed** on
the publisher. The data was all there; the acknowledgement was not, and it
would not have been for up to ten seconds. Gating the freeze on LSNs turns a
530 ms cutover into a ten-second one, and the ten seconds are entirely the
feedback interval.

The order of the remaining four is not arbitrary:

1. **Drop the forward subscription** before the new database generates a
   single id of its own, or two writers are inserting into one table.
2. **`setval` the sequence** — §1's stranded `1`, moved past the highest id
   that arrived, plus a margin. It has to be after the drain, because the
   drain is what decides what the highest id is.
3. **Create the reverse subscription** with `copy_data = false`, now, inside
   the freeze. It carries everything written from this moment on and nothing
   before it, so the only way it covers the whole post-cutover window is to
   exist before the window opens. This is the 157 ms that buys §4.
4. **Flip the routing**, 17 ms, the only step anyone remembers.

### Without the sequence

```bash
./cutover.sh naive-seq
```

Everything else identical; `setval` skipped.

```
t	ok/s	err/s	rawmiss/s	p99ms
13*	399	0	0	3
14	386	0	0	3
15	0	251	0	461
16	0	400	0	2
17	0	400	0	2

acknowledged        5584
errors              22249
```

Total outage, from the instant of the flip to the end of the run: every
insert asks the sequence for an id, gets 1, 2, 3, and every one of them is a
primary key that arrived in the COPY. It does not degrade and it does not
recover — it has 200 000 collisions to climb through at 400 a second before
the first write succeeds.

The reason this is the most common way to lose a Saturday is that nothing
before the flip is wrong. The copy is complete, the row counts match, the
shadow reads agree, the lag is microseconds. The sequence is a number in a
catalog nobody is looking at, and it is fine right up until the moment the
new database is asked to be a database rather than a copy.

### Without the freeze

```bash
./cutover.sh naive-lag
```

`setval` done, nothing held, nothing drained — the flip applied to a moving
target.

```
acknowledged        27989
errors              0
read-after-write    1 misses
p50 / p99 / max     3 / 4 / 61 ms

  acknowledged writes    27989
  absent from new db     87   <- acknowledged and lost
```

This is the dangerous one. **Zero errors.** No latency spike — the p99 is
better than the runbook's, because nothing was held. Every dashboard is
green, the deploy looks like the cleanest of the three, and 87 acknowledged
payments are not in the database that now owns payments.

They are the writes that were still in the replication stream when the
subscription was dropped. They committed on the monolith, they were
acknowledged to the client, and they are still there — the monolith just
stopped being the place anybody looks. Nothing in the service can detect
this, because from the service's point of view nothing failed. It surfaces
weeks later as a customer who was charged and has no payment record.

The single read-after-write miss is the same event caught in the act: one
client wrote, the flip happened underneath it, and its own write was not in
the database it was now reading from.

## 4. Going back

```bash
./rollback.sh
```

The cutover is reversible for exactly as long as the reverse subscription has
been running, which is why it gets created inside the freeze rather than on
the day it is needed. Twenty seconds of live traffic on the new database, and
then back:

```
  drain reverse stream     144 ms
  writes held for          672 ms

acknowledged        27543
errors              0
absent from monolith   0
```

Same shape as the cutover, because it is the same procedure with the arrows
turned around. What is new is what those twenty seconds did to the monolith:

```
  max(id) in its table   210629
  its sequence           201569
  inserts that would     9060   <- every one a duplicate key
```

9 599 rows arrived in the monolith through the reverse subscription, carrying
ids the _new_ database generated. An arriving row does not advance the
sequence that would have produced it, so the monolith's sequence is now
stranded 9 060 behind its own table. Nothing is wrong while the monolith is
not inserting. It becomes wrong the instant it is asked to again — which is
precisely what rolling back means, and it is §3's outage waiting at the end
of the recovery path.

So the rollback runbook is the cutover runbook, including the `setval`, in
the other direction. A rollback plan that is not itself a tested runbook is
two outages, not one.

## 5. What is gone for good

```bash
./atomicity.sh
```

The cutover is a day. This is the rest of the time.

```
  payment for a non-existent order, monolith   HTTP 500
  the same request, new database               HTTP 204   <- accepted
```

The foreign key was not weakened, it was deleted, and the write path is the
only thing left that checks. It is worth being precise about what that means:
the database no longer has an opinion about whether a payment points at a
real order, and every future bug in the calling code is now a data integrity
bug.

The transaction is gone in the same way. Paying an order used to be one
commit; it is now an insert in one database and an update in another, with a
gap between them that the process can die in. Three percent of writes do,
here:

```
  writes that stopped halfway   240 of 9592
```

And finding them is the third thing the split took away. The query that would
have found them is a join, and the join no longer exists:

```
  payments recorded             17803
  orders still open             240
  payments with no order        1
```

Both numbers come out of a program, not a query — read the ids out of one
database, stream them into the other, compare there. It found all 240 orders
that were paid and left open, and the single orphan payment from the top of
this section. That program is now permanent infrastructure: it needs a
watermark, a schedule, an alert, and somewhere to put what it finds, and it
is the piece that is invariably missing when the split is declared done.

What actually fixes the 240 is not a better reconciler. It is making the
second write derivable from the first — a transactional outbox in the
payments database, drained by a relay that retries until the order update
sticks. The reconciler stays anyway, to catch the days the relay is wrong.

## What this costs you

- **The migration starts with a restart.** `wal_level = logical` is not the
  default and cannot be changed without bouncing the primary. If that restart
  has not already happened, the online migration is not available to you
  today, and the honest plan starts with scheduling the outage you were
  trying to avoid.
- **The copy is the part that works.** 200 000 rows, 945 ms, nothing blocked,
  nothing lost. Every hour spent worrying about the copy is an hour not spent
  on the list of things it does not copy.
- **The sequence is the outage.** It is the one item on that list that fails
  100% of writes, immediately, and it is invisible in every check performed
  before the flip. It also fails in both directions: forward at the cutover,
  backward at the rollback.
- **Held is not rejected, and the client's timeout is the spec.** "Zero
  downtime" here means 530 ms of elevated latency inside a 3 s timeout. Pin
  the budget to the tightest caller timeout you have, then make the runbook
  fit inside it, and if it does not fit, the cutover needs to be shorter
  rather than the claim looser.
- **Gate the drain on rows, not on LSNs.** `confirmed_flush_lsn` is feedback
  driven and trails by up to `wal_receiver_status_interval`. It is the right
  metric for "is the publisher's disk safe" and the wrong one for "may I
  flip".
- **The cutover that looks cleanest is the one that lost data.** Skipping the
  freeze produced no errors, no latency spike, and 87 acknowledged writes
  that are not in the new database. If the only evidence the migration is
  going well is that nothing went red, there is no evidence.
- **Reversibility is a decision made before the flip, not after.** The
  reverse subscription costs 157 ms inside the freeze and cannot be created
  retroactively, because `copy_data = false` means it only carries what comes
  after it.
- **The split is permanent and the guarantees do not come back.** One
  constraint and one transaction, traded for a reconciler you have to write,
  run and watch forever. That trade may well be worth it — but it is the
  actual price of the microservice, and it is paid every day after the day
  everyone remembers.

## Notes

- `CREATE SUBSCRIPTION` connects from the subscriber to the publisher, so the
  connection string is resolved inside the subscriber's container
  (`host=monolith`), not from wherever the script runs.
- Subscriptions own a replication slot on the publisher. Drop the subscriber
  side first; a subscription dropped while its publisher is unreachable
  leaves the slot behind, and a forgotten slot holds WAL until the publisher
  runs out of disk. `pg_replication_slots` is the thing to alert on.
- `srsubstate` in `pg_subscription_rel` goes `i` → `d` → `f` → `s` → `r`.
  Only `r` means the table is following the publisher, and it still does not
  mean it is at the head of it.
- A table with no primary key needs `REPLICA IDENTITY FULL` before its
  `UPDATE`s and `DELETE`s can replicate at all, and then each one is matched
  on the subscriber by full row comparison — a sequential scan per row. The
  default `d` is only enough because there is a primary key here.
- Bidirectional replication does not loop by accident in this lab because the
  forward subscription is dropped before the reverse one is created. If both
  have to exist at once, `CREATE SUBSCRIPTION ... WITH (origin = none)` is
  what stops a row applied from a subscription being published back.
- The per-step timings are measured around `psql` invocations from bash, so
  each carries about 20-30 ms of process startup. The 530 ms is an honest
  upper bound on the real cutover, not a floor.
