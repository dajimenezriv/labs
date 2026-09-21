# Zero-downtime Database Split

- [The setup](#the-setup)
- [1. The copy is the easy part](#1-the-copy-is-the-easy-part)
  - [What does not come across](#what-does-not-come-across)
- [2. Shadow reads](#2-shadow-reads)
- [3. The cutover](#3-the-cutover)
- [4. Going back](#4-going-back)
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

--  bucket | count |               md5                
-- --------+-------+----------------------------------
--       0 |  9999 | 392a535480609f8b68ddcae276e10155
--       1 | 10000 | d67cbb6ab646ee7abc1551acbf7c0ea8
--       2 | 10000 | a0ae72ccbadf892075c9cc1949daacf7
--     ... |   ... |                              ...
```

4. Cutover of writes: achieving zero downtime is almost never worth. Instead we freeze for a few seconds writes on this table.

Moving a table out of a monolith's database is a copy, a flip, and a list of
things that do not get copied. The copy is the part everyone plans for and
the only part that takes care of itself.

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

```bash
./backfill.sh   # copy a live table into another database
./cutover.sh    # freeze, drain, flip, thaw -- measured
./rollback.sh   # and back again
```

Every script resets the monolith to the seed and tears the replication down
before it starts, so they can be run in any order and repeated.

## The setup

The monolith owns the table that is leaving:

```sql
lab.payments  200 000 rows   id, order_id, amount_cents, provider_ref, created_at
```

Everything that has to be decoupled in the application first — the joins, the
foreign key, the transaction that spanned this table and another — is taken
as already done, by the outbox. A payment is a single-row insert. What is
left is the part no amount of application work avoids: moving a table that is
being written to out of one database and into another, without dropping a
write.

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
  indexes                2 on monolith, 2 on new db (created by hand)
  replica identity       d (default: the primary key)
```

Logical replication copies rows. Not tables, not indexes, not constraints,
not sequences, and no DDL from that point on:

- **The table** has to exist on the subscriber before the subscription can
  copy a single row, with matching column names and types. Nothing creates it
  for you, and a column type that does not match is found at apply time.
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
  writes acknowledged    4012
  shadow mismatches      6
  replay lag             00:00:00.000183
```

Six, out of four thousand reads, and every one of them a client reading back
a write from a few hundred microseconds ago. Runs vary between zero and a
handful. That is the finding, not a disappointment: replication lag is not
what makes cutting reads over risky here, and the only way to learn that
without learning it in production is to have measured it.

What does make it risky is everything in the list above that the comparison
cannot see: an index that was not created, a column type that does not match,
a sequence that was never advanced. A shadow read compares answers, and two
databases can agree on every answer and still not be interchangeable.

## 3. The cutover

```bash
./cutover.sh
```

Five statements, and their order is the whole difference between a held
request and an outage.

```
  quiesce in-flight         20 ms
  drain to caught up       149 ms
  drop subscription         86 ms
  advance sequence          51 ms
  reverse replication      159 ms
  flip routing              15 ms
  ------------------------------
  writes held for          571 ms
  WAL still unconfirmed   7968 bytes at the moment rows agreed
```

```
t	ok/s	err/s	rawmiss/s	p99ms
12	401	0	0	4
13	399	0	0	4
14*	401	0	0	3
15	187	0	0	577
16	399	0	0	6

acknowledged        27779
errors              0
read-after-write    0 misses
p50 / p99 / max     3 / 4 / 583 ms

  acknowledged writes    27779
  absent from new db     0
```

Half a second of requests taking half a second, and nothing else. No errors,
no lost writes, no client that failed to read back what it had just written.
That is what "without downtime" is allowed to mean, and it is a claim about
the client's timeout, not about the database: the writes were **held, not
rejected**. A request that waits 571 ms is slow. A request that gets a 503 is
downtime. The loader's client timeout is 3 s, and the entire runbook fits
inside it with room to spare.

The quiesce is one `sync.RWMutex`. Writers hold it for reading for the
duration of their write; the freeze takes it for writing, which blocks new
writes *and* does not return until the in-flight ones have committed. Both
halves of a quiesce in one primitive, and the second half is the one that
matters: after `freeze=on` returns, the monolith's `payments` table is a
fixed target, which is the only condition under which "caught up" means
anything at all.


Then the drain, and the trap that the last line of the output is about. When
the two row counts agreed there were still **8 KB of WAL unconfirmed** on
the publisher. The data was all there; the acknowledgement was not, and it
would not have been for up to ten seconds. Gating the freeze on LSNs turns a
571 ms cutover into a ten-second one, and the ten seconds are entirely the
feedback interval.

The order of the remaining four is not arbitrary:

1. **Drop the forward subscription** before the new database generates a
   single id of its own, or two writers are inserting into one table.
2. **`setval` the sequence** — §1's stranded `1`, moved past the highest id
   that arrived, plus a margin. It has to be after the drain, because the
   drain is what decides what the highest id is. Skip it and every insert
   asks for an id, gets 1, 2, 3, and claims a primary key that arrived in the
   COPY; there are 200 000 collisions to climb through before the first write
   succeeds.
3. **Create the reverse subscription** with `copy_data = false`, now, inside
   the freeze. It carries everything written from this moment on and nothing
   before it, so the only way it covers the whole post-cutover window is to
   exist before the window opens. This is the 159 ms that buys §4.
4. **Flip the routing**, 15 ms, the only step anyone remembers.

A last note on what the freeze is worth. It binds writers that go through
this service, and in the lab that is all of them. In production it is not:
other replicas need the flag to be shared state rather than a mutex in one
process, and cron jobs, batch workers and admin sessions do not read the flag
at all. The backstop belongs in the database — `LOCK TABLE lab.payments IN
EXCLUSIVE MODE` in a held transaction blocks writers while still allowing
`SELECT`, and blocks them by making them wait, which is the same hold with
teeth. Set `lock_timeout` before taking it, or the lock request queues behind
a long write and everything else queues behind the request.

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
  every write, immediately, and it is invisible in every check performed
  before the flip: the copy is complete, the row counts match, the shadow
  reads agree, the lag is microseconds. It also fails in both directions —
  forward at the cutover, backward at the rollback, where §4 measures the gap
  at 9 060.
- **Held is not rejected, and the client's timeout is the spec.** "Zero
  downtime" here means 571 ms of elevated latency inside a 3 s timeout. Pin
  the budget to the tightest caller timeout you have, then make the runbook
  fit inside it, and if it does not fit, the cutover needs to be shorter
  rather than the claim looser.
- **Gate the drain on rows, not on LSNs.** `confirmed_flush_lsn` is feedback
  driven and trails by up to `wal_receiver_status_interval`. It is the right
  metric for "is the publisher's disk safe" and the wrong one for "may I
  flip".
- **Count the writes, do not watch the dashboard.** A flip applied to a
  moving target loses acknowledged writes without producing a single error or
  a latency spike, because from the service's point of view nothing failed.
  The check that catches it is the one §3 ends on: every id that got a 2xx,
  looked up in the database that now owns it.
- **Reversibility is a decision made before the flip, not after.** The
  reverse subscription costs 159 ms inside the freeze and cannot be created
  retroactively, because `copy_data = false` means it only carries what comes
  after it.

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
  each carries about 20-30 ms of process startup. The 571 ms is an honest
  upper bound on the real cutover, not a floor.
