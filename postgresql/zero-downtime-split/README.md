# Zero-downtime Database Split

- [The setup](#the-setup)
- [1. The copy is the easy part](#1-the-copy-is-the-easy-part)
  - [What does not come across](#what-does-not-come-across)
- [2. Shadow reads](#2-shadow-reads)
- [3. The cutover](#3-the-cutover)
- [4. Going back](#4-going-back)
- [What this costs you](#what-this-costs-you)
- [Notes](#notes)

Should we add the `EXCLUSIVE_LOCK` thing?

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

The monolith owns two tables, joined by a foreign key and written by one
transaction:

```sql
lab.orders    200 000 rows   id, created_at
lab.payments  200 000 rows   id, order_id -> lab.orders(id), created_at
```

**Both of them move, together.** That is the decision the rest of the lab
follows from, and it is worth being explicit about why: a foreign key cannot
span two databases, and neither can a transaction. Move one table and you
lose both — the constraint has to be dropped and the single commit becomes
two commits with a gap in the middle. Move the pair and you keep them, and
the application does not change at all.

So the unit of migration is not a table, it is a **consistency group**: the
set of tables that are written together or referenced together. Which tables
are in the group is not a preference, it is read off the transaction
boundaries and the foreign key graph.

```go
tx.QueryRow(`INSERT INTO lab.orders DEFAULT VALUES RETURNING id`)
tx.Exec(`INSERT INTO lab.payments (order_id) VALUES ($1)`, orderID)
tx.Commit()
```

One commit before the cutover, one commit after it, both tables in whichever
database currently owns them.

The service (`go run . serve`) is an HTTP API with two connection pools and a
routing switch that can be moved while it is running, which is the only
honest way to do this: the migration has to happen to a service that is
already up. The workload (`go run . load`) places 400 orders a second and,
after each one, immediately reads it back through the same API. Every write
returns the order id it created, so an id it was given a 2xx for and that has
no payment behind it afterwards is a write the service acknowledged and lost.

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

`CREATE PUBLICATION ... FOR TABLE lab.orders, lab.payments` on the monolith,
`CREATE SUBSCRIPTION` on the new database, and 400 000 rows across both
tables move while both are being written to:

```
  rows before copy       402384
  rows after copy        403056
  written during copy    672   <- none of them blocked
  initial COPY           778 ms
```

One publication covering both tables is not a convenience, it is the reason
the copy is safe. Logical replication preserves transaction boundaries across
every table in a publication: a commit that inserted an order and its payment
is applied on the subscriber as one transaction. Split the two across
separate subscriptions and there is a window in which the new database has
the payment and not the order — the exact tear the single commit exists to
prevent.

Each table syncs with its own worker, so `pg_subscription_rel` has a row per
table and the subscription is only following the publisher once every one of
them reports `r`.

Nothing was locked, nothing queued, and no write was lost between the
snapshot and the stream. That handoff is the whole reason to use logical
replication instead of a `pg_dump` and a maintenance window: the subscription
opens a replication slot _first_, copies the table as of the slot's snapshot,
and then applies everything the slot has been holding since. There is no gap
for a write to fall into.

Then it follows. Three different answers to "how far behind is it":

```
        rows         bytes          time
          56         37248  00:00:00.019795
          56         46032  00:00:00.000152
          54         10776  00:00:00.000229
          54         13672  00:00:00.000289
          60         21784  00:00:00.000318
```

They do not agree, and the disagreement is the useful part:

- **time** is `pg_stat_replication.replay_lag`, computed by Postgres on the
  publisher. 200 microseconds. Replication is not the bottleneck in this
  migration and will not be the reason anything goes wrong.
- **rows** is two `count(*)`s against two databases, and at 400 writes/s —
  two rows per write, one per table — the ~60 ms between them is worth ~50
  rows on its own. It cannot resolve a lag
  this small; what it is measuring here is mostly itself.
- **bytes** is WAL the publisher has produced and the subscriber has not
  confirmed, and it never reaches zero under load — the subscriber only
  reports its position every `wal_receiver_status_interval`, 10 s by default.
  This is the number the _monolith_ cares about, because unconfirmed WAL is
  disk it may not reclaim, and it is the wrong number to gate a cutover on.

### What does not come across

```
  orders_id_seq            204057 on monolith, 1 on new db
  payments_id_seq          204096 on monolith, 1 on new db
  indexes                       3 on monolith, 2 on new db
  foreign keys                  1 on monolith, 0 on new db
  replica identity              d (default: the primary key)
```

Logical replication copies rows. Not tables, not indexes, not constraints,
not sequences, and no DDL from that point on:

- **The tables** have to exist on the subscriber before the subscription can
  copy a single row, with matching column names and types. Nothing creates
  them for you, and a column type that does not match is found at apply time.
- **The sequences** — plural now, and that is the point. Two tables mean two
  ways to cause the same outage, and a group of a dozen tables means twelve,
  every one of them to be advanced inside the freeze while a clock runs. This
  is the argument for migrating groups rather than the whole schema at once.
- **The foreign key** is not recreated. Here that is recoverable, because
  both ends of it came across and the constraint can simply be added on the
  new database. Had only `payments` moved, there would have been nothing to
  add it to.
- **DDL** is not replicated at all. A migration that adds a column between
  the backfill and the cutover breaks the subscription, and it stays broken
  until someone applies the same DDL on the subscriber by hand. The freeze on
  schema changes starts at `CREATE SUBSCRIPTION`.

The index is the one with a number attached, because it is measurable before
anything has gone wrong. This is `GET /pay`'s own query — not a benchmark,
the actual read path — run against both databases:

```
  read path, monolith        0.145 ms   Index Only Scan
  read path, new db         14.460 ms   Seq Scan   <- if reads cut over now
  read path, new db          0.177 ms   Bitmap Heap Scan   <- after finishing the schema
  index + FK took         241 ms   (the FK validates every row)
```

**A 100x regression, waiting to be switched on.** Every check that runs before
a read cutover passes: the row counts match, the data is identical, the
replication lag is microseconds. The only thing wrong is that nobody created
an index, and nothing anywhere reports a missing index as a problem — it is
not an error, it is a plan.

Finishing the schema belongs in the backfill rather than the cutover, and
the 241 ms is why. Adding a foreign key validates every existing row, so it
is a full table scan whose cost scales with the table — 241 ms here on
200 000 rows with a warm cache, and measured at 1262 ms on a run where it
contended with the apply worker. Neither number is large; both are large
enough that they have no business being inside a freeze, and on a table a
hundred times this size neither would be survivable there.

## 2. Shadow reads

Reads keep being served by the monolith. The new database is queried
alongside, purely so the answers can be compared:

```
  writes acknowledged    4014
  shadow mismatches      3
  replay lag             00:00:00.00019
```

Three, out of four thousand reads, and every one of them a client reading back
a write from a few hundred microseconds ago. Runs vary between zero and a
handful. That is the finding, not a disappointment: replication lag is not
what makes cutting reads over risky here, and the only way to learn that
without learning it in production is to have measured it.

What does make it risky is everything in the list above that the comparison
cannot see. The shadow read compares *answers*, and both databases answer
`1` — one in 0.146 ms off an index, the other in 14.332 ms off a sequential
scan. Correctness is identical and the service would fall over. A shadow read compares answers, and two
databases can agree on every answer and still not be interchangeable.

## 3. The cutover

```bash
./cutover.sh
```

Five statements, and their order is the whole difference between a held
request and an outage.

```
  quiesce in-flight         15 ms
  drain to caught up       147 ms
  drop subscription         87 ms
  advance sequence          66 ms
  reverse replication      142 ms
  flip routing              17 ms
  ------------------------------
  writes held for          548 ms
  WAL still unconfirmed  35456 bytes at the moment rows agreed
```

```
t	ok/s	err/s	rawmiss/s	p99ms
12	400	0	0	3
13	399	0	0	3
14*	401	0	0	3
15	200	0	0	541
16	401	0	0	3

acknowledged        27799
errors              0
read-after-write    0 misses
p50 / p99 / max     3 / 3 / 551 ms

  acknowledged writes    27799
  absent from new db     0
```

Half a second of requests taking half a second, and nothing else. No errors,
no lost writes, no client that failed to read back what it had just written.
That is what "without downtime" is allowed to mean, and it is a claim about
the client's timeout, not about the database: the writes were **held, not
rejected**. A request that waits 548 ms is slow. A request that gets a 503 is
downtime. The loader's client timeout is 3 s, and the entire runbook fits
inside it with room to spare.

There is a step before the freeze that does not appear in the timings,
because it happens while writes are still flowing and therefore costs
nobody anything: **wait until the subscriber is nearly caught up, and only
then freeze.** A freeze lasts as long as whatever is still queued, so
freezing into a backlog makes the outage the size of the backlog. Skipping
this turned a 506 ms cutover into a **3721 ms** one in `rollback.sh` —
past the workload's 3 s client timeout, and 16 writes failed that had no
business failing. The backlog was the apply worker stalled behind the
foreign key validation from §1.

The quiesce is one `sync.RWMutex`. Writers hold it for reading for the
duration of their write; the freeze takes it for writing, which blocks new
writes *and* does not return until the in-flight ones have committed. Both
halves of a quiesce in one primitive, and the second half is the one that
matters: after `freeze=on` returns, the monolith's `payments` table is a
fixed target, which is the only condition under which "caught up" means
anything at all.


Then the drain, and the trap that the last line of the output is about. When
the two row counts agreed there were still **35 KB of WAL unconfirmed** on
the publisher. The data was all there; the acknowledgement was not, and it
would not have been for up to ten seconds. Gating the freeze on LSNs turns a
548 ms cutover into a ten-second one, and the ten seconds are entirely the
feedback interval.

The order of the remaining four is not arbitrary:

1. **Drop the forward subscription** before the new database generates a
   single id of its own, or two writers are inserting into one table.
2. **`setval` every sequence** — §1's stranded `1`s, moved past the highest
   id that arrived, plus a margin. Both of them, and in a real group all
   twelve. It has to be after the drain, because the drain is what decides
   what the highest id is. Skip one and every insert against that table asks
   for an id, gets 1, 2, 3, and claims a primary key that arrived in the
   COPY; there are 200 000 collisions to climb through before the first write
   succeeds.
3. **Create the reverse subscription** with `copy_data = false`, now, inside
   the freeze. It carries everything written from this moment on and nothing
   before it, so the only way it covers the whole post-cutover window is to
   exist before the window opens. This is the 142 ms that buys §4.
4. **Flip the routing**, 17 ms, the only step anyone remembers.

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
  drain reverse stream     178 ms
  writes held for          615 ms

acknowledged        27580
errors              0
absent from monolith   0
```

Same shape as the cutover, because it is the same procedure with the arrows
turned around. What is new is what those twenty seconds did to the monolith:

```
  max(id) in its table   210722
  its sequence           201670
  inserts that would     9052   <- every one a duplicate key
```

9 700 rows arrived in the monolith through the reverse subscription, carrying
ids the _new_ database generated. An arriving row does not advance the
sequence that would have produced it, so the monolith's sequence is now
stranded 9 052 behind its own table. Both sequences are, in fact — this is
the `orders` one, and `payments` has the same gap. Nothing is wrong while the monolith is
not inserting. It becomes wrong the instant it is asked to again — which is
precisely what rolling back means, and it is §3's outage waiting at the end
of the recovery path.

So the rollback runbook is the cutover runbook, including a `setval` for
every sequence in the group, in the other direction. A rollback plan that is not itself a tested runbook is
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
- **The sequences are the outage**, one per table in the group. They fail
  every write immediately, they are invisible in every check performed before
  the flip — the copy is complete, the row counts match, the shadow reads
  agree, the lag is microseconds — and they fail in both directions, forward
  at the cutover and backward at the rollback, where §4 measures the gap at
  9 052. Twelve tables is twelve chances to miss one inside a freeze.
- **The unit of migration is a consistency group, not a table.** Transaction
  boundaries and the foreign key graph decide what travels together, and
  neither is negotiable: a constraint cannot span two databases and neither
  can a commit. Move a group and the application does not change. Move half
  of one and you have traded a foreign key and a transaction for a reconciler
  you now have to write, run and watch forever.
- **Do not freeze into a backlog.** The freeze lasts as long as whatever the
  subscriber still has to apply. Waiting for it to catch up *before* freezing
  costs nothing because writes are still flowing; skipping that wait turned
  506 ms into 3721 ms and broke the client timeout.
- **Held is not rejected, and the client's timeout is the spec.** "Zero
  downtime" here means 548 ms of elevated latency inside a 3 s timeout. Pin
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
  reverse subscription costs 142 ms inside the freeze and cannot be created
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
  each carries about 20-30 ms of process startup. The 548 ms is an honest
  upper bound on the real cutover, not a floor.
