# Table Bloat and Vacuum

- [The setup](#the-setup)
- [1. The table grows, the row count does not](#1-the-table-grows-the-row-count-does-not)
  - [VACUUM, and what "reclaim" means](#vacuum-and-what-reclaim-means)
  - [VACUUM FULL](#vacuum-full)
  - [The counter that lies](#the-counter-that-lies)
- [2. Why one workload bloats 6x and another 2.2x](#2-why-one-workload-bloats-6x-and-another-22x)
- [3. When autovacuum decides to care](#3-when-autovacuum-decides-to-care)
- [4. One open transaction, and cleanup stops](#4-one-open-transaction-and-cleanup-stops)
  - [What it looks like while it is happening](#what-it-looks-like-while-it-is-happening)
  - [The holders with no session to find](#the-holders-with-no-session-to-find)
- [What this costs you](#what-this-costs-you)
- [Notes](#notes)

We should monitor how many dead tuples we have, if the autovacuum is running and so.

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

## The setup

- 1M rows, 86 MB total (57 MB heap, 28 MB index).
- Two indexes: the primary key, and one on `status`.
- Stock autovacuum. The only non-default setting is
  `log_autovacuum_min_duration=0`, so autovacuum runs show up in the log
  instead of having to be inferred.

`UPDATE` never edits a row. Another transaction's snapshot may still need the
old value, so Postgres writes a whole new row version elsewhere and marks the
old one dead. Two columns exist to exercise that:

```sql
value   numeric   -- not indexed
status  text      -- indexed
```

## 1. The table grows, the row count does not

```bash
./bloat.sh
```

Five `UPDATE lab.readings SET status = ...` passes, each one rewriting all 1M
rows:

| step          | heap MB | idx MB | n_live_tup | n_dead_tup | autovacs |
| ------------- | ------: | -----: | ---------: | ---------: | -------: |
| baseline      |      57 |     28 |  1 000 000 |          0 |        0 |
| update pass 1 |     114 |     55 |  1 000 000 |  1 000 000 |        0 |
| update pass 2 |     172 |     63 |  1 000 000 |  2 000 000 |        0 |
| update pass 3 |     229 |     73 |  1 000 000 |  3 000 000 |        0 |
| update pass 4 |     287 |     90 |  1 000 000 |  4 000 000 |        0 |
| update pass 5 |     344 |    101 |  1 000 000 |  5 000 000 |        0 |

`n_live_tup` is flat at exactly 1M for the whole run. The heap adds 57 MB a
pass — one full copy of the table per pass, which is precisely what "rewrite
every row" means. Six times the size, same data.

`autovacs` is 0 the whole way, and that is not autovacuum failing. Each pass
takes ~6 s and `autovacuum_naptime` is 60 s, so the launcher simply has not
come around yet. Left alone afterwards it fires once and clears all 5M:

```
automatic vacuum of table "db.lab.readings": index scans: 1
  pages: 0 removed, 29412 remain, 29412 scanned (100.00% of total)
  tuples: 1000008 removed, 1000000 remain, 0 are dead but not yet removable
  index scan needed: 22059 pages from table (75.00% of total) had 3000000 dead item identifiers removed
  WAL usage: 85125 records, 30914 full page images, 123190944 bytes
  system usage: CPU: user: 1.66 s, system: 0.44 s, elapsed: 12.00 s
```

**`pages: 0 removed`.** It reclaimed 3M dead tuples and gave back zero pages.
That one line is the whole misunderstanding about `VACUUM`: it can only
truncate pages at the _end_ of the file that are entirely empty, and live rows
are spread across all 29 412 of them. Everything else becomes free space
_inside_ pages the table still owns — reusable by this table, invisible to the
filesystem and to every other table.

The rest of that log line is worth reading field by field, because it is the
only place the work is itemised:

- **`3000000 dead item identifiers removed`** — a _TID_ (tuple id) is a row
  version's physical address, `(page, slot)`. Indexes store TIDs, so before a
  dead version's slot can be reused, every index entry pointing at it has to
  go. That is what `index scans: 1` is: a full pass over both indexes. It is
  also why vacuum cost scales with index count, not just table size.
- **`visibility map: 29412 pages set all-visible`** — one bit per page saying
  "every row here is visible to every transaction". It is what lets the next
  vacuum skip pages entirely, and what makes index-only scans possible. Churn
  clears those bits; vacuum sets them again.
- **`WAL usage: … 123190944 bytes`** — 123 MB of WAL to clean up a 57 MB table.
  Cleanup is not free, it is a write amplifier, and it all ships to your
  replicas and your backups.

### VACUUM, and what "reclaim" means

| step                | heap MB | idx MB | n_dead_tup |
| ------------------- | ------: | -----: | ---------: |
| after 5 passes      |     344 |    101 |  5 000 000 |
| after `VACUUM`      |     344 |    101 |          0 |
| 5 more passes       |     382 |    138 |  5 000 000 |
| after `VACUUM FULL` |      65 |     28 |  5 000 000 |

`VACUUM` took 897 ms and moved the size by nothing. Dead tuples went to zero,
which is the actual product: 287 MB of reusable space inside a 344 MB file.

The five passes after it are the proof. The heap sat at **344 MB for four
consecutive passes** before moving at all:

```
update pass 1   344 MB      update pass 4   344 MB
update pass 2   344 MB      update pass 5   382 MB
update pass 3   344 MB
```

Reclaimed space is a budget, and a full-table update spends 57 MB of it. Four
passes fit in the 287 MB `VACUUM` freed; the fifth did not and extended the
file. A table under steady churn settles at whatever high-water mark its
vacuum-to-write ratio implies and stays there. That is not a leak, and chasing
it back to 57 MB is not a goal.

Indexes are the part that keeps creeping — 101 → 138 MB across those same five
passes, while the heap held flat. Btree pages only become reusable when a page
empties completely, so index bloat is stickier than heap bloat, and `VACUUM`
alone will not undo it. `REINDEX CONCURRENTLY` is the tool for that one.

### VACUUM FULL

2.5 s, and the heap goes 382 → 65 MB. It gets the space back because it does not
reclaim anything — it writes a brand new file containing only live rows and
swaps it in.

The cost is in the lock. `ACCESS EXCLUSIVE` for the entire rewrite: no reads,
no writes, not even a `SELECT`, for as long as it takes to copy the table. It
also needs room for a _second complete copy_ on disk while it runs, so the
recovery for a disk that is 85% full will not fit. `pg_repack` is the online
version.

**65 MB, not the original 57 MB**:

- `status` went from `'ok'` to `'pass-reuse-5'`.
- `pg_stats.avg_width` moved from 3 bytes to 13.

### The counter that lies

`n_dead_tup` still reads **5 000 000** in that last row, against a table that
was just rewritten and has none. `VACUUM FULL` does not reset it. The one
column most bloat monitoring is built on goes stale exactly when the bloat is
gone, and only a subsequent plain `VACUUM` zeroes it.

## 2. Why one workload bloats 6x and another 2.2x

```bash
./hot.sh
```

A **HOT** (Heap-Only Tuple) update puts the new row version on the same page as the old one and writes **no index entries at all**. The dead version can then be reclaimed by a `SELECT`, without waiting for a vacuum. Queries use the original index entry, hit the page, and follow a mini-pointer chain to the new row, that exists even if the old tuple is deleted.

Two conditions, both required:

1. No indexed column changed **value**.
2. The new version **fits on the same page** — which is what `fillfactor` buys.

200 000 rows, five full-table passes each:

| what the UPDATE sets    |  ff | heap before | heap after | heap growth | idx growth |   HOT |
| ----------------------- | --: | ----------: | ---------: | ----------: | ---------: | ----: |
| indexed col, new value  | 100 |     10.0 MB |    59.7 MB |       6.00x |      3.57x |  0.0% |
| indexed col, new value  |  70 |     14.3 MB |    39.3 MB |       2.74x |      3.35x |  0.0% |
| indexed col, same value |  70 |     14.3 MB |    31.5 MB |       2.20x |      2.02x | 76.0% |
| unindexed col           | 100 |     10.0 MB |    59.7 MB |       6.00x |      3.73x |  0.0% |
| unindexed col           |  90 |     11.1 MB |    50.2 MB |       4.53x |      2.65x | 29.3% |
| unindexed col           |  70 |     14.3 MB |    31.5 MB |       2.20x |      2.06x | 76.0% |

- **Row 4 is the one that surprises people.** The column is not indexed, and
  HOT is still 0%. At the default `fillfactor = 100` the initial load packs
  every page solid, so there is nowhere on the page to put the new version.
  Unindexed columns do not get you HOT; _free space_ gets you HOT.
- **Rows 3 and 6 are identical** — same size, same HOT rate — and row 3 is
  updating an _indexed_ column. HOT tests whether the indexed **value changed**,
  not whether the column carries an index, so `SET status = status` stays
  eligible.

  This kills a common piece of advice: _"one index on a hot column disables HOT
  for every update on the table."_ It does not. It disables HOT only for the
  updates that actually change that column's value. The practical consequence
  runs the other way from how it is usually told — an ORM issuing
  `UPDATE … SET every_column = …` on every save is fine, as long as the values
  it rewrites are unchanged.

- **`fillfactor` alone is not enough** (row 2): it cut heap growth 6.00x →
  2.74x by giving new versions somewhere local to land, but index growth barely
  moved (3.57x → 3.35x), because a non-HOT update writes index entries wherever
  it puts the row.
- **Absolute, not relative.** `fillfactor = 70` starts 43% bigger (14.3 vs
  10.0 MB) and still finishes at half the size (31.5 vs 59.7 MB). The ratio
  column flatters the tables that started fat; the one that matters is the
  fourth column.

The tradeoff `fillfactor` charges is real: every sequential scan and every
index-only scan reads 30% more pages forever, for a table whose updates might
not be HOT-eligible anyway. It pays on hot, narrow, frequently-updated tables
and nowhere else.

## 3. When autovacuum decides to care

```bash
./autovacuum.sh
```

- Wakes up every 60s (`autovacuum_naptime`) and starts a worker for a database.
- The worker checks each table against a threshold

```bash
threshold = autovacuum_vacuum_threshold (default 50)
          + autovacuum_vacuum_scale_factor (default 0.2)
          * reltuples
```

- `reltuples` is the planner's estimate, so if the statistics are stale it can be wrong.
- capped at `autovacuum_vacuum_max_threshold` (default 100M, new in PG18).
- The dead-tuple trigger is not the only one:
  - `autovacuum_vacuum_insert_*` covers insert-mostly tables.
  - `autovacuum_freeze_max_age` forces an anti-wraparound vacuum eventually no matter what.

## 4. One open transaction, and cleanup stops

```bash
./blocker.sh
```

- `VACUUM` may not remove a dead version anyone might still need to see. "Anyone"
  is the **xmin horizon**: the oldest transaction id any session could still be
  looking at. A single session holding that horizon back freezes cleanup across
  the whole cluster (every table and every database).

The folklore blames `idle in transaction`. All three sessions below are idle in
transaction, and they do not agree:

| the open transaction        | backend_xmin | backend_xid | tuples removed | dead, not removable |
| --------------------------- | -----------: | ----------: | -------------: | ------------------: |
| repeatable read, read-only  |          978 |           - |              0 |             199 999 |
| read committed, read-only   |            - |           - |        199 999 |                   0 |
| read committed, has written |            - |         984 |              0 |             199 999 |

Being idle in a transaction is neither necessary nor sufficient:

- **Row 2 blocks nothing.** A read-committed transaction takes a fresh snapshot
  per _statement_ and drops it when the statement ends, so between statements it
  holds no horizon at all. It can sit there all afternoon. (It still pins a
  connection and holds any locks it took — just not the horizon.)
- **Row 3 blocks everything**, with no snapshot, because it holds an unfinished
  `xid`. This is the shape of the common production bug:
  `BEGIN; UPDATE …; ` → call a payment API → `COMMIT;`.
- **Row 1 blocks everything** via a snapshot it is not using. Nothing here
  requires idleness — a 40-minute analytics `SELECT` in repeatable read holds
  the horizon exactly the same way while working hard.

The rule is: **a session holds the horizon if it has a snapshot (`backend_xmin`)
or an unfinished write (`backend_xid`)**, and `state` is not the thing to filter
on.

### What it looks like while it is happening

Same table, one repeatable-read holder, an update pass and a `VACUUM` between
each line:

```
vacuum 1, holder still open:       0 removed,  399999 not removable, heap  25.3 MB, horizon 2 xids behind
vacuum 2, holder still open:       0 removed,  599998 not removable, heap  33.8 MB, horizon 3 xids behind
vacuum 3, holder still open:       0 removed,  799997 not removable, heap  42.2 MB, horizon 4 xids behind
vacuum 4, holder committed:   799997 removed,       0 not removable, heap  42.2 MB
```

Three vacuums ran. All three succeeded — no error, no warning, nothing in the
log to distinguish them from useful work. All three reclaimed **nothing**,
while the table went 16.9 → 42.2 MB. (The horizon age is 2-4 xids only because
this lab is the sole writer; on a busy cluster the same open transaction shows
up as millions, which is what makes it findable.) Autovacuum in this position behaves identically,
and worse: the dead tuples are over threshold, so it is triggered constantly,
burns I/O scanning the whole table, removes nothing, and immediately qualifies
again.

The moment the holder commits, all 799 997 come back at once — and the file
stays at 42.2 MB forever, because §1 already established that reclaiming is not
shrinking.

`n_dead_tup` does report this honestly, which is worth knowing since the
previous section caught it lying elsewhere — measured directly, it stays at
200 000 across a blocked vacuum and only drops to 0 once the holder commits:

```
before vacuum, blocked:   n_dead_tup=200000
after  vacuum, blocked:   n_dead_tup=200000
after  vacuum, unblocked: n_dead_tup=0
```

**What it cannot tell you is why.** A table sitting at 200 000 dead looks
identical whether autovacuum is blocked by a horizon holder, has not reached
its threshold, or simply has not woken up yet — three problems with three
different fixes. `VACUUM VERBOSE`'s _"N are dead but not yet removable"_
separates the first from the others, and so does the horizon age:

```sql
SELECT pid, state, age(backend_xmin) AS xids_behind, now() - xact_start AS open_for
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL OR backend_xid IS NOT NULL
ORDER BY age(backend_xmin) DESC NULLS LAST;
```

### The holders with no session to find

The script also checks the three that do not appear in `pg_stat_activity` at
all, because there is no session behind them:

```
replication slots:     none
prepared transactions: none
idle_in_transaction_session_timeout = 0 (0 = no limit)
```

- **Replication slots.** An inactive slot pins `xmin` indefinitely. A replica
  that was decommissioned without dropping its slot, or a stalled logical
  decoding consumer, blocks vacuum on the primary forever, with nothing in
  `pg_stat_activity` to show for it. Check `pg_replication_slots`.
- **Prepared transactions.** A two-phase commit that was prepared and never
  resolved holds its xid across server restarts. `pg_prepared_xacts`.
- **`hot_standby_feedback = on`** exports a replica's horizon to the primary,
  so a long query on the replica blocks cleanup upstream.

`idle_in_transaction_session_timeout` is 0 by default — nothing ever gets
killed. Setting it (30 s, a minute) converts the worst version of this from an
unbounded outage into an application error, and is close to free.

## What this costs you

- **Bloat is a high-water mark, not a leak.** `VACUUM` makes space reusable;
  only a rewrite gives it back. A steadily-churned table settling at 2-6x its
  minimum size is working correctly, and `VACUUM FULL` on a schedule is a
  self-inflicted outage.
- **`n_dead_tup` tells you there is garbage, never why it is still there.**
  Blocked, below threshold, and not-yet-woken all look the same in it, and it
  goes stale after `VACUUM FULL` on top of that. Pair it with horizon age and
  `last_autovacuum`.
- **The scale factor is a proportion**, so the biggest, hottest tables are the
  ones stock settings neglect most. It is a per-table setting in practice.
- **Any transaction that holds a snapshot or a write is a cluster-wide brake.**
  Not just a long _query_ — a long _transaction_, including one doing nothing.
- **HOT is worth designing for**, and the lever is _free space_, not index
  avoidance. Room on the page plus updates that leave indexed values alone is
  the difference between 6.00x and 2.20x, and it removes the index write
  amplification entirely.

## Notes

- `pg_stat_user_tables` counters are flushed on a timer, so a read straight after an `UPDATE` can show stale numbers. The scripts call `pg_stat_force_next_flush()` first.
