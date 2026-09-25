# PostgreSQL

- DDL (Data Definition Language): `CREATE`, `ALTER`, `DROP`, `TRUNCATE`.
- DML (Data Manipulation Language): `INSERT`, `UPDATE`, `DELETE`, `SELECT`.

## MVCC (Multi-Version Concurrency Control)

### The physical thing

Postgres stores rows in 8KB (8192 bytes) heap pages:

- Page header.
- Array of 4-byte line pointers growing forward.
- Free space.
- Tuples growing backward from the end. Each tuple carries a header, some of its fields are:
  - `xmin`: the transaction ID (`xid`) that inserted this version. Practically everything is wrapped inside a transaction.
  - `xmax`: `xid` that deleted or locked it (0 if neither).
  - `ctid`: `(page, line_pointer)` pointing at the next of this row; self-referencing when it's the latest.
  - bitmap of hint bits.

```bash
psql "postgres://postgres:postgres@localhost:5432/postgres"
```

```sql
CREATE DATABASE lab;
\c lab

CREATE EXTENSION pageinspect; -- needs superuser
CREATE TABLE t (id int PRIMARY KEY, v text);

INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t (id, v) VALUES (4, 'd');
UPDATE t SET v = 'z' WHERE id = 2; -- delete plus insert.
DELETE FROM t WHERE id = 3;

-- lp = line_pointer
SELECT lp, t_xmin, t_xmax, t_ctid FROM heap_page_items(get_raw_page('t', 0));

\c postgres
DROP DATABASE lab;
```

| lp  | t_xmin | t_xmax | t_ctid | Explanation                          |
| --- | ------ | ------ | ------ | ------------------------------------ |
| 1   | 14363  | 0      | (0,1)  | `INSERT`                             |
| 2   | 14363  | 14365  | (0,5)  | `UPDATE`, points to the new one (5). |
| 3   | 14363  | 14366  | (0,3)  | `DELETE`, points to himself.         |
| 4   | 14364  | 0      | (0,4)  | `INSERT`, Another `xmin` (`xid`).    |
| 5   | 14365  | 0      | (0,5)  | `UPDATE`, Another `xmin` (`xid`).    |

`SELECT ... FOR UPDATE` also writes `xmax`, but with a lock-only flag so it isn't read as a deletion.

### Snapshots and the visibility rule

A snapshot is three things: the lowest still-running `xid`, the first not-yet-assigned `xid`, and the list of in-progress `xid`s in between.

```sql
-- Session A
SELECT pg_current_snapshot(); -- 14367:14367:

-- Session B
BEGIN;
INSERT INTO t VALUES (100, 'x');
SELECT pg_current_xact_id(); -- 14367

-- Session C
BEGIN;
INSERT INTO t VALUES (101, 'y');
SELECT pg_current_xact_id(); -- 14368

-- Session A
SELECT pg_current_snapshot(); -- 14367:14367:
SELECT pg_current_xact_id(); -- 14369 (if we don't commit then we are not going to see in-flight transactions greater than our own `xid`, why? It seems that this has to be executed inside a transaction, so if we don't wrap this in BEGIN it automatically creates a transaction)
SELECT pg_current_snapshot(); -- 14367:14370:14367,14368
-- 14367 lowest still-running xid
-- 14370 first not-yet-assigned xid
-- 14367,14368 in-progress xids in between
```

For each tuple:

- `xmin` is commited and not in-flight per my snapshot → visible.
- `xmax` is set, commited and not in-flight per my snapshot → invisible.

To answer "did xid 5231 commit?", Postgres reads the commit log (`pg_xact`). That's expensive to do per row per scan, so the answer gets cached back into the tuple's hint bits by the first reader that touches it. This is why a `SELECT` immediately after a large bulk load can be surprisingly slow and can dirty pages it never wrote to. It's a nice detail to have ready because most candidates don't know reads can generate writes.

```sql
CREATE TABLE hb (id int, v text);
INSERT INTO hb SELECT g, repeat('x', 100) FROM generate_series(1, 500000) g;
CHECKPOINT;

EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM hb;
-- Buffers: shared hit=8621 dirtied=8621

EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM hb;
-- Buffers: shared hit=8621
```

Isolation levels are just snapshots policies on top of the same mechanism:

- **Read Commited** (default): a fresh snapshot per statement. Two identical `SELECT`s in one transaction can return different results.
- **Repeatable Read**: one snapshot for the whole transaction. If you update a row someone else has updated since your snapshot, you can't silently proceed, so you get `40001 could not serialize access` due to concurrent update.
- **Serializble**: Repeatable Read plus SSI predicate locking, detecting read-write antidependency cycles. Can abort even read-only transactions. I don't get this, maybe an example too.
- **Read Uncommited**: same as **Read Commited**. There is no dirty read level. SQL standard defines four levels and it just exists so an application written for another database won't fail to parse.

```sql
t0  T1: BEGIN; UPDATE t SET v='b' WHERE id=1; -- stamps xmax=T1, holds the row lock
t1  T2: UPDATE t SET v='c' WHERE id=1;        -- finds xmax=T1, sees T1 in progress, BLOCKS
t2  T1: COMMIT;                               -- releases
t3  T2: wakes up and must decide what to do
```

At `t3`, the isolation level decides:

- **Read Committed** → EvalPlanQual. Follow t_ctid forward to the newest version, re-check the WHERE clause against it. Still matches → update it. No longer matches → silently skip the row. Row was deleted → skip.
- **Repeatable Read / Serializable** → `40001 could not serialize access`.

Postgres has no dirty reads at any level, and its Repeatable Read already prevents phantom reads, which is stronger than the SQL standard requires. What are phantom reads?

### What an `UPDATE`/dead tuples actually cost

Bloat is the obvious cost.

- **Index amplification**: a normal `UPDATE` inserts a new index entry in index on the table, even for indexes on columns that you didn't touch. Indexes bloat faster than the heap and don't shrink on their own. Example?

- **HOT (Heap-Only-Table) updates**: if you update only columns that no index references and there's free space on the same page, Postgres writes the new version on that page and chains it from the old one via `ctid` (I assume that this chain is always done right?), with no indexes writes at all. This is the single biggest lever on write-heavy tables.

Two practical levers:

- Don't index columns you update frequently — one index on a hot column disables HOT for every update.
- Lower `fillfactor` (85–90) on update-heavy tables so pages keep room for new versions. Default is 100 for heap tables, which guarantees HOT fails as soon as a page is full. Don't get this.

```sql
-- I don't know what I'm reading.
SELECT relname, n_tup_upd, n_tup_hot_upd
FROM pg_stat_user_tables ORDER BY n_tup_upd DESC;
```

A bonus: HOT chains get pruned opportunistically. Any page access, including a `SELECT`, can clean up dead intermediate versions and free space in-page without waiting for VACUUM.

A normal update writes a new heap tuple and a new entry on every index on the table (I don't get this second sentence), including indexes on columns you didn't touch, because the new version sits at a new `ctid`. Ten indexes means ten index writes plus WAL for all of it. I don't get this.

**HOT** (Heap-Only-Table) is the escape hatch. If no indexed column changed and the new version fits on the same page, Postgres skips all index writes. The existing index entry keeps pointing at the original line pointer, which becomes a redirect, and the chain is followed within the page. Not very sure about this.


### Why bloat happens: the xmin horizon

What means bloat?

A dead version can only be removed when no snapshot could still need it. That bar is the global xmin horizon, and it's held back by:

- long-running transactions, especially, sessions sitting iddle in transactions (what are these?).
- replication slots, particularly inactive ones, and standbys with `hot_standby_feedback = on`. I don't know what's this.
- orphaned prepared transaction.

```sql
-- Give me an example of this.
SELECT * FROM pg_prepared_xacts;
```

One `psql` window someone left open with `BEGIN;` two days ago will stop vacuum from cleaning any table in the cluster. Then, even after the cleanup, the space is only reusable, it's never given backend to the OS (why???). Can you give me an example of how to reproduce this?

```sql
SELECT pid, state, backend_xmin, now() - xact_start AS age, query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL ORDER BY age DESC;
-- pid=16472 state=active backend_xmin=812 age=00:00:00
-- What I'm reading?
```

And set idle_in_transaction_session_timeout in production. Always. How can I check the current value of this?

### What VACUUM actually does

1. Scans heap pages, skipping all-visible ones via the visibility map, collecting dead line pointers. What is the visibility map?
2. Scans each index, removing entries that point at those TIDs. What are TIDs?
3. Second heap pass: mark line pointers unused, defragments the pages.
4. Updates the free space map and visibility map.
5. Freezes old transaction IDs so the database never hits transaction ID wraparound. It happens when the transaction ID counter reaches 4-billion, transactions remain unvacuuned past 2 billion and the database cannot determine row visibility.
6. Truncates empty pages at the very end of the file, which briefly needs an exclusive lock.

It doesn't:

- Return space to the OS in general. Why not?
- Rebuild indexes. So what happens with old indexes from old tuples?
- Update planner statistics. This is what `ANALYZE` does.

`VACUUM FULL` is a different beast: it rewrites the table into a new file, takes `ACCESS EXCLUSIVE` for the duration, and needs roughly double the disk space.

Run a manual `VACUUM ANALYZE` after bulk loads or big batch deletes, since you know the table just changed a lot and don't need to wait for the threshold.

### Autovacuum

The default trigger is `dead_tuples > 50 + 0.2 × n_live_tup`. That 20% is fine for a 10k-row table and absurd for a 50M-row table, where it means waiting for 10M dead rows. Runs `VACUUM` + `ANALYZE`;

## Table-level locks

- `ACCESS SHARE`: `SELECT`. Conflicts only with `ACCESS EXCLUSIVE`.
- `ROW SHARE`: `SELECT ... FOR UPDATE / FOR SHARE`. This is what we used with the outbox pattern.

```sql
-- SKIP LOCKED lets a second relay take the next batch rather than block on this one, so the relay can be scaled out without coordination, and the lock is what stops two relays publishing the same row.
SELECT * FROM outbox LIMIT 100 FOR UPDATE SKIP LOCKED;
```

- `ROW EXCLUSIVE`: `INSERT`, `UPDATE`, `DELETE`, `MERGE`. It doesn't conflict with itself: two writers on the same table are fine at table level, they only fight on row level.
- `SHARE UPDATE EXCLUSIVE`: `VACUUM` (plain), `ANALYZE`, `CREATE INDEX CONCURRENTLY`, `REINDEX CONCURRENTLY`, `ALTER TABLE VALIDATE CONSTRAINT`. This is the important one for migrations: it allows reads and writes to continue. It conflicts with itself, so you can't run two of these on the same table at once.
- `SHARE`: `CREATE INDEX` (non-concurrent). Blocks writes, allows reads.
- `SHARE ROW EXCLUSIVE`: `CREATE TRIGGER`, some `ALTER TABLE` forms. Blocks concurrent writes (why do you mean by concurrent writes??), allows reads.

```sql
CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW
  EXECUTE FUNCTION set_updated_at();

-- Takes SHARE ROW EXCLUSEIVE on order_items and orders.
ALTER TABLE order_items
  ADD CONSTRAINT order_items_order_id_fkey
  FOREIGN KEY (order_id) REFERENCES orders (id);

-- The standard migration:
ALTER TABLE order_items
  ADD CONSTRAINT order_items_order_id_fkey
  FOREIGN KEY (order_id) REFERENCES orders (id) NOT VALID;

-- In a separate transaction:
ALTER TABLE order_items VALIDATE CONSTRAINT order_items_order_id_fkey;
```

- `EXCLUSIVE`: `REFRESHED MATERIALIZED VIEW CONCURRENTLY`. Blocks everything except plain `SELECT`. (why do you mean by plain SELECT, are other SELECTs).
- `ACCESS EXCLUSIVE`: most `ALTER TABLE`, `DROP TABLE`, `TRUNCATE`, `REINDEX`, `CLUSTER`, `VACUUM FULL`, non-concurrent `REFRESH MATERIALIZED VIEW`, and bare `LOCK TABLE`.

The practical shape of the matrix, rather than memorizing 64 cells:

- ACCESS SHARE (reads) only ever loses to ACCESS EXCLUSIVE. Reads are blocked by DDL and nothing else.
- ROW EXCLUSIVE (writes) loses to SHARE and everything above it. So an index build blocks writes; a vacuum does not.
- The four modes at the bottom of the list are all self-conflicting, which is why you can't parallelize them on one table.

A migration wrapped in a transaction holds its ACCESS EXCLUSIVE lock for the whole transaction, not just the ALTER.

And a lock request queues behind the requests ahead of it. If your ALTER is waiting for a long-running SELECT to finish, every SELECT that arrives after your ALTER now waits too, because it can't jump the queue past a conflicting pending request. That's how an instant DDL statement takes a table down for minutes. Hence SET lock_timeout = '3s' before migrations: fail fast and retry rather than build a queue.

## Zero downtime migrations

- Don't rename columns/tables which are use by the app - always copy the data and drop the old one once the app is no longer using it.
- Don't rewrite a table while you have an exclusive lock on it (e.g. no ALTER TABLE foos ADD COLUMN bar varchar DEFAULT 'baz' NOT NULL).
- Don't perform expensive, synchronous actions while holding an exclusive lock (e.g. adding an index without the CONCURRENTLY flag).

## Why a migration can fail

### The SQL itself is invalid

- Syntax errors, or referencing a column/table/type that doesn't exist.

### Existing data that violates the new constraint

- Adding `NOT NULL` when rows have NULLs, or without a default on an old Postgres version. What do you mean with the second case? Example?
- Adding a `UNIQUE` constraint when duplicates exist.
- Adding a foreign key when orphan rows exist.
- `ALTER COLUMN ... TYPE` when values can't be cast (`text` → `int`)

### Locks and timeouts

This is the classic production failure. Most `ALTER TABLE` variants take an `ACCESS EXCLUSIVE` lock, which blocks every read and write on that table. If a long-running query or an idle-in-transaction session holds a conflicting lock, your migration waits behind it — and everything else queues behind your migration. Then `lock_timeout` or `statement_timeout` kills it, or your deploy times out.

### Migration tool state

- A previously failed migration left the schema_migrations table marked as dirty.
- Two instances of the app starting simultaneously and both trying to migrate — without an advisory lock, they race.
- Checksum mismatch because someone edited an already-applied migration file.
