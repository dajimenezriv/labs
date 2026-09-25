# Connection Pooling and Exhaustion

PostgreSQL follows a process-per-connection model, so any new connection opens a OS-level process.

`max_connections` defines the maximum number of concurrent client connections that PostgreSQL will accept:

- Default value is 100.
- More connections isn't better performance:
  - Memory usage: it reserves a portion of memory for each connection. Can exhaust server memory.
  - CPU load: more connections often mean more parallel queries, which can saturate CPU cores.
  - I/O Pressure: simultaneous disk reads/writes from many sessions can strain storage systems.

`superuser_reserved_connections` defines the number of connection slots reserved exclusively for superusers:

- Default value is 3.
- Available connections is `max_connections` - `superuser_reserved_connections`, in our example, 17.

When a connection is not working it can be:

- `idle`: connected but the client hasn't send a query (very common with pools).
- `idle in transaction`: the worst kind, holding locks and snapshots while the app does something else.
- `blocked on I/O`: reading pages that aren't in shared_buffers, waiting on WAL fsync.
- `waiting on locks`: row locks, or internal lightweight locks on shared memory structures.

## The setup

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

```sql
EXPLAIN (ANALYZE, TIMING OFF) SELECT count(*) FROM lab.accounts WHERE balance > 500;
-- Aggregate
--   Buffers: shared hit=319
--   ->  Seq Scan on accounts
--         Filter: (balance > '500'::numeric)
--         Rows Removed by Filter: 25005
--         Buffers: shared hit=319
-- Planning Time: 0.102 ms
-- Execution Time: 13.576 ms
```

- 50000 rows in 319 pages (2.5MB).
- Every page is a `shared hit` and `read=0`, so there is no disk.
- Query execution time is 13.5ms.

## 1. Running out 100 goroutines

```bash
./bench.sh
```

| pool | qps | p50ms | p99ms  | waitms (mean) | ok   | err | codes   |
| ---- | --- | ----- | ------ | ------------- | ---- | --- | ------- |
| 0    | 180 | 294.3 | 2390.8 | 385.8         | 899  | 109 | `53300` |
| 1    | 177 | 442.6 | 1477.3 | 534.3         | 887  | 0   | -       |
| 2    | 452 | 220.6 | 232.7  | 212.1         | 2258 | 0   | -       |
| 4 ✅ | 848 | 117.1 | 131.4  | 111.9         | 4239 | 0   | -       |
| 6    | 846 | 112.8 | 153.3  | 109.5         | 4230 | 0   | -       |
| 8    | 812 | 112.4 | 166.7  | 111.7         | 4059 | 0   | -       |
| 12   | 693 | 117.6 | 186.1  | 125.5         | 3468 | 0   | -       |
| 16   | 628 | 185.7 | 194.1  | 131.0         | 3142 | 0   | -       |
| 50   | 456 | 107.2 | 193.3  | 127.5         | 2283 | 789 | `53300` |
| 100  | 453 | 16.8  | 100.1  | 106.8         | 2264 | 789 | `53300` |
| 150  | 444 | 16.7  | 98.5   | 117.8         | 2222 | 826 | `53300` |

`53300` is `too_many_connections`. Three different mistakes:

- **No pool**: connect, query, close. Mean time inside `Connect` is **385.5 ms**, guarding 13.5 ms of work.
- **pool=4**: can have up to 4 open connections and the others are queued in the app.
- **pool=100**: pgxpool tries to open a 100th connection, Postgres says no, and the error goes straight to the caller. `pgxpool.MaxConns` has to be sized against `max_connections` divided by every process that connects.

**A bigger pool is not a bigger budget**. Four backends already saturate four cores. Connections 5 through 16 add no capacity, only context switching, lock contention and cache pressure between backends fighting over the same cores. Concurrency past the point of saturation is not throughput, it is queueing — and queueing inside Postgres is
strictly worse than queueing in the app, because a waiting backend still holds memory, a snapshot, and a slot you cannot give to anyone else.

The rule of thumb people quote — `cores * 2 + effective_spindles` — is a guess
at where this knee sits for a mixed read/write workload. Here the workload is
pure in-cache CPU with no I/O to overlap, so the knee lands exactly on the core
count.

### Why p50 barely moves

With a fixed 100 callers in flight, Little's law pins the _mean_ latency at
`concurrency / throughput`, whatever the pool does. That is the last column:
109 ms at pool=4, 148 ms at pool=16. The only way to reduce latency in a closed
loop is to raise throughput, and enlarging the pool lowers it.

p50 stays near 110 ms the whole way while the mean rises to 148, which means
the extra 39 ms went entirely into the tail. Watching p50 during a pool-size
change shows nothing. The damage is all at p99.

## 3. pgbouncer in transaction mode

```bash
./bouncer.sh
```

If we have multiple Go services is more difficult to do the calculations with `max_connections` to know the `pgxpool.MaxConns` we shoud allow in each service. We can raise our services `MaxConns` and add a pgbouncer that allows a lot of client connections and just opens some server connections (which are the PostgreSQL processes that are more expensive).

If the sum of your client pools is less than or equal to pgbouncer's server pool, pgbouncer does nothing except add a hop. One Go service with max 10 behind a pgbouncer with pool size 10 is pure overhead.

### What 100 application connections cost

```
idle:                             2 backends
direct, pgxpool MaxConns=16:     18 backends
pgbouncer, pgxpool MaxConns=100:  6 backends
  pgbouncer holds cl_waiting=95 clients over sv_idle=5 server connections
```

That is the entire argument for running one. `pool=100` on the client side is
now harmless, because a pgbouncer client connection is a socket in a single
3 MB process, not a forked Postgres backend. The 95 waiting clients cost
nothing while they wait.

| exec            | qps | p50ms | p99ms | waitms (mean) |   ok | err | codes   |
| --------------- | --: | ----: | ----: | ------------: | ---: | --: | ------- |
| cache_statement | 880 | 109.3 | 141.1 |           0.6 | 4410 |   0 | -       |
| cache_describe  | 894 | 108.0 | 135.7 |           0.6 | 4476 |   0 | -       |
| describe_exec   | 861 | 112.2 | 140.0 |           0.9 | 4309 |  55 | `26000` |
| exec            | 881 | 110.3 | 138.9 |           0.6 | 4408 |   0 | -       |
| simple          | 906 | 107.4 | 135.0 |           0.6 | 4531 |   0 | -       |

### The scar that expired

**pgx's default mode works, and every mode performs the same.** For years the
first row was the famous one: `cache_statement` issues `PARSE stmtcache_1` once
and `BIND stmtcache_1` forever after, a named prepared statement is per-session
state, and in transaction mode the session is not yours between transactions —
so the BIND arrived at a backend that never saw the PARSE
(`26000 invalid_sql_statement_name`) or the PARSE collided with another
client's name (`42P05 duplicate_prepared_statement`).

pgbouncer 1.21 (2023) fixed it: `max_prepared_statements` makes pgbouncer track
the names itself and replay the PARSE onto whichever server connection the
transaction lands on. A later release turned it on by default. On 1.25:

```
 key                     | value | default
 max_prepared_statements | 200   | 200
```

So the advice this scar produced — "disable your statement cache before you put
pgbouncer in front" — is now obsolete, and the modes it recommends are not
faster. There is no longer a reason to touch `DefaultQueryExecMode`.

### The one that did not expire

`describe_exec` still fails, **intermittently**, with the old `26000`. Its
Describe and its Execute are two separate protocol exchanges outside a
transaction, so pgbouncer is free to move the server connection between them —
and unnamed statements have no name for pgbouncer to track, so
`max_prepared_statements` cannot help. Across ten runs the rate ranged from
**0.04% to 1.8%** and was never zero:

```
run1  4814 ok  18 err      run4  4894 ok  86 err
run2  4839 ok   2 err      run5  4897 ok  32 err
run3  4873 ok  45 err
```

The trap is the shape of it. Someone hits no problem at all, then reads the old
folklore, goes to disable the statement cache, picks a mode off the list — and
lands on the one that is _worse than the default_. Loudly broken gets fixed on
day one. This gets a retry wrapped around it and ships.

### pgbouncer beat the hand-tuned pool

1 001 qps through pgbouncer against 914 qps for the best direct pool size, on
5 backends instead of 16, with 100 client connections instead of 4. Acquire
wait fell from 105 ms to **0.6 ms**, because the client pool stopped being the
scarce resource.

The lesson is not that pgbouncer is faster. It is that the scarce thing is
_backends_, and once something else owns that number, `MaxConns` stops being a
capacity decision.

## 4. What transaction mode still breaks

Prepared statements were the loud failure, and they got fixed. A pooled server
connection is still handed to the next transaction in whatever state the last
one left it, and the rest of that state fails quietly — with no setting to
turn on:

```
advisory locks before anything:                    none
after a direct client took 43 and disconnected:    none
after a pgbouncer client took 42 and disconnected: 42

an unrelated later client through pgbouncer sees work_mem = 64MB
a client on its own backend sees work_mem                 = 4MB
```

- **Session-level advisory locks leak.** `pg_advisory_lock(42)` outlives the
  client that took it, because the server connection it was taken on goes back
  into the pool still holding it. Nothing releases it. Direct to Postgres the
  lock dies with the backend, which is exactly the behaviour the API implies.
  `pg_advisory_xact_lock` is the version that is safe here.
- **`SET` escapes to strangers.** One client sets `work_mem = '64MB'` and an
  unrelated later client inherits it. `SET LOCAL` inside a transaction is the
  safe form.
- Same story for `LISTEN`/`NOTIFY`, session temp tables, `SET ROLE`, and
  `WITH HOLD` cursors. None of them error. They just apply to the wrong client.

## What pooling costs you

- **A queue you cannot see in p50.** Everything above p50 is acquire wait, and
  at pool=4 that was 105 ms of a 109 ms request. Instrument acquire time
  separately or you will spend the incident looking at the query.
- **Transaction mode is not a drop-in.** It buys the connection count back and
  charges you every piece of session state your code assumed it owned.
