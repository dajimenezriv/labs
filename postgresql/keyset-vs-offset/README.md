# Keyset vs Offset Pagination

What deep pages actually cost, and why `OFFSET` pages can be wrong even when
they are fast.

```bash
docker compose up --build
```

```bash
./bench.sh
./drift.sh
psql postgres://postgres:postgres@localhost:5555/db -f predicate.sql
```

## The setup

```bash
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

- 5M rows, 702 MB total (444 MB heap, 150 MB index).
- `shared_buffers` of 128 MB, so the table does not fit in cache.
- `created_at` is coarse on purpose: 50 rows share every second.

Both strategies get the same index and produce the same plan shape, an index
scan. Only the addressing changes:

```sql
-- offset: skip n rows of the result as it is now
SELECT * FROM lab.events
ORDER BY created_at DESC, id DESC
LIMIT 20 OFFSET :n;

-- keyset: start after a named row
SELECT * FROM lab.events
WHERE (created_at, id) < (:ts, :id)
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

## Results

Mean over 10 executions with the cache warm, from `pg_stat_statements`.
Buffers are per call.

|     depth | strategy | mean ms | blks hit | blks read |
| --------: | -------- | ------: | -------: | --------: |
|         0 | offset   |   0.088 |        5 |         0 |
|         0 | keyset   |   0.078 |        5 |         0 |
|     1 000 | offset   |   0.344 |       19 |         0 |
|     1 000 | keyset   |   0.109 |        4 |         0 |
|    10 000 | offset   |   2.027 |      156 |         0 |
|    10 000 | keyset   |   0.067 |        4 |         0 |
|   100 000 | offset   |  12.717 |    1 524 |         0 |
|   100 000 | keyset   |   0.113 |        4 |         0 |
| 1 000 000 | offset   |  93.203 |   15 199 |         0 |
| 1 000 000 | keyset   |   0.115 |        4 |         0 |
| 4 000 000 | offset   | 552.352 |        0 |    60 784 |
| 4 000 000 | keyset   |   0.068 |        4 |         0 |

Offset grows linearly with depth — 6 000x from first page to last. Keyset is
flat at ~4 buffers and under 0.12 ms regardless of depth, because 4 buffers is
just the height of the btree plus the leaf.

## Why

The plan at depth 4M, from `explain.txt`:

```sql
-- Since this query doesn't ORDER BY an index it performs a Seq Scan.
EXPLAIN ANALYZE SELECT * FROM lab.events OFFSET 4000000 LIMIT 20;
-- Limit  (actual time=9990.186..9990.259 rows=20.00 loops=1)
--   Buffers: shared hit=4699 read=40757
--   ->  Seq Scan on events  (actual time=0.175..5049.300 rows=4000020.00 loops=1)

EXPLAIN ANALYZE SELECT * FROM lab.events ORDER BY created_at DESC, id DESC OFFSET 4000000 LIMIT 20;
-- Limit  (actual time=10640.262..10640.338 rows=20.00 loops=1)
--   Buffers: shared hit=4931 read=55853 written=4574
--   ->  Index Scan using events_created_at_id_desc on events
--         (actual time=0.079..5600.352 rows=4000020.00 loops=1)
```

- `LIMIT` returns 20 rows. The index scan under it scans 4 000 020. Visits the heap for each one and throws away all but the last 20.

Two second-order effects show up only at the bottom:

- **`blks hit` collapses to 0.** One deep page touches 60 784 blocks;
  `shared_buffers` holds 16 384. A single execution evicts everything the
  previous one cached, so the query can never warm up no matter how often it
  runs. That is the discontinuity between the 1M and 4M rows above — the cost
  stops being just "more work" and becomes "cache-destroying work", for every
  other query on the box too.
- **`written=4574`.** A read-only `SELECT` doing 4 574 writes: evicting dirty
  buffers to make room for its own scan.

## The trap in the measurement

`EXPLAIN ANALYZE` reported 10 654 ms for the query `pg_stat_statements` times
at 552 ms. Same query, 19x apart:

| how                             | 4M offset page |
| ------------------------------- | -------------: |
| `EXPLAIN (ANALYZE, TIMING ON)`  |      10 680 ms |
| `EXPLAIN (ANALYZE, TIMING OFF)` |         630 ms |
| plain execution, mean of 5      |         555 ms |

`ANALYZE` times every row the plan produces. At 20 rows that is free; at 4M
rows the two `clock_gettime` calls per row cost more than the query. The
inflation is proportional to rows _produced_, not rows returned, so it lands
hardest exactly on the plans you are profiling because they are slow.

This is why the numbers above come from `pg_stat_statements` and only the
shape comes from `EXPLAIN`. `TIMING OFF` is the middle ground when you want a
plan and a believable total.

`pg_stat_statements` normalises literals, so all six depths collapse into one
`LIMIT $1 OFFSET $2` row — [bench.sh](keyset-vs-offset/bench.sh) resets before
each measurement to keep them apart.

## Correctness, which timing cannot show

`drift.sh` reads page 1, inserts one row that sorts above it, then reads page 2:

```
offset
  page 1 last:  4999981
  page 2 first: 4999981
  seen twice:   4999981
keyset
  page 1 last:  4999981
  page 2 first: 4999980
  seen twice:   none
```

`OFFSET 20` means "skip 20 rows of the result _as it is now_". One insert above
the page shifts everything down, and the row that ended page 1 begins page 2. A
delete shifts the other way and a row is skipped — never shown, no error. On a
feed with writes this happens constantly; it is not a race you can win by
retrying.

A keyset cursor names a row instead of a position, so rows arriving above it do
not move it.

## What keyset costs you

- **No random access.** `page=500` is unanswerable; you can only go next/prev
  from a cursor. Numbered page links are out, infinite scroll is fine.
- **The sort key must be unique.** `created_at` alone has 50-way ties here, so
  the cursor is `(created_at, id)` — tie-break with something unique or pages
  overlap at tie boundaries, and compare the pair as a pair.
- **The index must match the order exactly**, including direction, or the
  descent degenerates into a filter.
- **Total count is a separate problem**, and an expensive one either way.

## Notes

- `pg_stat_statements` takes two steps, not one: `compose.yaml` loads the
  library at server startup, and the extension is created once _per database_.
  A fresh volume has the first and not the second, so `bench.sh` creates it.
- Teardown: `DROP SCHEMA lab CASCADE;`
