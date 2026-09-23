# Reprocessing

- [Replayable queue](#replayable-queue)
- [The setup](#the-setup)
- [1. Blocking retry vs retry topics](#1-blocking-retry-vs-retry-topics)
- [2. Merging the DLQ back](#2-merging-the-dlq-back)
- [3. Rewinding a consumer group](#3-rewinding-a-consumer-group)
- [4. Retention is the replay window](#4-retention-is-the-replay-window)

```bash
docker compose up -d
./pipeline.sh     # section 1
./deadletter.sh   # section 2
./replay.sh       # section 3
./retention.sh    # section 4
```

Following Uber's [Building Reliable Reprocessing and Dead Letter Queues with Apache Kafka](https://www.uber.com/us/en/blog/reliable-reprocessing/).

## Replayable queue

- **Classic queue (RabbitMQ, Amazon SQS)**: reading is destructive. Ack a message and it is gone.
- **Kafka**: reading deletes nothing. Records stay until `retention.ms` (default 7 days). A consumer group's position is just an offset committed into `__consumer_offsets`. Replay = move that offset back and read again.
- Two ways to replay:
  - **Rewind the group** (section 3): everything from a point in time, good records included.
  - **Merge the DLQ** (section 2): only the failures. It is a parking lot until the fix ships, not a graveyard.
- Both are **redelivery**, so both need an idempotent consumer, and both are bounded by retention (section 4).

## The setup

- One broker.
- Topics: `readings` plus `readings.retry.1..3` and `readings.dlq`, **3 partitions** each. Every tier is its own topic _and its own consumer group_ (`<group>.retry.N`), so a retry backlog cannot slow live traffic.
- **6000 records at 200/s** (30s of traffic) over **30 keys**. Each value carries `id,seq,produced_ms`: `seq` is the record's position within its key, so the sink can tell when a key's events were handled out of order.
- Handler: **1ms** per successful call. It writes one line per success to a sink file, which `verify` scores.
- Retry ladder **1s, 2s, 4s**. Scaled down from the minutes a production ladder would use, so a record reaches the DLQ within a run. The blocking consumer backs off on the same ladder, so the two modes only differ in _where_ the wait happens.
- Failures are chosen by id, not randomly, so every variant fails the same records:
  - **flaky**: every 500th id (12 records) fails twice with a 503, then works.
  - **poison**: id 3000 always fails with a _retryable_ 500. Nothing tells the consumer it will never work.
  - **bug**: every 100th id fails with `errNonRetryable` (unparseable field).
  - **bad**: ids 2001..3000 succeed but write a wrong result.

## 1. Blocking retry vs retry topics

```bash
./pipeline.sh
```

| variant                  | delivered | dlq | dup | drain | p50 first try | p99 first try | p50 retried | out of order | stale keys |
| ------------------------ | --------: | --: | --: | ----: | ------------: | ------------: | ----------: | -----------: | ---------: |
| blocking, flaky          | 6000/6000 |   0 |   0 | 45.0s |          5.7s |         18.1s |        7.1s |            0 |          0 |
| tiered, flaky            | 6000/6000 |   0 |   0 | 33.0s |          10ms |          16ms |        3.0s |           11 |          1 |
| blocking, flaky + poison | 3632/6000 |   0 |   0 | 24.2s |          3.6s |         10.5s |        5.4s |            0 |         30 |
| tiered, flaky + poison   | 5999/6000 |   1 |   0 | 33.0s |          10ms |          16ms |        3.0s |           10 |          1 |

- `first try`: latency of records that never failed. `stale keys`: keys whose last write is not their latest event, which is what a last-write-wins table ends up holding.
- **Blocking**: 12 flaky records × (1s + 2s) = 36s of stall on 30s of traffic. The median _healthy_ record waited **5.7s** behind someone else's retry. The stall hits all 3 partitions, not just the failing one, because one poll loop owns them all.
- **Tiered**: healthy records are untouched (**10ms** p50). Retried records land at **3.0s**, exactly 1s + 2s: the tier delay is a floor ("at least the configured timeout, possibly later" in Uber's words).
- **The price is ordering**: 11 of the 12 retried records were handled after a newer record of their key, and 1 key finished stale. Id 5500 (k09, seq 184) retried after k09's last event (seq 200) was already written. The 12th, id 6000, is its key's last event, so nothing overtook it.
- **Poison pill, blocking**: stuck at id 3000 forever (the 90s deadline ended it) with **30/30 keys stale** and lag growing without bound. The 632 records past 3000 came from other partitions in the batch already fetched before the stall.
- **Poison pill, tiered**: walks 1s → 2s → 4s and lands in the DLQ. Everything else is delivered.
- **Not committing is not a retry.** franz-go's (and Java's) fetch position has already moved past the record, so the next poll returns the records _after_ it. The failed one only comes back after a restart or rebalance, from the last commit. Blocking has to be an explicit loop.
- Java: sleeping in the poll loop counts against `max.poll.interval.ms` (default 300000). A tier delay longer than that gets the member evicted and triggers a rebalance. franz-go heartbeats and tracks liveness on its own goroutines, so here the sleep is harmless.

**Retry topics take the stall off healthy traffic and put it on ordering. Only a consumer that can live with out-of-order events per key can use them.**

## 2. Merging the DLQ back

```bash
./deadletter.sh
```

A buggy deploy cannot parse 1 in 100 records (`errNonRetryable`). The fix ships under the same consumer group, and `dlq merge` republishes the DLQ onto `readings.retry.1`:

```
1/0 key=k29 value=300,10,1790172234105 error=non-retryable: unknown field "unit"
    first_failed_at=2026-09-23T14:03:54Z original_offset=109 original_partition=1
    original_topic=readings retry_count=1
```

| moment                 | delivered | dlq | dup | out of order | stale keys |
| ---------------------- | --------: | --: | --: | -----------: | ---------: |
| buggy deploy           | 5940/6000 |  60 |   0 |            0 |          1 |
| fixed deploy + merge   | 6000/6000 |  60 |   0 |           59 |          2 |
| second merge, no purge | 6000/6000 |  60 |  60 |          118 |          2 |

- `retry_count=1` on a record that never touched a retry topic: non-retryable errors skip the ladder. A parse error in 2s is the same parse error.
- The headers are what separate topics buy over retrying in place: where the record came in (`original_*`), since when (`first_failed_at`), and why (`error`).
- **Merge replays 60 records, not 6000.** Only the failures go back, and through `retry.1`, so they never compete with live traffic.
- **A merge writes old events over new state.** 59 of the 60 arrive after newer records of their key, and 2 keys end stale. Fix in the sink, not the pipeline: `UPDATE ... WHERE incoming.seq > stored.seq` (or `updated_at`).
- **Merge does not consume.** It reads the DLQ with no group and commits nothing. Merge twice and you get **60 duplicates**. The sequence is merge, check, `dlq purge` (`DeleteRecords` up to the current end offsets, which moves the log start offset).

## 3. Rewinding a consumer group

```bash
./replay.sh
```

A deploy writes wrong results for ids 2001..3000 and raises no errors. Once the fix is out, the group is rewound with the stock CLI to when the first bad record was produced:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group g-24470 --topic readings \
  --reset-offsets --to-datetime 2026-09-23T14:04:38.235 --execute
```

```
GROUP     TOPIC     PARTITION  NEW-OFFSET
g-24470   readings  2          667
g-24470   readings  1          733
g-24470   readings  0          600
```

| moment        | sink            |  rows | distinct ids | duplicate rows | wrong rows |
| ------------- | --------------- | ----: | -----------: | -------------: | ---------: |
| before replay | append (INSERT) |  6000 |         6000 |              0 |       1000 |
| before replay | upsert by id    |  6000 |         6000 |              0 |       1000 |
| after replay  | append (INSERT) | 10000 |         6000 |           4000 |       1000 |
| after replay  | upsert by id    |  6000 |         6000 |              0 |          0 |

- **4000 records replayed to fix 1000.** A rewind is a point in time, not a set: everything after it comes back, including the 3000 records that were fine.
- `--to-datetime` is resolved per partition via `ListOffsets` by timestamp (the time index): the first offset whose timestamp is ≥ T. The three partitions land on different offsets for the same instant.
- **Append-only sink**: 4000 duplicate rows, _and the 1000 wrong rows are still there_ next to the corrected ones. **Upsert by id**: 0 wrong, 0 duplicates. Replay only fixes an idempotent sink.
- Without `--execute` it is a dry run that prints the plan and changes nothing. The group must have no live members, or the broker refuses the reset. Stop the consumers first.
- Other reset modes: `--to-earliest`, `--to-latest`, `--to-offset`, `--shift-by -N`, `--by-duration PT2H`.
- Side effects outside the sink (emails, payments, webhooks) replay too. Idempotency has to cover them, or they must be skipped during a replay.

## 4. Retention is the replay window

```bash
./retention.sh
```

`retention.ms` is scaled from 7 days to **20s**, and the broker's `log.retention.check.interval.ms` from 5 min to 5s. `segment.ms` stays at its 7-day default. 1000 records are produced and consumed, then:

| seconds since produce | log start offset | log end offset |
| --------------------: | ---------------: | -------------: |
|                     6 |                0 |           1000 |
|                    11 |                0 |           1000 |
|                    16 |                0 |           1000 |
|                    21 |             1000 |           1000 |
|                    26 |             1000 |           1000 |
|                    32 |             1000 |           1000 |
|                    37 |             1000 |           1000 |
|                    42 |             1000 |           1000 |

Then the group is rewound to before the first record:

```
Warn: Partition 0 from topic readings is empty. Falling back to latest known offset.
consume: blocking handled=0
```

- Gone between 16s and 21s, even though the records sat in the only (active) segment and `segment.ms` is 7 days. When retention would delete every segment, the broker rolls a fresh empty active segment first, so an idle topic still expires.
- Deletion is per segment and happens on a sweep, so `retention.ms` is a **minimum**. A record can outlive it by up to one segment's worth of time (`segment.bytes` 1 GiB, `segment.ms` 7 days) plus a sweep interval.
- **Rewinding past the log start is a warning, not an error**: exit code 0, and the replay silently handles 0 records. A replay script has to compare the log start offset to where it expected to begin.
- **A replay can only reach back as far as `retention.ms`.** Past that you need tiered storage (KIP-405, `remote.storage.enable`) or an archive to object storage that you replay from.
