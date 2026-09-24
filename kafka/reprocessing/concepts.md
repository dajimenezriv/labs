# Reprocessing: concepts

- [Replayable queue](#replayable-queue)
- [The setup](#the-setup)
- [1. Why a failed record is a problem at all](#1-why-a-failed-record-is-a-problem-at-all)
- [2. Uber's retry topics](#2-ubers-retry-topics)
	- [Expected results](#expected-results)
- [3. The price: ordering](#3-the-price-ordering)
	- [The fix lives in the sink](#the-fix-lives-in-the-sink)
	- [Expected results](#expected-results-1)
- [4. The DLQ and merging it back](#4-the-dlq-and-merging-it-back)
	- [Reading the whole DLQ: `drain`](#reading-the-whole-dlq-drain)
	- [`list`](#list)
	- [`merge`](#merge)
	- [`purge`](#purge)
	- [Expected results](#expected-results-2)
- [5. Rewinding a consumer group](#5-rewinding-a-consumer-group)
- [6. Retention is the replay window](#6-retention-is-the-replay-window)
- [Interview answers](#interview-answers)

Based on Uber's [Building Reliable Reprocessing and Dead Letter Queues with Apache Kafka](https://www.uber.com/us/en/blog/reliable-reprocessing/).

## Replayable queue

|                      | Classic queue (RabbitMQ, Amazon SQS) | Kafka                                          |
| -------------------- | ------------------------------------ | ---------------------------------------------- |
| Reading a message    | removes it (on ack)                  | removes nothing                                |
| Consumer position    | the broker tracks each message       | one offset per partition, per group            |
| What "done" means    | ack → deleted                        | commit → offset stored in `__consumer_offsets` |
| When data disappears | on ack                               | on `retention.ms` (default 7 days)             |
| Replay               | impossible unless you kept a copy    | move the offset back                           |

- In a classic queue, the DLQ is the only survivor of a failure. In Kafka, the DLQ is just another topic, so it is replayable too.
- Two ways to replay: **rewind the group** (everything since a point in time) or **merge the DLQ** (only the failures).
- Both are redelivery. Both need an idempotent consumer, and both are bounded by retention.

## The setup

- `readings` topic, 3 partitions. Retry topics `readings.retry.1..3` and `readings.dlq`.
- **6000 records at 200/s** (30s of traffic) over **30 keys**, so each key gets 200 events (`seq` 1..200).
- Handler: 1ms per call.
- Retry ladder: **1s, 2s, 4s** (production would use minutes).
- Failures:
  - **flaky**: every 500th id (**12 records**) fails twice, then works. Cost per record: 1s + 2s = **3s**.
  - **poison**: id 3000 always fails, retryably. Nothing tells the consumer it never will work.

## 1. Why a failed record is a problem at all

One record that won't process blocks everything behind it: **head-of-line blocking**.

The options, and why most don't work:

**Don't commit and hope it comes back.** It won't, because a consumer keeps **two positions** per partition:

|                      | where it lives               | what moves it                              | who reads it                                         |
| -------------------- | ---------------------------- | ------------------------------------------ | ---------------------------------------------------- |
| **fetch position**   | client memory                | every poll that returns records, or a seek | the next poll                                        |
| **committed offset** | broker, `__consumer_offsets` | only a commit (manual or autocommit)       | only a newly assigned partition (startup, rebalance) |

A poll always continues from the fetch position. The committed offset is never read while the consumer runs; it is a bookmark for whoever gets the partition next.

**Retry in place.** It works, but everything behind the record waits, on every partition the poll loop owns:

```go
func retryInPlace(ctx context.Context, rec *kgo.Record, handle handler) bool {
	for attempt := 1; ; attempt++ {
		if handle(rec, attempt) == nil {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(retryDelays[min(attempt, len(retryDelays))-1]):
		}
	}
}
```

- In Java, that sleep counts against `max.poll.interval.ms` (default 300000 ms = 5 min). Go past it and the member is evicted, the group rebalances, and the poisoned partition is handed to someone else to get stuck on.
- `pause()`/`resume()` avoids the eviction (keep polling, get nothing), but the partition is still blocked.

**Move the failure somewhere else.** That is Uber's design.

## 2. Uber's retry topics

```
readings ──fail──> readings.retry.1 ──> readings.retry.2 ──> readings.retry.3 ──> readings.dlq
                         1s                   2s                   4s
```

- The consumer publishes the failed record to the next topic, then **commits the original**. The partition moves on.
- Each tier is its own topic **and its own consumer group**, so a retry backlog never slows live traffic.

The routing decision:

```go
count := retryCount(rec) + 1
destination := retryTopic(count)
if count > len(retryDelays) || errors.Is(err, errNonRetryable) {
	destination = dlqTopic
}
producer.ProduceSync(ctx, &kgo.Record{
	Topic:   destination,
	Key:     rec.Key, // same key → same partition on every tier
	Value:   rec.Value,
	Headers: routeHeaders(rec, count, err),
})
```

- `ProduceSync` **before** the commit. The other order loses the record if the process dies in between.
- `errNonRetryable` (a parse error, a bug) skips the ladder.

**Kafka has no delayed delivery**, so a tier's delay is the consumer sleeping until the record is due:

```go
if c.delay > 0 {
	time.Sleep(time.Until(rec.Timestamp.Add(c.delay)))
}
```

- The timestamp is when the record was written _to this tier_. Records in a partition are in publish order, so the next one is never due before this one, and sleeping doesn't reorder anything.
- So the delay is a **floor**: "at least the configured timeout, possibly later" (Uber).

**Headers** are the big gain over retrying in place: the record carries its own history.

| header                                                      | why                                                                     |
| ----------------------------------------------------------- | ----------------------------------------------------------------------- |
| `retry_count`                                               | which tier it's on, so where it goes next                               |
| `original_topic` / `original_partition` / `original_offset` | where it came in; by the DLQ, the topic it arrived from is a retry tier |
| `first_failed_at`                                           | how long it has been stuck is a subtraction                             |
| `error`                                                     | why it failed the last time                                             |

### Expected results

| variant                  | delivered | dlq | drain | p50 first try | p50 retried |
| ------------------------ | --------: | --: | ----: | ------------: | ----------: |
| blocking, flaky          | 6000/6000 |   0 |  ~42s |           ~5s |         ~7s |
| tiered, flaky            | 6000/6000 |   0 |  ~33s |         ~10ms |          3s |
| blocking, flaky + poison |     ~3000 |   0 |     ∞ |           ~5s |         ~7s |
| tiered, flaky + poison   | 5999/6000 |   1 |  ~33s |         ~10ms |          3s |

- **Blocking, flaky**: 12 × 3s = **36s of stall**, on top of 6000 × 1ms = 6s of work. That's ~42s of consumer time for 30s of traffic, so it falls behind and healthy records wait **seconds** behind someone else's retry.
- **Tiered, flaky**: healthy records are untouched. A retried record lands at exactly 1s + 2s = **3s**. Drain is 30s of traffic plus the last retry's 3s.
- **Blocking, poison**: stuck at id 3000 forever. Lag grows without bound. Only a lag alert notices.
- **Tiered, poison**: 1s → 2s → 4s = **7s** later, it's in the DLQ. Exactly **1 dead letter**; everything else is delivered.

## 3. The price: ordering

A retried record arrives after the records behind it. Per-key order, which is the whole reason for choosing the partition key, is gone for anything that fails.

Two different measures of the damage:

- **Out of order**: a record handled after a newer record of its key. Temporary: the next event overwrites it.
- **Stale key**: the key's _final_ write is not its latest event. Permanent: nothing newer comes to fix it.

```
k09, seq 50 retried (heals):   … 49, 51, … 60, 50, 61 … 200   → final 200 ✓
k09, seq 184 retried (stale):  … 183, 185, … 200, 184         → final 184 ✗
```

A retried record makes its key stale only if its key has no newer event within its 3s delay.

### The fix lives in the sink

Reject a write that is older than what's stored, **in one statement**:

```sql
INSERT INTO readings (key, value, seq) VALUES ($1, $2, $3)
ON CONFLICT (key) DO UPDATE
  SET value = EXCLUDED.value, seq = EXCLUDED.seq
  WHERE readings.seq < EXCLUDED.seq;
```

- **One statement, not read-then-write.** The live consumer and three tiers write the same keys concurrently; a retry can land between your read and your write.
- **Per-key version over timestamp.** Clocks skew between producers, and equal milliseconds tie. The `original_offset` header also works: a key always maps to the same partition, and offsets there only grow.
- **Only for state** ("k09 reads 21.4°C"). For changes ("add 5 to the balance") ignoring a late record loses it; those need dedup by event id, or they can't use retry topics at all.

### Expected results

| variant               | out of order | stale keys |
| --------------------- | -----------: | ---------: |
| blocking, flaky       |            0 |          0 |
| tiered, flaky         |           11 |          1 |
| tiered, flaky + guard |           11 |          0 |

- 12 flaky ids: 500, 1000, …, 6000. Id 6000 is the very last event of its key, so nothing overtakes it: **11 out of order**.
- Id 5500 (k09, seq 184) is produced at 27.5s and retried at 30.5s, after k09's last event (seq 200, produced at 29.9s): **1 stale key**.
- The guard doesn't stop records from arriving late (still 11), it stops them from winning (0 stale).

**Retry topics trade stalls for ordering. Use them only if the consumer can handle out-of-order events per key, typically with a version guard.**

## 4. The DLQ and merging it back

Nothing consumes the DLQ. Someone has to, or it's a leak with a topic name. Three operations:

| command     | what it does                                              |
| ----------- | --------------------------------------------------------- |
| `dlq list`  | read everything, no consumer group, so it doesn't consume |
| `dlq merge` | republish everything onto `readings.retry.1`              |
| `dlq purge` | `DeleteRecords` up to the current end offsets             |

### Reading the whole DLQ: `drain`

`list` and `merge` both start by reading everything currently in the DLQ. Two problems make that less trivial than a loop over `PollFetches`:

- **When to stop.** A poll on a topic you've read to the end doesn't return "done". It blocks, waiting for the next record. So "drained" and "quiet" look the same. The fix: ask the broker for the **end offsets first** and stop once you've read that many records.
- **Where it starts.** After a purge, a partition no longer starts at offset 0. It starts at the **log start offset**. Counting `end − 0` would wait forever for records that were deleted. So the count is `end − start`, per partition.

```go
func drain(topic string) []*kgo.Record {
	adm := admin()
	starts, _ := adm.ListStartOffsets(ctx, topic) // first offset still in the log, per partition
	ends, _ := adm.ListEndOffsets(ctx, topic)     // next offset to be written, per partition
	pending := 0
	ends.Each(func(end kadm.ListedOffset) {
		start, _ := starts.Lookup(end.Topic, end.Partition)
		pending += int(end.Offset - start.Offset)
	})

	// Never commits, so reading it twice shows the same records both times.
	cl := client(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	var records []*kgo.Record
	for len(records) < pending {
		records = append(records, cl.PollFetches(ctx).Records()...)
	}
	return records
}
```

### `list`

Print each record with its headers. This is where the metadata from section 2 pays off: an operator sees why each record failed, where it came from and since when, without digging through logs.

```go
for _, r := range drain(dlqTopic) {
	fmt.Printf("%d/%d key=%s value=%s", r.Partition, r.Offset, r.Key, r.Value)
	for _, h := range r.Headers {
		fmt.Printf(" %s=%s", h.Key, h.Value)
	}
	fmt.Println()
}
```

```
1/0 key=k29 value=300,10,1790172234105 error=non-retryable: unknown field "unit"
    first_failed_at=2026-09-23T14:03:54Z original_offset=109 original_partition=1
    original_topic=readings retry_count=1
```

### `merge`

Republish every record onto the first retry tier, once the fix has shipped:

```go
for _, r := range drain(dlqTopic) {
	// Drop the retry count so a merged record gets every tier again, not one
	// attempt at the tier it died on. The original_* and first_failed_at
	// headers stay: they still say where it came from and since when.
	headers := slices.DeleteFunc(r.Headers, func(h kgo.RecordHeader) bool {
		return h.Key == retryCountHeader || h.Key == errorHeader
	})
	producer.ProduceSync(ctx, &kgo.Record{Topic: retryTopic(1), Key: r.Key, Value: r.Value, Headers: headers})
}
```

- **retry.1, not the live topic** (Uber's choice): records that already failed don't compete with live traffic, and if they fail again they walk the ladder and come back to the DLQ instead of looping.
- **Same key**, so each record lands on the same partition as the rest of its key's history.
- It only waits the retry.1 delay (1s here) before the retry.1 consumer handles it: `rec.Timestamp` is the merge time.
- **Nothing is removed from the DLQ.** The records are now in two places, which is why the next step is `purge`.

### `purge`

Delete everything in the DLQ up to what you just looked at:

```go
ends, _ := adm.ListEndOffsets(ctx, dlqTopic)
adm.DeleteRecords(ctx, ends.Offsets()) // every partition: delete everything before its current end
```

- Kafka can't delete one record in the middle of a log. `DeleteRecords` (the `kafka-delete-records.sh` CLI) **moves the log start offset** forward: everything before it becomes unreadable, and the segment files are removed later, in the background.
- The line is drawn at the end offsets **read at purge time**. A record that lands in the DLQ after that point survives. Purge clears what you looked at, not what arrived while you were deciding.
- **The gap between merge and purge.** A failure that lands in the DLQ _after_ the merge read it but _before_ the purge is below the purge line: it's deleted without ever being merged. The safe version reads the end offsets once, merges up to them, and purges up to those same offsets. The lab reads them again at purge time, for simplicity.

### Expected results

A buggy deploy can't parse every 100th record (`errNonRetryable`), so 6000 / 100 = **60** go straight to the DLQ. The fix ships, then merge:

| moment                 | delivered | dlq | dup | stale keys |
| ---------------------- | --------: | --: | --: | ---------: |
| buggy deploy           | 5940/6000 |  60 |   0 |         ≥1 |
| fixed deploy + merge   | 6000/6000 |  60 |   0 |         ≥1 |
| second merge, no purge | 6000/6000 |  60 |  60 |         ≥1 |

- **Merge replays 60 records, not 6000.** Only the failures.
- **Merged records are old events arriving late**, so they can leave keys stale. Same fix as section 3: a version guard in the sink.
- **Merge doesn't consume.** It commits nothing, so merging twice sends all 60 twice. The sequence is **merge → check → purge**.

## 5. Rewinding a consumer group

A deploy writes wrong results for ids 2001..3000 and raises no errors. Hours later: fix, then rewind the group to when the bug went out:

```bash
# Stop the consumers first: the broker refuses to reset a group with live members.
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh
	--bootstrap-server kafka:9092 \
	--group readings-consumer \
	--topic readings \
	--reset-offsets \
	--to-datetime 2026-09-23T14:05:00.000 \
	--execute
```

- We need to make the consumer idempotent or we will get duplicates.

## 6. Retention is the replay window

`retention.ms = 20s`, 1000 records produced at t = 0:

| seconds since produce | log start offset | log end offset |
| --------------------: | ---------------: | -------------: |
|                     5 |                0 |           1000 |
|                    15 |                0 |           1000 |
|                   ~25 |             1000 |           1000 |

Then rewind to before the first record:

```
Warn: Partition 0 from topic readings is empty. Falling back to latest known offset.
replayed: 0 records
```

- Deleted a bit **after** 20s: deletion runs on a sweep (`log.retention.check.interval.ms`, default 5 min) and works per whole segment, so `retention.ms` is a minimum.
- An idle topic still expires: when every segment is past retention, the broker rolls a fresh empty active segment and deletes the old one.
- **Rewinding past the log start is a warning, not an error.** Exit code 0, zero records replayed. A replay script has to check the log start offset itself.
- For replays past retention: tiered storage (KIP-405, `remote.storage.enable`) or an archive in object storage.

## Interview answers

**"How do you handle a message that fails processing?"**
Classify the error. Retryable errors go to a retry topic with a growing delay (1 tier per delay, each its own consumer group), then to a DLQ. Non-retryable errors go straight to the DLQ. The original is committed as soon as the failure is safely on the next topic, so the partition never blocks.

**"What's the downside?"**
Ordering. A retried record arrives after newer events of its key. I'd fix that in the sink with a version guard (`WHERE stored.seq < incoming.seq`), and only if the events are state, not changes. If order truly matters, you have to block and accept the stall.

**"Why not just not commit?"**
A consumer has two positions: the fetch position (in memory, moves on every poll) and the committed offset (on the broker, only read on assignment). Not committing doesn't move the fetch position back, so the record only returns after a restart or rebalance. Seeking back works, but it still blocks the partition, and blocking inside the poll loop in Java eventually trips `max.poll.interval.ms` and triggers a rebalance.

**"What do you do with the DLQ?"**
Alert on anything landing there. After the fix: merge it back into the first retry tier (not the live topic), check, then purge. Merge doesn't consume, so merging twice duplicates.

**"How do you reprocess data after a bug?"**
Stop the consumers, `kafka-consumer-groups --reset-offsets --to-datetime <when the bug shipped> --execute`, restart. It replays everything since then, not just the bad records, so the sink must be idempotent (upsert), and it can only reach back as far as retention.

**"What's a replayable queue?"**
One where reading doesn't delete. In Kafka, a consumer's position is an offset you can move back, and data lives until retention runs out, not until someone reads it.
