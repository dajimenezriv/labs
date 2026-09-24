# Delivery Guarantees

- [The setup](#the-setup)
- [1. The leader dies mid-produce](#1-the-leader-dies-mid-produce)
  - [Expected results](#expected-results)
- [2. What `min.insync.replicas` actually refuses](#2-what-mininsyncreplicas-actually-refuses)
  - [Expected results](#expected-results-1)
- [3. One crash, three commit placements](#3-one-crash-three-commit-placements)
  - [Expected results](#expected-results-2)
- [Interview answers](#interview-answers)

## The setup

```yaml
# compose.yaml
name: delivery-guarantees

x-kafka: &kafka
  image: apache/kafka:4.1.0
  environment: &kafka-env
    CLUSTER_ID: delivery-guarantees-lab

services:
  kafka1:
    <<: *kafka
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "1"

  kafka2:
    <<: *kafka
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "2"

  kafka3:
    <<: *kafka
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: "3"
```

- 3 brokers (`apache/kafka:4.1.0`, KRaft), each also a controller. The client is franz-go.
- Every topic has **1 partition** and **rf=3**, so there is one leader and one log to reason about.
- The test is to produce ids `1..N`, write down every id the broker **acknowledged**, consume the topic, and write down every id processed. Then compare the two sets:
  - **lost**: acked but never processed. The producer was told it was durable, so nothing upstream will ever retry it.
  - **duplicated**: processed more than once.
  - A record that returns an error is a handled failure, not a loss. The caller knows and can retry.
- Failures are a `SIGKILL`, not a stop:

```bash
docker compose kill -s SIGKILL kafka1
```

- A graceful stop (`SIGTERM`) does a controlled shutdown (`controlled.shutdown.enable` = true), which moves leadership off the broker first. That loses nothing, so it proves nothing.

[GitHub: franz-go producing and consuming.](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md)

Prometheus here scrapes every **1s**, against a default of 15s.

- `topic` is split into N `partitions`, each a separate append-only log with its own offsets, its own leader and its own ordering.
- `partitions`: how many independent logs.
- `rf` (replication factor): how many copies of each partition.
  - Replication factor 3 means each partition lives on 3 different brokers: one leader and two followers fetching from it.
  - It sets how many failures the data can survive at all.
  - It can't exceed the number of brokers.
  - It's fixed at creation in practice; changing it later means a partition reassignment, not a config flip.
- `min.insync.replicas`: how many copies must be done for `acks=all` to succeed. Can be changed easily.
- `acks` is a producer setting, sent on every producer request. Trades latency for safety.
  - `acks=0`: waits until the message is dispatched to the socket buffer. Does not wait for any response from the broker.
  - `acks=1`: waits until the partition leader receives the record and writes it to its local log.
  - `acks=all`: waits until the full set of ISR acknowledge the record.
- **High watermark (HW)**: the highest offset that has been successfully replicated across all ISRs for a partition. Consumers can only read messages up to the high watermark to prevent reading uncommitted data that might disappear if a leader fails.

The classic combination is `rf=3`, `min.insync.replicas=2`, `acks=all`. Both the Java client (since 3.0) and franz-go default to `acks=all` with the idempotent producer.

```bash
# Use the kafka-topics tool to create or delete a topic.
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh
  --bootstrap-server kafka1:9092 \
  --create \
  --topic seq \
  --partitions 1 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

## 1. The leader dies mid-produce

- Produce **2,000,000** ids as fast as possible (`linger` = 0, so nothing waits client-side).
- **2s** in, find the leader of partition 0 and `SIGKILL` it.
- Wait for the producer to finish, restart the broker, consume everything, compare.

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --describe --topic seq
# Topic: seq  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

The only difference between the two producers:

```go
// acks=all: stock franz-go, nothing to set.
opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ProducerLinger(0)}

// acks=1: franz-go refuses it with idempotence on ("idempotency requires acks=all"),
// so both have to go.
opts = append(opts,
	kgo.RequiredAcks(kgo.LeaderAck()),
	kgo.DisableIdempotentWrite(),
)
```

**`acks=1`** loses whatever the leader had acked but the followers hadn't fetched yet:

1. The leader appends record 578386 and acks it. The followers are a few ms behind, which is normal.
2. `SIGKILL`. The records past the followers' position exist on one disk only.
3. The controller elects a new leader from the ISR. The followers are still in the ISR (they're only ms behind, and the limit is 30s), so this is a **clean** election. Nothing was misconfigured.
4. The old leader restarts as a follower, compares its log with the new leader's by leader epoch, and deletes what the new leader never had. This is an INFO line, not an error:

```
[ReplicaFetcher replicaId=2, leaderId=3] Truncating partition seq-acks1-1-0 with
  TruncationState(offset=578385, completed=true) due to leader epoch and offset
  EpochEndOffset(errorCode=0, partition=0, leaderEpoch=0, endOffset=578385)
[UnifiedLog partition=seq-acks1-1-0] Truncating to offset 578385
```

- The lost ids are one **contiguous** range: the suffix of the log that only ever existed on the dead leader. How long it is = produce rate × follower lag at the instant of the kill.
- Those records were above the HW, so **no consumer ever saw them either**. Nothing downstream can notice they are missing.
- The same kill can lose zero if the followers happened to be caught up. `acks=1` doesn't lose data on every failure, only on a hard leader failure with replication lag, and that is why teams run it for years without noticing.

**`acks=all`** can't lose an acked record:

- **Leader dies after the ack**: every ISR member already had the record, so the new leader has it.
- **Leader dies before the ack**: the producer gets an error or a timeout and retries against the new leader. The record may be on some replicas already, so a plain retry would be a duplicate. The idempotent producer prevents that: each record carries a **producer id + sequence number**, and the broker drops a sequence it has already written.

### Expected results

| variant  |     acked |        delivered |                lost | duplicated |
| -------- | --------: | ---------------: | ------------------: | ---------: |
| acks=1   | 2,000,000 | 2,000,000 − lost | 0 to a few thousand |          0 |
| acks=all | 2,000,000 |        2,000,000 |                   0 |          0 |

- **acks=1**: one real run of this setup lost **1457** ids (`579842 − 578386 + 1`), one contiguous gap. Rerunning the same kill lost 0. Quote the range, not the number.
- **acks=all**: 0 lost and 0 duplicated, because of replication and idempotence together.

What the monitoring shows during each kill:

| signal                 | acks=1, lost records | acks=1, lost nothing | acks=all |
| ---------------------- | -------------------- | -------------------- | -------- |
| ISR size               | 3 → 2                | 3 → 2                | 3 → 2    |
| leader change          | yes                  | yes                  | yes      |
| under-replicated parts | yes                  | yes                  | yes      |

- **The run that destroyed data looks identical to the ones that didn't.** No broker metric reports loss. Only an end-to-end count (ids in vs ids out) finds it.

## 2. What `min.insync.replicas` actually refuses

- With three live brokers the ISR is full, so `acks=all` waits for all three whatever `min.insync.replicas` says. Part 1 would give the same results with 1, 2 or 3.
- `min.insync.replicas` is not about the healthy path. It's the number the leader checks **before accepting** an `acks=all` write, and it only matters once the ISR has shrunk.

So shrink it:

- Three topics, all `rf=3`, with `min.insync.replicas` = 1, 2 and 3.
- `SIGKILL` one broker. Only one: the controller quorum is these same three nodes, so killing two would leave no quorum, and the failure would be the missing controller, not the topic refusing writes.
- Wait **> 30s** for `replica.lag.time.max.ms` to expire. Until then the dead broker is still in the ISR, and `acks=all` writes just hang waiting for it.
- Produce **10,000** ids to each topic with `acks=all`.

The leader's check (simplified from `Partition.appendRecordsToLeader`):

```scala
if (requiredAcks == -1 && inSyncSize < minIsr)
  throw new NotEnoughReplicasException(...)
```

- `NOT_ENOUGH_REPLICAS` (error code 19): the leader rejects the write **before** appending.
- `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20): the ISR shrank after the append. The record is in the leader's log, but the producer is told it failed.
- Both are **retryable**. franz-go retries them forever by default (`RecordRetries` unlimited, no delivery timeout), so the producer blocks rather than failing. The Java client gives up at `delivery.timeout.ms` (default 120000). To see the broker's own error, the lab caps retries at 3.

### Expected results

| topic           |   sent |  acked | failed | first error         |
| --------------- | -----: | -----: | -----: | ------------------- |
| rf=3, min.isr=1 | 10,000 | 10,000 |      0 | -                   |
| rf=3, min.isr=2 | 10,000 | 10,000 |      0 | -                   |
| rf=3, min.isr=3 | 10,000 |      0 | 10,000 | NOT_ENOUGH_REPLICAS |

- `min.isr=3` on an rf=3 topic: **one broker loss is a full write outage**.
- `min.isr=1`: `acks=all` still succeeds with only the leader left, which is `acks=1` with extra steps.
- `min.isr=2`: survives one failure, refuses writes on the second instead of accepting records that exist on one disk.
- **Weakening any one of rf, `min.insync.replicas` or `acks` makes the other two decorative.**

## 3. One crash, three commit placements

Parts 1 and 2 are about the producer. The consumer can lose or duplicate on its own, depending on where the offset commit sits relative to the work.

- **20,000** ids, produced with `acks=all` (so the log is complete).
- Each poll returns up to **500** records. Processing is **200µs** per record, so a batch takes 500 × 200µs = **100ms**.
- Autocommit interval: **100ms** (stock `auto.commit.interval.ms` is 5000; scaled down so a commit fires inside a batch).
- The consumer processes record **5300** and calls `os.Exit(9)`: no close, no leave-group, no final commit.
- Then the same group starts a new member, which resumes from the committed offset.

The crash lands in the 11th batch (records 5001–5500). The only question is what the committed offset was at that moment:

```go
for {
	recs := cl.PollRecords(ctx, 500).Records()
	for _, r := range recs {
		process(r) // appends the id to the sink; once written, the effect is visible
	}
	if commit == "manual" {
		cl.CommitRecords(ctx, recs...) // after the work
	}
}
```

The three placements:

```go
// greedy: commits whatever poll returned, on a timer, processed or not.
// This is enable.auto.commit=true in the Java client and librdkafka.
kgo.GreedyAutoCommit(), kgo.AutoCommitInterval(100*time.Millisecond)

// franz-go default: autocommits on a timer, but only offsets from the PREVIOUS poll.
// Calling PollRecords is what tells the client you're done with the last batch.
kgo.AutoCommitInterval(100 * time.Millisecond)

// manual: no timer; CommitRecords after processing, as in the loop above.
kgo.DisableAutoCommit()
```

| placement           | can commit a record before it's processed? | committed at the crash                   | resumes at |
| ------------------- | ------------------------------------------ | ---------------------------------------- | ---------- |
| greedy autocommit   | yes                                        | 5500, if the timer fired during batch 11 | 5501       |
| franz-go autocommit | no                                         | ≤ 5000                                   | ≤ 5001     |
| commit after work   | no                                         | 5000                                     | 5001       |

**Recovery time is the session timeout.** A killed member never sends leave-group, so the coordinator keeps its partition reserved until `session.timeout.ms` expires (default 45000; the lab uses 6000, the broker's `group.min.session.timeout.ms` floor). The new member waits that long before it gets anything.

### Expected results

| placement           |  acked |          delivered |       lost | duplicated |
| ------------------- | -----: | -----------------: | ---------: | ---------: |
| greedy autocommit   | 20,000 | 19,800 (or 20,300) | 200 (or 0) | 0 (or 300) |
| franz-go autocommit | 20,000 |           ≥ 20,300 |          0 |      ≥ 300 |
| commit after work   | 20,000 |             20,300 |          0 |        300 |

- **Greedy**: with a 100ms timer and a 100ms batch, the timer almost always fires inside batch 11 and commits 5500. The restart skips 5301..5500: 5500 − 5300 = **200 lost**, one contiguous gap. If the timer happened not to fire, it's 300 duplicates instead.
- **franz-go default**: can't commit batch 11 while it's being processed, so it redoes 5001..5300: 5300 − 5000 = **300 duplicates**, more if the last commit was an earlier batch.
- **Commit after work**: exactly 300 duplicates, deterministically.
- **Every safe placement is at-least-once. Exactly-once processing needs an idempotent sink** (a unique key, an upsert, a dedup table), not a different commit.

## Interview answers

**How do you guarantee no message loss in Kafka?**
Producer `acks=all` with idempotence, topic `rf=3` and `min.insync.replicas=2`, and a consumer that commits only after the work is durable. The first three make an acked record survive one broker loss; the last makes a crash redo work instead of skipping it. Then make the sink idempotent, because what's left is duplicates.

**Is `acks=1` ever acceptable?**
It only loses data on a hard leader failure while followers lag, and then only the few ms of records that were on the leader alone. In one test run that was 1457 of 2M records, and on the next run 0. It's fine for metrics or logs where a small gap is acceptable, and nowhere the producer's "success" has to mean durable, because no metric will tell you it happened.

**What does `min.insync.replicas` do?**
It's the number of in-sync replicas the leader requires before accepting an `acks=all` write. On a healthy cluster it changes nothing. Once the ISR shrinks below it, the leader rejects with `NOT_ENOUGH_REPLICAS`, trading availability for durability. `rf=3, min.isr=2` survives one broker loss; `min.isr=3` turns one broker loss into a write outage.

**Why can an acked record disappear with `acks=1` if the election was clean?**
ISR membership is based on lag time (`replica.lag.time.max.ms`, 30s), not offsets, so a follower a few ms behind is in sync and eligible to lead. When it takes over, the old leader truncates to the new leader's log on rejoin. Those records were above the high watermark, so no consumer had seen them either.

**Why does auto-commit lose messages?**
In the Java client, `enable.auto.commit` commits the offsets returned by `poll()` on a timer, whether or not they've been processed. A crash after the commit but before the work skips those records. franz-go avoids this by only autocommitting the previous poll's offsets, which turns the same crash into duplicates.

**How long does a crashed consumer take to recover?**
Until `session.timeout.ms` expires (45s by default), because a killed process never sends leave-group. Until then its partitions stay assigned to a dead member and nobody consumes them.

**What does the idempotent producer protect against?**
Duplicates from producer retries. Each record carries a producer id and sequence number, and the broker drops a sequence it already has. It doesn't cover the consumer side or a producer restart that re-sends from the application; that needs transactions or an idempotent sink.
