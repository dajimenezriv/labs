# Kafka

- [Introduction](#introduction)
- [Producer](#producer)
- [Delivery](#delivery)
- [Trace id](#trace-id)
- [Examples](#examples)
  - [Setup](#setup)
  - [Publish record](#publish-record)

## Introduction

- `topic` is split into N `partitions`, each a separate append-only log with its own offsets, its own leader and its own ordering.
- `partitions`: how many independent logs. Changing the number later is difficult.
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

QUESTIONS

- On redeploy, affected partitions pause (rebalance time if graceful (franz-go sends a `LeaveGroup` on `client.Close()`), up to `session.timeout.ms` if killed).

- Consumer lag as the one metric that matters, and how you'd alert on it.
- Schema evolution. Real teams run Avro/Protobuf with a schema registry and backward-compatibility rules. Adding a required field breaks consumers; that's the interview trap.

- We always deploy a kafka cluster.
- A kafka cluster has brokers (which are virtual or physical servers). Is the kafka cluster like another system that manages brokers or how are they managed?

## Producer

- Commiting is just write a number into `__consumer_offsets` in kafka.
- `session.timeout.ms` (default 45s): network liveness check. A background thread sends a heartbeat every `heartbeat.interval.ms`, if timeout, it declares the member dead and rebalances.
- `max.poll.interval.ms` (default 5min): processing liveness check. The client is the one that sends a `LeaveGroup` after timeout and then kafka rebalances.
- If a dedad member rejoins, it does a `poll()`, finds it's not a member, sends a `JoinGroup` and rejoins, triggering a second rebalance. If it tries to commit the previous work it gets a `CommitFailedException`.

## Delivery

- `exactly-once` only works from Kafka → Kafka.

## Trace id

```go
headers := []kgo.RecordHeader{
  {Key: EventTypeHeader, Value: []byte(eventType)},
}
for k, v := range p.tel.InjectTraceContext(ctx) {
  headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
}

record := &kgo.Record{
  Topic:   topic,
  Key:     []byte(key),
  Value:   value,
  Headers: headers,
}
```

## Examples

### Setup

```go
func createTopic(topic string, partitions int32) {
  client, err := kgo.NewClient(kgo.SeedBrokers([]string{"kafka:port"}))
  admin := kadm.NewClient(client)
  err := admin.CreateTopic(ctx, partitions, -1, nil, topic)
}
```

### Publish record

```go
func publishRecord(topic, key, eventType string, value []byte) {
  client, err := kgo.NewClient(kgo.SeedBrokers([]string{"kafka:port"}))

  headers := []kgo.RecordHeader{
    {Key: EventTypeHeader, Value: []byte(eventType)},
  }
  for k, v := range observability.InjectTraceContext(ctx) {
    headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
  }

  // We can send records in batch.
  record := &kgo.Record{topic, []byte(key), value, headers}
  err := p.client.ProduceSync(record).FirstErr()
}
```
