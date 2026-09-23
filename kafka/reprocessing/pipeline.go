package main

// Uber's reprocessing pipeline: a live topic, a ladder of retry topics, and a
// dead letter queue, wired together so a rejected record walks from one to the
// next instead of holding up the records behind it.
//
//	readings ──fail──> readings.retry.1 ──> readings.retry.2 ──> readings.retry.3 ──> readings.dlq
//	                         1s                   2s                   4s
//
// The price is ordering. A record that fails is handled seconds (in production,
// minutes) after the ones behind it, so a key's events no longer arrive in the
// order they happened. Section 1 measures both.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const liveTopic = "readings"

// retryDelays is one entry per retry topic: how long a record waits on that
// tier before it is handled again. Each is longer than the last, so a
// downstream that blipped is retried almost at once and one that is properly
// down is not hammered while it recovers.
//
// The blocking consumer backs off on the same ladder, so the two modes differ
// only in where the wait happens.
var retryDelays = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

// Tiers are numbered from one, so the number is both the topic's name and the
// retry count of everything on it.
func retryTopic(tier int) string { return fmt.Sprintf("%s.retry.%d", liveTopic, tier) }

// Nothing consumes the DLQ. It is a queue in the sense that things pile up in
// it, and someone owes work when they do.
const dlqTopic = liveTopic + ".dlq"

func labTopics() []string {
	topics := []string{liveTopic, dlqTopic}
	for tier := 1; tier <= len(retryDelays); tier++ {
		topics = append(topics, retryTopic(tier))
	}
	return topics
}

// Headers the pipeline writes onto a record it moves on. That this metadata
// exists at all is the point of separate topics: a handler retried in place
// knows nothing about its own history.
const (
	// How many times the record has been rejected, which is also the tier it
	// is on, which is what decides where it goes next.
	retryCountHeader = "retry_count"

	// Where the record entered the pipeline. Carried unchanged from the first
	// hop: by the time something lands in the DLQ, the topic it arrived from
	// is a retry topic and says nothing about where it came from.
	originalTopicHeader     = "original_topic"
	originalPartitionHeader = "original_partition"
	originalOffsetHeader    = "original_offset"

	// So how long something has been stuck is a subtraction rather than a
	// hunt through logs.
	firstFailedAtHeader = "first_failed_at"

	// Why it failed the most recent time.
	errorHeader = "error"
)

// errNonRetryable marks a failure that waiting will not fix: a payload that
// will not parse, a bug. Wrap it and the record skips the tiers and goes
// straight to the DLQ, instead of occupying the pipeline for the length of the
// ladder to fail identically at the end of it.
var errNonRetryable = errors.New("non-retryable")

// handler processes one record. attempt is 1 the first time a record is seen.
type handler func(rec *kgo.Record, attempt int) error

// consumer reads one topic as its own consumer group and commits only what it
// has finished with, where "finished" means handled or handed on.
type consumer struct {
	client *kgo.Client

	// How long after a record was published it may be handled. Zero on the
	// live topic; a tier's backoff on a retry topic.
	delay time.Duration

	// Moves a rejected record to the next topic so the partition carries on.
	// nil means retry it in place until it succeeds: the blocking consumer.
	route func(rec *kgo.Record, err error)
}

func newConsumer(group, topic string) *consumer {
	return &consumer{client: client(
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Commit by hand, after the record is finished with. Autocommit could
		// commit a record whose handler then fails, which loses it.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)}
}

func (c *consumer) run(ctx context.Context, handle handler) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		var done []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) {
			if ctx.Err() != nil {
				return
			}

			// Kafka has no delayed delivery, so a tier's delay is the consumer
			// sleeping until the record is due. Nothing is reordered by doing
			// so: a partition is already in publish order, so the next record
			// is never due before this one.
			//
			// On a Java consumer this sleep counts against
			// max.poll.interval.ms (5 min) and gets the member evicted if a
			// tier's delay is longer. franz-go heartbeats and tracks liveness
			// on its own goroutines, so here it just blocks the loop.
			if c.delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Until(rec.Timestamp.Add(c.delay))):
				}
			}

			if c.route == nil {
				if !retryInPlace(ctx, rec, handle) {
					return
				}
			} else if err := handle(rec, retryCount(rec)+1); err != nil {
				// Handed on, so this copy is finished with and commits like a
				// success would. That is the whole trick: the partition moves
				// past the failure instead of sitting on it.
				c.route(rec, err)
			}
			done = append(done, rec)
		})

		// Shutting down mid-batch. The unfinished records will be delivered
		// again to whoever joins next, which is the at-least-once contract.
		if ctx.Err() != nil {
			return
		}
		c.client.CommitRecords(ctx, done...)
	}
}

// retryInPlace is what a consumer without a retry topic does: keep trying the
// same record, backing off on the same ladder as the tiers, until it works.
// Every record behind it -- on every partition this poll loop owns, not just
// this one -- waits.
//
// Not committing is not an alternative. The fetch position has already moved
// past the record, so the next poll returns the records after it; the failed
// one only comes back after a restart or rebalance, from the last commit.
// Blocking has to be done on purpose.
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

// newPipeline is the live consumer plus one consumer per tier, each its own
// consumer group reading its own topic, so a backlog of retries cannot slow
// live traffic and the tiers rebalance and commit separately. They share one
// process here only because there is no reason for four.
func newPipeline(group string, deadLetter func(*kgo.Record)) []*consumer {
	producer := client()

	// Publishes a rejected record onto the next topic along. ProduceSync, so
	// the record is on the next topic before this copy is committed: commit
	// first and a crash in between loses it.
	route := func(rec *kgo.Record, err error) {
		count := retryCount(rec) + 1
		destination := retryTopic(count)
		if count > len(retryDelays) || errors.Is(err, errNonRetryable) {
			destination = dlqTopic
			deadLetter(rec)
		}
		producer.ProduceSync(context.Background(), &kgo.Record{
			Topic: destination,
			// Same key, so a record keeps landing on the same partition of
			// whatever topic it is on and the tiers stay as parallel as the
			// original.
			Key:     rec.Key,
			Value:   rec.Value,
			Headers: routeHeaders(rec, count, err),
		})
	}

	live := newConsumer(group, liveTopic)
	live.route = route
	consumers := []*consumer{live}
	for tier := 1; tier <= len(retryDelays); tier++ {
		c := newConsumer(fmt.Sprintf("%s.retry.%d", group, tier), retryTopic(tier))
		c.delay = retryDelays[tier-1]
		c.route = route
		consumers = append(consumers, c)
	}
	return consumers
}

// routeHeaders builds the headers for the next hop: the origin headers from
// the first hop, carried unchanged, plus a fresh retry count and error.
func routeHeaders(rec *kgo.Record, count int, err error) []kgo.RecordHeader {
	// Defaults for a record on its first hop. One that has been round before
	// keeps the values it already carries.
	origin := map[string]string{
		originalTopicHeader:     rec.Topic,
		originalPartitionHeader: strconv.Itoa(int(rec.Partition)),
		originalOffsetHeader:    strconv.FormatInt(rec.Offset, 10),
		firstFailedAtHeader:     time.Now().UTC().Format(time.RFC3339),
	}
	for _, h := range rec.Headers {
		if _, ok := origin[h.Key]; ok {
			origin[h.Key] = string(h.Value)
		}
	}

	headers := []kgo.RecordHeader{
		{Key: retryCountHeader, Value: []byte(strconv.Itoa(count))},
		{Key: errorHeader, Value: []byte(err.Error())},
	}
	for key, value := range origin {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return headers
}

// Zero for a record on the live topic, which has no such header.
func retryCount(rec *kgo.Record) int {
	for _, h := range rec.Headers {
		if h.Key == retryCountHeader {
			n, _ := strconv.Atoi(string(h.Value))
			return n
		}
	}
	return 0
}
