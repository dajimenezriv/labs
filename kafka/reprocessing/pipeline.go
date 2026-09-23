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
// order they happened. A consumer that genuinely needs per-key order has to
// block instead, and eat the head-of-line stall. Section 1 measures both.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const liveTopic = "readings"

// retryDelays is one entry per retry topic: how long a record waits on that
// tier before it is handled again. Each is longer than the last, so a
// downstream that blipped is retried almost at once and one that is properly
// down is not hammered while it recovers. Uber's are minutes; these are scaled
// down so a record reaches the DLQ while you are still watching.
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

// Caps the error header. A handler that returns a whole response body would
// otherwise push the record past the broker's size limit, and a record that
// cannot be routed is a record the consumer has to block on.
const maxErrorHeader = 512

// handler processes one record. attempt is 1 the first time a record is seen.
type handler func(rec *kgo.Record, attempt int) error

// consumer reads one topic as its own consumer group and commits only what it
// has finished with, where "finished" means handled or handed on.
type consumer struct {
	client *kgo.Client

	// How long after a record was published it may be handled. Zero on the
	// live topic; a tier's backoff on a retry topic.
	delay time.Duration

	// What becomes of a record the handler rejected. nil means retry it in
	// place until it succeeds -- the blocking consumer. Set, it moves the
	// record to the next topic and the partition carries on.
	route func(ctx context.Context, rec *kgo.Record, err error) error
}

func newConsumer(brokers []string, group, topic string) (*consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Commit by hand, after the record is finished with. Autocommit could
		// commit a record whose handler then fails, which loses it.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	return &consumer{client: client}, nil
}

func (c *consumer) run(ctx context.Context, handle handler) {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			fmt.Fprintf(os.Stderr, "fetch %s/%d: %v\n", topic, partition, err)
		})

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
				if err := c.route(ctx, rec, err); err != nil {
					// The record is still only here. The broker being
					// unwritable is not a situation this lab creates.
					die(err)
				}
			}
			done = append(done, rec)
		})

		// Shutting down mid-batch. The unfinished records will be delivered
		// again to whoever joins next, which is the at-least-once contract.
		if ctx.Err() != nil {
			return
		}
		if len(done) > 0 {
			if err := c.client.CommitRecords(ctx, done...); err != nil {
				fmt.Fprintf(os.Stderr, "commit: %v\n", err)
			}
		}
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

// pipeline is the live consumer plus one consumer per tier, each its own
// consumer group reading its own topic, so a backlog of retries cannot slow
// live traffic and the tiers rebalance and commit separately. They share one
// process here only because there is no reason for four.
type pipeline struct {
	producer  *kgo.Client
	consumers []*consumer
	// Called with every record sent to the DLQ, so the sink can count them.
	deadLetter func(rec *kgo.Record)
}

func newPipeline(brokers []string, group string, deadLetter func(*kgo.Record)) (*pipeline, error) {
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	p := &pipeline{producer: producer, deadLetter: deadLetter}

	live, err := newConsumer(brokers, group, liveTopic)
	if err != nil {
		return nil, err
	}
	live.route = p.route
	p.consumers = append(p.consumers, live)

	for tier := 1; tier <= len(retryDelays); tier++ {
		c, err := newConsumer(brokers, fmt.Sprintf("%s.retry.%d", group, tier), retryTopic(tier))
		if err != nil {
			return nil, err
		}
		c.delay = retryDelays[tier-1]
		c.route = p.route
		p.consumers = append(p.consumers, c)
	}
	return p, nil
}

func (p *pipeline) close() {
	for _, c := range p.consumers {
		c.client.Close()
	}
	p.producer.Close()
}

// route publishes a rejected record onto the next topic along. Returning an
// error means the record is still only on the topic it came from, so the
// caller must not commit it.
func (p *pipeline) route(ctx context.Context, rec *kgo.Record, handleErr error) error {
	count := retryCount(rec) + 1

	destination := retryTopic(count)
	if count > len(retryDelays) || errors.Is(handleErr, errNonRetryable) {
		destination = dlqTopic
	}

	err := p.producer.ProduceSync(ctx, &kgo.Record{
		Topic: destination,
		// Same key, so a record keeps landing on the same partition of
		// whatever topic it is on and the tiers stay as parallel as the
		// original.
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: routeHeaders(rec, count, handleErr),
	}).FirstErr()
	if err != nil {
		return fmt.Errorf("route to %s: %w", destination, err)
	}
	if destination == dlqTopic {
		p.deadLetter(rec)
	}
	return nil
}

// routeHeaders builds the headers for the next hop: the origin headers from
// the first hop, carried unchanged, plus a fresh retry count and error.
func routeHeaders(rec *kgo.Record, count int, handleErr error) []kgo.RecordHeader {
	// Defaults for a record on its first hop. One that has been round before
	// keeps the values it already carries.
	origin := map[string]string{
		originalTopicHeader:     rec.Topic,
		originalPartitionHeader: strconv.Itoa(int(rec.Partition)),
		originalOffsetHeader:    strconv.FormatInt(rec.Offset, 10),
		firstFailedAtHeader:     time.Now().UTC().Format(time.RFC3339),
	}

	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+len(origin)+2)
	for _, h := range rec.Headers {
		if _, isOrigin := origin[h.Key]; isOrigin {
			origin[h.Key] = string(h.Value)
			continue
		}
		// Rewritten below. Copying this tier's as well would leave a record
		// in the DLQ carrying one of each per hop.
		if h.Key == retryCountHeader || h.Key == errorHeader {
			continue
		}
		headers = append(headers, h)
	}
	for key, value := range origin {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	reason := handleErr.Error()
	if len(reason) > maxErrorHeader {
		reason = reason[:maxErrorHeader]
	}
	return append(headers,
		kgo.RecordHeader{Key: retryCountHeader, Value: []byte(strconv.Itoa(count))},
		kgo.RecordHeader{Key: errorHeader, Value: []byte(reason)},
	)
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Zero for a record on the live topic, which has no such header.
func retryCount(rec *kgo.Record) int {
	n, _ := strconv.Atoi(header(rec, retryCountHeader))
	return n
}
