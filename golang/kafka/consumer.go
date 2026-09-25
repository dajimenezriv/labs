package main

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

func runConsumer(ctx context.Context, client *kgo.Client) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			// Fetch errors are usually transient (rebalance, broker restarting).
			// Continue polling.
			for _, e := range errs {
				slog.ErrorContext(ctx, "kafka fetch", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
			continue
		}

		var handled []*kgo.Record
		var failed bool

		// 		Key:          string(r.Key),
		// EventType:    header(r, EventTypeHeader),
		// Value:        r.Value,
		// RetryCount:   retryCount(r),
		// TraceContext: traceContext,

		fetches.EachRecord(func(r *kgo.Record) {
			// Once one record in this batch fails, stop committing the rest:
			// committing past it would skip it forever.
			if failed {
				return
			}

			slog.InfoContext(ctx, "consume event", "payload", string(r.Value))
			handled = append(handled, r)
		})

		if len(handled) == 0 {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if err := client.CommitRecords(ctx, handled...); err != nil {
			slog.ErrorContext(ctx, "commit records", "err", err)
		}
	}
}
