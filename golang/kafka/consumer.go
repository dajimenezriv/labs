package main

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
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
				slog.ErrorContext(ctx, "kafka fetch",
					"topic", e.Topic,
					"partition", e.Partition,
					"err", e.Err)
			}
			continue
		}

		var handled []*kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			headers := make(map[string]string, len(r.Headers))
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}

			traceContext := extractTraceContext(ctx, headers)
			spanCtx, span := otel.Tracer(serviceName).Start(traceContext, "consume "+topic)
			slog.InfoContext(spanCtx, "consume event", "payload", string(r.Value))
			span.End()

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
