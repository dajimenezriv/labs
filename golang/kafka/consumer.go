package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
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
			traceContext := make(map[string]string, len(r.Headers))
			for _, h := range r.Headers {
				traceContext[h.Key] = string(h.Value)
			}

			handleCtx := extractTraceContext(ctx, traceContext)
			spanName := fmt.Sprintf("consume %s", topic)
			handleCtx, span := otel.Tracer(group).Start(handleCtx, spanName)
			span.SetAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination.name", topic),
				attribute.String("messaging.kafka.consumer.group", group),
			)

			slog.InfoContext(handleCtx, "consume event", "payload", string(r.Value))
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

func extractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}
