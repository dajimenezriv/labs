package main

import (
	"context"
	alertspb "golang/kafka/proto"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"
)

func runConsumer(ctx context.Context, client *kgo.Client) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		// Fetch errors are usually transient (rebalance, broker restarting).
		// Log them and still process the records: one poll can mix an error on
		// one partition with records from others, and the read position has
		// already moved past those records, so skipping them loses them.
		for _, e := range fetches.Errors() {
			slog.ErrorContext(ctx, "kafka fetch",
				"topic", e.Topic,
				"partition", e.Partition,
				"err", e.Err)
		}

		var handled []*kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			headers := make(map[string]string, len(r.Headers))
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}

			traceContext := extractTraceContext(ctx, headers)
			spanCtx, span := otel.Tracer(serviceName).Start(traceContext, "consume "+topic)
			var alert alertspb.Alert
			if err := proto.Unmarshal(r.Value, &alert); err != nil {
				// A payload that cannot be decoded will not decode on a retry either.
				slog.ErrorContext(spanCtx, "unmarshal alert", "err", err)
			} else {
				slog.InfoContext(spanCtx, "consume event",
					"sensor_id", alert.GetSensorId(),
					"value", alert.GetValue())
			}
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
