package main

import (
	"context"
	"errors"
	"fmt"
	"kafka/db"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type relay struct {
	pool     *pgxpool.Pool
	producer *kgo.Client
}

func (r *relay) run(ctx context.Context) {
	ticker := time.NewTicker(outboxInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "outbox relay stopped")
			return
		case <-ticker.C:
			// A burst should not have to wait one tick per batch to drain.
			for {
				published, err := r.publishBatch(ctx)
				if err != nil {
					slog.ErrorContext(ctx, "publish outbox batch", "err", err)
					break
				}
				if published < int(outboxBatchSize) {
					break
				}
			}
		}
	}
}

func (r *relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.WarnContext(ctx, "rollback transaction", "err", err)
		}
	}()

	queries := db.New(tx)
	events, err := queries.GetOutboxEvents(ctx, outboxBatchSize)
	if err != nil {
		return 0, fmt.Errorf("get outbox events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	ids := make([]int64, len(events))
	for _, e := range events {
		// Get trace from outbox

		headers := []kgo.RecordHeader{}
		for k, v := range injectTraceContext(ctx) {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}

		if err := r.producer.ProduceSync(ctx, &kgo.Record{
			Key:     []byte(e.PartitionKey),
			Topic:   topic,
			Value:   e.Payload,
			Headers: headers,
		}).FirstErr(); err != nil {
			return 0, fmt.Errorf("publish event %d: %w", e.ID, err)
		}

		ids = append(ids, e.ID)
	}

	if _, err := tx.Exec(ctx, "UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])", ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	slog.InfoContext(ctx, "published outbox events", "count", len(ids))

	return len(ids), nil
}

func injectTraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}
