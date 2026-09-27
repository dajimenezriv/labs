package main

import (
	"context"
	"encoding/json"
	"errors"
	"kafka/db"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	serviceName       = "kafka"
	brokers           = "localhost:29092"
	databaseURL       = "postgresql://postgres:postgres@localhost:5555/db?sslmode=disable"
	outboxBatchSize   = 100
	outboxInterval    = time.Second
	group             = "alerts"
	topic             = "alerts"
	partitions        = 3
	replicationFactor = -1
)

type alert struct {
	SensorID string  `json:"sensor_id"`
	Value    float64 `json:"value"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.SetDefault(slog.New(traceHandler{slog.NewTextHandler(os.Stderr, nil)}))

	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		panic("new pool: " + err.Error())
	}
	if err := pool.Ping(ctx); err != nil {
		panic("pool ping: " + err.Error())
	}
	defer pool.Close()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		panic("new kafka producer: " + err.Error())
	}
	defer producer.Close()

	admin := kadm.NewClient(producer)

	if _, err := admin.CreateTopic(ctx, partitions, replicationFactor, nil, topic); err != nil &&
		!errors.Is(err, kerr.TopicAlreadyExists) {
		panic("create topic: " + err.Error())
	}

	payload, err := json.Marshal(alert{SensorID: "sensorID", Value: 10.2})
	if err != nil {
		panic("marshal: " + err.Error())
	}

	var wg sync.WaitGroup

	r := relay{pool: pool, producer: producer}
	wg.Go(func() { r.run(ctx) })

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Committing is done by hand, after the handler has succeeded. On the
		// automatic setting an offset can be committed for a record whose
		// handler then fails, which loses the event.
		kgo.DisableAutoCommit(),
		// A group joining a topic it has never read starts at the beginning, so
		// a consumer added later still sees the history.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		panic("new kafka client: " + err.Error())
	}
	defer consumer.Close()

	wg.Go(func() { runConsumer(ctx, consumer) })

	spanCtx, span := otel.Tracer(serviceName).Start(ctx, "main")
	slog.InfoContext(spanCtx, "create span")

	queries := db.New(pool)
	if _, err := queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		Key:          "sensorID",
		Payload:      payload,
		TraceContext: traceContextJSON(spanCtx),
	}); err != nil {
		panic("create outbox event: " + err.Error())
	}

	span.End()

	<-ctx.Done()
	slog.Info("shutting down")

	wg.Wait()
}
