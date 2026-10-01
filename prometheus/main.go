package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// setupOTel wires traces, metrics and logs to push OTLP/HTTP to the local Alloy (localhost:4318).
// Alloy holds the Grafana Cloud credentials; the app holds none.
func setupOTel(ctx context.Context) (func(context.Context) error, error) {
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName("users")))
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)

	logExp, err := otlploghttp.New(ctx, otlploghttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))
	global.SetLoggerProvider(lp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	shutdown, err := setupOTel(ctx)
	if err != nil {
		panic(err)
	}
	// Flushes the batched spans, logs and the last metric reading before exit.
	defer shutdown(context.Background())

	logger := otelslog.NewLogger("users")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		logger.InfoContext(r.Context(), "user fetched", "id", r.PathValue("id"))
	})

	// otelhttp creates the server span and records http.server.request.duration.
	srv := &http.Server{Addr: ":8000", Handler: otelhttp.NewHandler(mux, "http")}
	go srv.ListenAndServe()

	<-ctx.Done()
	srv.Shutdown(context.Background())
}
