package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	otelprom "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func setupOTel(ctx context.Context) (func(context.Context) error, error) {
	// One id per replica. Without it, every replica writes the same metric series and their
	// cumulative counters overwrite each other. The hostname is the machine name here and the
	// replica name in Container Apps.
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName("users"),
		semconv.ServiceInstanceID(host),
	))
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	// Without a propagator, every service starts a new trace instead of continuing the caller's.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	metricExp, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	// The bridge reads client_golang's default registry (go_* and process_* collectors) on every
	// export and sends it over OTLP with the rest, so the runtime metrics keep their Prometheus names.
	reader := sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithProducer(otelprom.NewMetricProducer()))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)

	logExp, err := otlploghttp.New(ctx, otlploghttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))
	otel.SetLoggerProvider(lp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

func monitor(ctx context.Context) {
	interval := time.Second
	t := time.NewTicker(interval)
	defer t.Stop()

	var m runtime.MemStats
	var prev, ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &prev)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runtime.ReadMemStats(&m)
			syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

			used := (ru.Utime.Nano() + ru.Stime.Nano()) - (prev.Utime.Nano() + prev.Stime.Nano())
			prev = ru

			fmt.Printf("heap=%dMiB sys=%dMiB gc=%d goroutines=%d cpu=%.0fm\n",
				m.HeapAlloc>>20,
				m.Sys>>20,
				m.NumGC,
				runtime.NumGoroutine(),
				float64(used)/float64(interval)*1000)
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := setupOTel(ctx)
	if err != nil {
		panic(err)
	}
	// Flushes the batched spans, logs and the last metric reading before exit.
	defer shutdown(context.Background())

	go monitor(ctx)

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
