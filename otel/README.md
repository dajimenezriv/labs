# Observability

- [Prometheus](#prometheus)
  - [Metrics](#metrics)
  - [Check metrics](#check-metrics)
- [Tempo](#tempo)
  - [The wire format](#the-wire-format)
  - [Who creates spans](#who-creates-spans)
    - [Server span](#server-span)
    - [Client span](#client-span)
    - [Manual span](#manual-span)
  - [How to propagate the trace through the wire?](#how-to-propagate-the-trace-through-the-wire)
  - [How to provide the spans to Tempo?](#how-to-provide-the-spans-to-tempo)
  - [Sampling propagates](#sampling-propagates)
  - [Kafka and the outbox](#kafka-and-the-outbox)

How are these metrics stored in the app?
What does an SRE? Looks at dashboards? Drilldown? Alerts?
What we do if we don't have a sample because we are not capturing all?
How are spans and so stored in tempo?

Reasons to keep `request_id` and `trace_id`:

- **Sampling**: at 1% sampling, a `trace_id` in a log line usually resolves to nothing. Then what's the point of the trace_id???
- **Retention mismatch**: traces are expensive and usually kept less time than logs.
- **Granularity**: a client could use the same `trace_id` to span multiple HTTP requests.

1. Start in Loki filtered `level="error"`.
2. Grab `trace_id`.

## Prometheus

- Collects and stores real-time numerical performance data.
- It scraps an HTTP /metrics endpoint at a fixed interval (usually 15s). Targets must be discoverable (static config, DNS, Docker).

### Metrics

- **Counter**: just increases. Use `rate()` or `increase()`.
- **Gauge**: goes up and down. Queue depth, memory, connections in use.
- **Histogram**: cumulative buckets (`_bucket` with an `le` label) plus `_sum` and `_count`.

| Group        | Count | Who wrote it                           |
| ------------ | ----- | -------------------------------------- |
| `go_*`       | 31    | Go runtime collector.                  |
| `process_*`  | 9     | Linux `/proc`.                         |
| `scrape_*`   | 5     | Prometheus, about the scrape itself.   |
| `promhttp_*` | 2     | The metrics endpoint measuring itself. |

### Check metrics

- `docker compose stop payments`: watch payments instance metrics.
- Hammer the gateway past its limit: `hey -z 30s -c 50 http://localhost:8000/identity/docs`.
- `docker compose stop kafka` → outbox_pending_events climbs and never recovers.
- `docker compose stop payments` while orders are being placed → `kafka_consumer_lag{group="payments"}` climbs; note it goes stale rather than climbing once the last scrape is gone, which is why the alert has to cover absence too.
- Curl a service directly with random path segments → watch `prometheus_tsdb_head_series` climb and not come back down. That's the cardinality bomb, self-inflicted on purpose.
- Slow something down past 5s → watch p99 stop moving while things get genuinely worse.

## Tempo

A **span** is one unit of work: a name, a start time, a duration, attributes, its own span id, its parent's span id and a trace id. The span is carried inside the `Context`.

A **trace** is the set of spans sharing a trace id, which Tempo assembles at query time by grouping on that field. So there is no such thing as starting a trace, we start a span and then we have the trace id.

```go
// TraceID returns a trace id if the context carries a span. No span in the context, no
// id, empty string, untraced log line.
func TraceID(ctx context.Context) string {
  spanContext := trace.SpanContextFromContext(ctx)
  if !spanContext.IsValid() {
    return ""
  }
  return spanContext.TraceID().String()
}
```

### The wire format

The span travelling through the network has this format:

```
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
^version  ^trace id (16 bytes)      ^span id (8)     ^flags
```

Three things travel, not one:

- **trace id**: shared identity, the join key for Loki and the grouping key for Tempo.
- **span id**: the _parent pointer_. This is what makes the next hop's span a child instead of a sibling.
- **flags**: the sampling decision.

No span ever crosses the wire. Each service builds its own; what crosses is a reference to the parent.

### Who creates spans

| Wrapper                 | Span it makes | Propagator direction      |
| ----------------------- | ------------- | ------------------------- |
| `otelhttp.NewHandler`   | server span   | `Extract` (read incoming) |
| `otelhttp.NewTransport` | client span   | `Inject` (write outgoing) |
| `Tracer(name).Start`    | manual span   | none                      |

#### Server span

```go
// This is done inside a middleware. With `otelhttp.NewHandler` we start a span in the
// server which is used through the whole endpoint function.
handler := otelhttp.NewHandler(handler, "",
  otelhttp.WithTracerProvider(tel.Provider),
  otelhttp.WithPropagators(tel.Propagator),
  otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
    return r.Method + " " + r.Path
  }),
)

// Summary of what happens inside the otelhttp.NewHandler.
func otelhttp.NewHandler() http.Handler {
  propagator.Extract(ctx, r.Header) // Remote span (trace id + parent span id) into ctx.
  tracer.Start(ctx, "POST /orders") // New span, inherits trace id, new span id.
  next.ServeHTTP(w, r.WithContext(ctx))
}
```

That `r.WithContext` is the moment. Everything after it — the `slog` handler adding `trace_id`, `InjectTraceContext`, the outbox row — only reads what those lines put there.

We can setup how to propagate the trace context and how to provide it to Tempo by using `WithPropagators` and `WithTracerProvider` (see below what they do). If we omit those fields then otel falls back to the global provider.

#### Client span

```go
client := http.Client{
  Timeout: 5 * time.Second,
  Transport: otelhttp.NewTransport(
    http.DefaultTransport,
    otelhttp.WithTracerProvider(tel.Provider),
    otelhttp.WithPropagators(tel.Propagator),
  ),
}
```

#### Manual span

```go
ctx, span := t.Provider.Tracer(packageName).Start(ctx, serviceName)
defer span.End()
```

### How to propagate the trace through the wire?

What is the baggage header for?

```go
otelhttp.WithPropagators(
  propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
  )
)
```

The `propagation.TraceContext` and `propagation.Baggage` structs have an `Extract` and `Inject` methods that take the span from the carrier or put the span into the carrier.

```go
// Inject puts the trace context from ctx into carrier.
func (propagation.TraceContext) Inject(ctx context.Context, carrier TextMapCarrier) {
  spanContext := trace.SpanContextFromContext(ctx)
  var sb strings.Builder // Take span values from `spanContext` and construct the header.
  carrier.Set(traceparentHeader, sb.String())
}

// Extract puts the trace context from carrier into ctx.
func (tc TraceContext) Extract(ctx context.Context, carrier TextMapCarrier) context.Context {
	sc := tc.extract(carrier)
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}
```

In the case of http the carrier is an http.Request:

- when `Inject` we take the span from the context and add the traceparent header into a request.
- when `Extract` we take the traceparent header from the request and create a new span in the context.

### How to provide the spans to Tempo?

```go
exporter, _ := otlptracegrpc.New(ctx,
  otlptracegrpc.WithEndpoint("tempo:port"),
  otlptracegrpc.WithInsecure(),
)

// The default resource provides:
// - A default SDK version.
// - A default schema URL.
// - A default service name: "unknown_service:" + filepath.Base(executable).
// We want to keep all defaults and override the service name.
res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(
  semconv.ServiceName(serviceName),
))

provider := sdktrace.NewTracerProvider(
  // Every 2 seconds we send the spans.
  sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
  sdktrace.WithResource(res),
  // Include all samples. In production we reduce this.
  sdktrace.WithSampler(sdktrace.AlwaysSample()),
)
```

### Sampling propagates

```go
// Preserve only the spec-defined flags: sampled (0x01) and random (0x02).
flags := sc.TraceFlags() & (trace.FlagsSampled | trace.FlagsRandom)
```

The trailing `-01` is the sampled flag, and it travels. A downstream service does not decide independently whether to export; it just checks the flag.

### Kafka and the outbox

Nothing is automatic here; both halves are explicit.

`TraceContextJSON` serialises the whole `traceparent` into the outbox row — **not just the trace id**. The span id in there is the producing span's, and it is what makes the consumer span a child. Store only the trace id and the tree collapses into a flat list.

On the way out, `ExtractTraceContext` rebuilds the parent from the record headers _before_ starting the consumer span:

```go
handleCtx := tel.ExtractTraceContext(ctx, r.TraceContext)
spanName := fmt.Sprintf("consume %s %s", record.Topic, r.EventType)
handleCtx, span := tel.Tracer(c.group).Start(handleCtx, spanName)
span.SetAttributes(
  attribute.String("messaging.system", "kafka"),
  attribute.String("messaging.destination.name", record.Topic),
  attribute.String("messaging.kafka.consumer.group", c.group),
)
defer span.End()
```
