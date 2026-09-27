package main

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func injectTraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func extractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

func traceContextJSON(ctx context.Context) []byte {
	carrier := injectTraceContext(ctx)
	if len(carrier) == 0 {
		return nil
	}
	encoded, err := json.Marshal(carrier)
	if err != nil {
		return nil
	}
	return encoded
}

func contextFromJSON(ctx context.Context, encoded []byte) context.Context {
	if len(encoded) == 0 {
		return ctx
	}
	var carrier map[string]string
	if err := json.Unmarshal(encoded, &carrier); err != nil {
		return ctx
	}
	return extractTraceContext(ctx, carrier)
}
