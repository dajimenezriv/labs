package main

import (
	"context"
	"fmt"
	"grpc/proto"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = ":8000"

func main() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	// Server side: reads `traceparent` from the metadata and puts the span in ctx.
	s := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	proto.RegisterAlertsServer(s, &server{})

	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Second)

	// Client side: starts a child span and writes `traceparent` into the metadata.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Add the traceID to the context.
	ctx, span := otel.Tracer("client").Start(context.Background(), "main")
	defer span.End()
	fmt.Println("client trace_id:", span.SpanContext().TraceID())

	// The connection is lazy, until we don't do the first request it doesn't connect.
	// If the connection fails it will retry with backoff.
	client := proto.NewAlertsClient(conn)
	res, err := client.GetAlert(ctx, &proto.GetAlertRequest{Id: 1})
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

type server struct {
	// If we don't implement the method it gets a default "not implemented" err.
	// proto.UnimplementedAlertsServer
	// If we don't implement the method it doesn't compile.
	proto.UnsafeAlertsServer
}

func (s *server) GetAlert(ctx context.Context, in *proto.GetAlertRequest) (*proto.GetAlertResponse, error) {
	fmt.Println("server trace_id:", trace.SpanContextFromContext(ctx).TraceID())
	return &proto.GetAlertResponse{
		Alert: &proto.Alert{Id: in.GetId()},
		Ok:    true}, nil
}
