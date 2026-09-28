package main

import (
	"context"
	"golang/grpc/proto"
	"golang/logger"
	"log/slog"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = ":8000"

func main() {
	logger.Setup()

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

	// The connection is lazy, until we don't do the first request it doesn't connect.
	// If the connection fails it will retry with backoff.
	client := proto.NewAlertsClient(conn)
	alert, err := client.GetAlert(ctx, &proto.GetAlertRequest{Id: 1})
	slog.InfoContext(ctx, "client get alert",
		"alert", alert.Alert,
		"ok", alert.Ok,
		"err", err)
}

type server struct {
	// If we don't implement the method it gets a default "not implemented" err.
	// proto.UnimplementedAlertsServer
	// If we don't implement the method it doesn't compile.
	proto.UnsafeAlertsServer
}

func (s *server) GetAlert(ctx context.Context, in *proto.GetAlertRequest) (*proto.GetAlertResponse, error) {
	slog.InfoContext(ctx, "server get alert", "id", in.Id)
	return &proto.GetAlertResponse{
		Alert: &proto.Alert{Id: in.GetId()},
		Ok:    true}, nil
}
