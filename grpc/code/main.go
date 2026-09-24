package main

import (
	"context"
	"fmt"
	"grpc/proto"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = ":8000"

func main() {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	proto.RegisterAlertsServer(s, &server{})

	go func() {
		fmt.Printf("server listening at %s\n", addr)
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Second)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := proto.NewAlertsClient(conn)
	res, err := client.GetAlert(context.Background(), &proto.GetAlertRequest{Id: 1})
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}

type server struct {
	proto.UnimplementedAlertsServer
}

func (s *server) GetAlert(ctx context.Context, in *proto.GetAlertRequest) (*proto.GetAlertResponse, error) {
	return &proto.GetAlertResponse{
		Alert: &proto.Alert{Id: in.GetId()},
		Ok:    true}, nil
}
