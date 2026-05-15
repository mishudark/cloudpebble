package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/mishudark/cloudpebble/pkg/bigtable"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":9000", "gRPC listen address")
	dataDir := flag.String("data-dir", "/tmp/cloudpebble-bigtable", "local data directory")
	objectDir := flag.String("object-dir", "/tmp/cloudpebble-bigtable-obj", "object storage directory")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0750); err != nil {
		log.Fatalf("creating data dir: %v", err)
	}
	if err := os.MkdirAll(*objectDir, 0750); err != nil {
		log.Fatalf("creating object dir: %v", err)
	}

	store, err := local.New(*objectDir)
	if err != nil {
		log.Fatalf("creating local store: %v", err)
	}

	srv := bigtable.NewServer(*dataDir, store)

	grpcServer := grpc.NewServer()
	bigtablepb.RegisterBigtableServer(grpcServer, srv)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listening: %v", err)
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nShutting down...")
		grpcServer.GracefulStop()
		_ = srv.Close()
		os.Exit(0)
	}()

	fmt.Printf("Bigtable gRPC server listening on %s\n", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serving: %v", err)
	}
}
