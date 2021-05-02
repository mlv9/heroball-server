package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	pb "github.com/mlv9/protobuf"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{OrigName: true, EmitDefaults: true}))
	opts := []grpc.DialOption{grpc.WithInsecure()}

	serverLocation, exists := os.LookupEnv("GRPC_SERVER")

	if !exists {
		log.Fatal("Unable to locate GRPC_SERVER env")
		return
	}

	serverPort, exists := os.LookupEnv("GRPC_PORT")

	if !exists {
		log.Fatal("Unable to locate GRPC_PORT env")
		return
	}

	gatewayBind, exists := os.LookupEnv("GATEWAY_BIND")

	if !exists {
		log.Fatal("Unable to locate GATEWAY_BIND env")
		return
	}

	err := pb.RegisterHeroBallServiceHandlerFromEndpoint(ctx, mux, fmt.Sprintf("%v:%v", serverLocation, serverPort), opts)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(http.ListenAndServe(gatewayBind, mux))
}
