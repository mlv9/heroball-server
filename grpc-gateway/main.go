package main

import (
	"context"
	"log"
	"net/http"

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

	err := pb.RegisterHeroBallServiceHandlerFromEndpoint(ctx, mux, "grpc-server:8000", opts)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(http.ListenAndServeTLS(":443", "/etc/letsencrypt/live/api.heroball.xyz/fullchain.pem", "/etc/letsencrypt/live/api.heroball.xyz/privkey.pem", mux))
}
