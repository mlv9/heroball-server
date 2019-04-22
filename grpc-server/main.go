package main

import (
	"fmt"
	"log"
	"os"
)

func main() {

	log.Printf("Connecting to DB at %v\n", os.Getenv("POSTGRES_HOST"))
	connStr := fmt.Sprintf("user=%v password=%v host=%v dbname=%v sslmode=disable", os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_DBNAME"))

	/* create the GRPC server */
	server, err := NewHeroBallService(connStr)

	if err != nil {
		log.Printf("Error creating service: %v\n", err)
		return
	}

	log.Printf("Binding GRPC to %v\n", os.Getenv("GRPC_BIND_ADDR"))

	if err := server.Serve(os.Getenv("GRPC_BIND_ADDR")); err != nil {
		log.Fatal(err)
		return
	}
}
