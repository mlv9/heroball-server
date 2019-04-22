all: grpc docker

grpc:
	CGO_ENABLED=0 go build -o grpc-server .
docker:
	docker build -t heroball-grpc:latest .
clean:
	rm grpc-server
