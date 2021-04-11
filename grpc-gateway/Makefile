
all: gateway docker

gateway:
	CGO_ENABLED=0 go build -o grpc-gateway .
docker:
	docker build -t grpc-gateway:latest .
clean:
	rm grpc-gateway
