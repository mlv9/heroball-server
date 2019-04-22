
all: gateway docker

gateway:
	CGO_ENABLED=0 go build -o gateway .
docker:
	docker build -t heroball-gateway:latest .
clean:
	rm gateway
