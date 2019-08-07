all:
	make -C db
	make -C grpc-server
	make -C grpc-gateway

clean:
	make -C db clean
	make -C grpc-server clean
	make -C grpc-gateway clean
