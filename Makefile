all:
	make -C db
	make -C grpc-server
	make -C grpc-gateway
