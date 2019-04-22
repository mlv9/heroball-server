all:
	make -C db
	make -C grpc-service
	make -C grpc-gateway