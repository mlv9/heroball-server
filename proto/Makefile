# We need to add the protoc-gen-dart binaries

all:
	protoc -I$(PWD) --go_out=plugins=grpc:. heroball.proto
	protoc -I$(PWD)  --dart_out=grpc:. heroball.proto
	ls *.pb.go | xargs -n1 -IX bash -c 'sed s/,omitempty// X > X.tmp && mv X{.tmp,}'
