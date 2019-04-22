package heroball

import (
	"context"
	"log"
	"net"

	pb "github.com/heroballapp/server/protobuf"

	"google.golang.org/grpc"
)

type HeroBall struct {
	db *HeroBallDatabase
}

func NewHeroBallService(address string, dbstring string) (*HeroBall, error) {

	db, err := NewHeroBallDatabase(dbstring)

	if err != nil {
		return nil, err
	}

	service := &HeroBall{
		db: db,
	}

	return service, nil
}

func (hb *HeroBall) Serve(address string) error {

	grpcServer := grpc.NewServer()

	pb.RegisterHeroBallServiceServer(grpcServer, hb)

	lis, err := net.Listen("tcp", address)

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer.Serve(lis)

	return nil
}

func (hb *HeroBall) GetPlayerInfo(context context.Context, request *pb.GetPlayerInfoRequest) (*pb.PlayerInfo, error) {

	/* pass to database layer */
	info, err := hb.db.GetPlayerInfo(request.GetPlayerId())

	if err != nil {
		log.Printf("Error getting player info: %v", err)
		return nil, err
	}

	return info, nil
}
