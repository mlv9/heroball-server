package main

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

func NewHeroBallService(dbstring string) (*HeroBall, error) {

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

func (hb *HeroBall) GetGames(context context.Context, request *pb.GetGamesRequest) (*pb.GamesCursor, error) {

	/* pass to database layer */
	games, err := hb.db.GetGamesCursor(request.GetOffset(), request.GetCount(), request.GetFilter())

	if err != nil {
		log.Printf("Error getting games cursor: %v", err)
		return nil, err
	}

	return games, nil
}

func (hb *HeroBall) GetPlayers(context context.Context, request *pb.GetPlayersRequest) (*pb.PlayersCursor, error) {

	/* pass to database layer */
	players, err := hb.db.GetPlayersCursor(request.GetOffset(), request.GetCount(), request.GetFilter())

	if err != nil {
		log.Printf("Error getting players cursor: %v", err)
		return nil, err
	}

	return players, nil
}

func (hb *HeroBall) GetCompetitionInfo(context context.Context, request *pb.GetCompetitionInfoRequest) (*pb.CompetitionInfo, error) {

	/* pass to database layer */
	info, err := hb.db.GetCompetitionInfo(request.GetCompetitionId())

	if err != nil {
		log.Printf("Error getting competition info: %v", err)
		return nil, err
	}

	return info, nil
}

func (hb *HeroBall) GetGameInfo(context context.Context, request *pb.GetGameInfoRequest) (*pb.GameInfo, error) {

	/* pass to database layer */
	info, err := hb.db.GetGameInfo(request.GetGameId())

	if err != nil {
		log.Printf("Error getting game info: %v", err)
		return nil, err
	}

	return info, nil
}

func (hb *HeroBall) GetTeamInfo(context context.Context, request *pb.GetTeamInfoRequest) (*pb.TeamInfo, error) {

	/* pass to database layer */
	info, err := hb.db.GetTeamInfo(request.GetTeamId())

	if err != nil {
		log.Printf("Error getting team info: %v", err)
		return nil, err
	}

	return info, nil
}

func (hb *HeroBall) GetHeroBallMetadata(context context.Context, request *pb.GetHeroBallMetadataRequest) (*pb.HeroBallMetadata, error) {

	values, err := hb.db.GetHeroBallMetadata(request)

	if err != nil {
		log.Printf("Error getting heroball metadata: %v", err)
		return nil, err
	}

	return values, nil
}

func (hb *HeroBall) GetPlayerAverageStats(context context.Context, request *pb.GetPlayerAverageStatsRequest) (*pb.GetPlayerAverageStatsResponse, error) {

	values, err := hb.db.GetPlayerAverageStats(request)

	if err != nil {
		log.Printf("Error getting stats: %v", err)
		return nil, err
	}

	return values, nil
}

func (hb *HeroBall) GetPlayerGamesStats(context context.Context, request *pb.GetPlayerGamesStatsRequest) (*pb.GetPlayerGamesStatsResponse, error) {

	values, err := hb.db.GetPlayerGamesStats(request)

	if err != nil {
		log.Printf("Error getting stats: %v", err)
		return nil, err
	}

	return values, nil
}
