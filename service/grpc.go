package heroball

import (
	pb "basketball/server/proto"
	"context"
	"log"
	"net"

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

func (hb *HeroBall) GetCompetitions(context.Context, *pb.Empty) (*pb.GetCompetitionsResponse, error) {

	return nil, nil
}

func (hb *HeroBall) GetTeamsForCompetition(context.Context, *pb.GetTeamsForCompetitionRequest) (*pb.GetTeamsForCompetitionResponse, error) {

	return nil, nil
}

func (hb *HeroBall) GetTeam(context.Context, *pb.GetTeamRequest) (*pb.Team, error) {

	return nil, nil
}

func (hb *HeroBall) GetGamesForTeam(context.Context, *pb.GetGamesForTeamRequest) (*pb.GetGamesForTeamResponse, error) {

	return nil, nil
}

func (hb *HeroBall) GetPlayer(context.Context, *pb.GetPlayerRequest) (*pb.Player, error) {

	return nil, nil
}

func (hb *HeroBall) GetTopAverageStats(context.Context, *pb.TopAverageStatsSelector) (*pb.TopAverageStatsResponse, error) {

	return nil, nil
}

func (hb *HeroBall) GetGamesOnDate(context.Context, *pb.GetGamesOnDateRequest) (*pb.GetGamesOnDateResponse, error) {

	return nil, nil
}

func (hb *HeroBall) GetGame(context.Context, *pb.GetPlayerRequest) (*pb.Game, error) {
	return nil, nil
}

func (hb *HeroBall) GetGamesForPlayer(context.Context, *pb.GetGamesForPlayerRequest) (*pb.GetGamesForPlayerResponse, error) {
	return nil, nil
}

func (hb *HeroBall) GetPlayerGameStats(context.Context, *pb.GameStatsSelector) (*pb.PlayerGameStats, error) {
	return nil, nil
}

func (hb *HeroBall) GetPlayerAverageStats(context.Context, *pb.AverageStatsSelector) (*pb.PlayerAverageStats, error) {
	return nil, nil
}

func (hb *HeroBall) AddPlayerGameStats(context.Context, *pb.PlayerGameStats) (*pb.Empty, error) {
	return nil, nil
}
