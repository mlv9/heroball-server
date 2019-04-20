package heroball

import (
	pb "basketball/server/proto"
	"context"
	"database/sql"
)

type HeroBallDatabase struct {
	connectionString string
	db               *sql.DB
}

func NewHeroBallDatabase(connStr string) (*HeroBallDatabase, error) {
	return &HeroBallDatabase{}, nil
}

func (database *HeroBallDatabase) Connect(connStr string, db *sql.DB) error {
	if db != nil {
		database.db = db
		return nil
	}
	database.connectionString = connStr
	db, err := sql.Open("postgres", database.connectionString)
	if err != nil {
		return err
	}
	database.db = db
	return nil
}

func (db *HeroBallDatabase) GetCompetitions() (*pb.GetCompetitionsResponse, error) {

	return nil, nil
}

func (db *HeroBallDatabase) GetTeamsForCompetition(context.Context, *pb.GetTeamsForCompetitionRequest) (*pb.GetTeamsForCompetitionResponse, error) {

	return nil, nil
}

func (db *HeroBallDatabase) GetTeam(context.Context, *pb.GetTeamRequest) (*pb.Team, error) {

	return nil, nil
}

func (db *HeroBallDatabase) GetGamesForTeam(context.Context, *pb.GetGamesForTeamRequest) (*pb.GetGamesForTeamResponse, error) {

	return nil, nil
}

func (db *HeroBallDatabase) GetPlayer(context.Context, *pb.GetPlayerRequest) (*pb.Player, error) {

	return nil, nil
}


func (db *HeroBallDatabase) GetGamesOnDate(context.Context, *pb.GetGamesOnDateRequest) (*pb.GetGamesOnDateResponse, error) {

	return nil, nil
}

func (db *HeroBallDatabase) GetGame(context.Context, *pb.GetPlayerRequest) (*pb.Game, error) {
	return nil, nil
}

func (db *HeroBallDatabase) GetGamesForPlayer(context.Context, *pb.GetGamesForPlayerRequest) (*pb.GetGamesForPlayerResponse, error) {
	return nil, nil
}

func (db *HeroBallDatabase) GetPlayerGameStats(context.Context, *pb.GameStatsSelector) (*pb.PlayerGameStats, error) {
	return nil, nil
}

func (db *HeroBallDatabase) GetPlayerAverageStats(context.Context, *pb.AverageStatsSelector) (*pb.PlayerAverageStats, error) {
	return nil, nil
}

func (db *HeroBallDatabase) AddPlayerGameStats(context.Context, *pb.PlayerGameStats) (*pb.Empty, error) {
	return nil, nil
}
