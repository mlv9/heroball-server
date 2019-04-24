package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	pb "github.com/heroballapp/server/protobuf"
)

type HeroBallDatabase struct {
	connectionString string
	db               *sql.DB
}

const (
	recentGameCount = 3
)

func NewHeroBallDatabase(connStr string) (*HeroBallDatabase, error) {

	db := &HeroBallDatabase{
		connectionString: connStr,
	}

	err := db.connect()

	if err != nil {
		return nil, err
	}
	return db, nil
}

func (database *HeroBallDatabase) connect() error {

	db, err := sql.Open("postgres", database.connectionString)
	if err != nil {
		return err
	}
	database.db = db
	return nil
}

func (database *HeroBallDatabase) GetTeamInfo(teamId int32) (*pb.TeamInfo, error) {

	if teamId <= 0 {
		return nil, fmt.Errorf("Invalid teamId")
	}

	teamInfo := &pb.TeamInfo{}

	team, err := database.getTeam(teamId)

	if err != nil {
		return nil, err
	}

	teamInfo.Team = team

	playerIds, err := database.getPlayersForTeam(teamId)

	if err != nil {
		return nil, err
	}

	players := make([]*pb.PlayerInfo, 0)

	for _, playerId := range playerIds {

		playerInfo, err := database.GetPlayerInfo(playerId)

		if err != nil {
			return nil, fmt.Errorf("Error getting player info: %v", err)
		}

		players = append(players, playerInfo)
	}

	teamInfo.Players = players

	return teamInfo, nil
}

func (database *HeroBallDatabase) GetCompetitionInfo(competitionId int32) (*pb.CompetitionInfo, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	compInfo := &pb.CompetitionInfo{}

	/* now fill it out */
	comp, err := database.getCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	compInfo.Competition = comp

	/* now get locations */
	locationIds, err := database.getCompetitionLocations(competitionId)

	if err != nil {
		return nil, err
	}

	locations, err := database.getLocations(locationIds)

	if err != nil {
		return nil, err
	}

	compInfo.Locations = locations

	/* now get teams */
	teamIds, err := database.getCompetitionTeams(competitionId)

	if err != nil {
		return nil, err
	}

	teams, err := database.getTeams(teamIds)

	if err != nil {
		return nil, err
	}

	compInfo.Teams = teams

	return compInfo, nil
}

func (database *HeroBallDatabase) GetGameInfo(gameId int32) (*pb.GameInfo, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	gameInfo := &pb.GameInfo{}

	game, err := database.getGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game: %v", err)
	}

	/* get players in the game */
	playerIds, err := database.getPlayersInGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting players in game: %v", err)
	}

	players := make([]*pb.PlayerGameStats, 0)

	for _, playerId := range playerIds {
		playerStat, err := database.getPlayerGameStats(playerId, gameId)

		if err != nil {
			return nil, fmt.Errorf("Error getting player stats: %v", err)
		}

		players = append(players, playerStat)
	}

	gameInfo.Game = game
	gameInfo.PlayerStats = players

	return gameInfo, nil
}

func (database *HeroBallDatabase) GetPlayerInfo(playerId int32) (*pb.PlayerInfo, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	info := &pb.PlayerInfo{
		PlayerId: playerId,
		Teams:    make([]*pb.PlayerTeam, 0),
	}

	profile, err := database.getPlayerProfile(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player profile: %v", err)
	}

	info.Profile = profile

	teams, err := database.getAllTeamsForPlayer(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting teams for player: %v", err)
	}

	info.Teams = teams

	/* get games played */
	totalGameIds, err := database.getAllGamesByPlayer(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting all games for player: %v", err)
	}

	info.TotalGamesPlayed = int32(len(totalGameIds))

	totalStats, err := database.getPlayerTotalStatsForGames(playerId, totalGameIds)

	if err != nil {
		return nil, fmt.Errorf("Error getting all stats for player: %v", err)
	}

	info.StatTotals = totalStats

	return info, nil
}
