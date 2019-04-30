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

	gameIds, err := database.getGamesForTeam(teamId)

	if err != nil {
		return nil, err
	}

	teamInfo.GameIds = gameIds

	/* now get some recent games */
	recentGameIds := gameIds

	if len(gameIds) > recentGameCount {
		recentGameIds = gameIds[:recentGameCount]
	}

	recentGames, err := database.getGames(recentGameIds)

	if err != nil {
		return nil, err
	}

	teamInfo.RecentGames = recentGames

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

	/* get the comp */
	compId, err := database.getCompetitionForTeam(teamId)

	if err != nil {
		return nil, err
	}

	competition, err := database.getCompetition(compId)

	if err != nil {
		return nil, err
	}

	teamInfo.Competition = competition

	statLeaders, err := database.getStatsLeadersForTeam(teamId)

	if err != nil {
		return nil, err
	}

	teamInfo.StatsLeaders = statLeaders

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

	teams, err := database.getStandingsForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	compInfo.Teams = teams

	statLeaders, err := database.getStatsLeadersForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	compInfo.StatsLeaders = statLeaders

	/* get all GameIds */
	gameIds, err := database.getGamesForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	compInfo.GameIds = gameIds

	/* now get some recent games */
	recentGameIds := gameIds

	if len(gameIds) > recentGameCount {
		recentGameIds = gameIds[:recentGameCount]
	}

	recentGames, err := database.getGames(recentGameIds)

	if err != nil {
		return nil, err
	}

	compInfo.RecentGames = recentGames

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

		playerStat, err := database.getPlayerStatsForGame(playerId, gameId)

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

	/* TODO - we could do this client side with team stats */
	totalStats, err := database.getPlayerTotalStatsForAllTime(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting all stats for player: %v", err)
	}

	info.StatsAllTime = totalStats

	/* get games */
	gameIds, err := database.getGamesForPlayer(playerId)

	if err != nil {
		return nil, err
	}

	info.GameIds = gameIds

	/* now get some recent games */
	recentGameIds := gameIds

	if len(gameIds) > recentGameCount {
		recentGameIds = gameIds[:recentGameCount]
	}

	recentGames, err := database.getGames(recentGameIds)

	if err != nil {
		return nil, err
	}

	info.RecentGames = recentGames

	recentStats := make([]*pb.PlayerGameStats, 0)

	for _, game := range info.RecentGames {

		playerStats, err := database.getPlayerStatsForGame(playerId, game.GameId)

		if err != nil {
			return nil, err
		}

		recentStats = append(recentStats, playerStats)
	}

	info.RecentStats = recentStats

	return info, nil
}
