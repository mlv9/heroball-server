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

func (database *HeroBallDatabase) GetGameInfo(gameId int32) (*pb.GameInfo, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	gameInfo := &pb.GameInfo{}

	game, err := database.GetGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game: %v", err)
	}

	/* get players in the game */
	playerIds, err := database.GetPlayersInGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting players in game: %v", err)
	}

	players := make([]*pb.PlayerGameStats, 0)

	for _, playerId := range playerIds {
		playerStat, err := database.GetPlayerGameStats(playerId, gameId)

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
		PlayerId:    playerId,
		RecentGames: make([]*pb.PlayerGame, 0),
	}

	profile, err := database.GetPlayerProfile(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player profile: %v", err)
	}

	info.Profile = profile

	gameIds, err := database.GetRecentPlayerGames(playerId, recentGameCount)

	if err != nil {
		return nil, fmt.Errorf("Error getting recent games for player: %v", err)
	}

	for _, gameId := range gameIds {
		game, err := database.GetPlayerGame(playerId, gameId)

		if err != nil {
			return nil, fmt.Errorf("Error getting player game stats: %v", err)
		}

		info.RecentGames = append(info.RecentGames, game)

	}

	return info, nil
}

func (database *HeroBallDatabase) GetPlayerProfile(playerId int32) (*pb.PlayerProfile, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	profile := &pb.PlayerProfile{}

	err := database.db.QueryRow(`
		SELECT
			Name,
			YearStarted,
			Position,
			Description
		FROM
			Players
		WHERE
			PlayerId = $1`,
		playerId).Scan(
		&profile.Name,
		&profile.YearStarted,
		&profile.Position,
		&profile.Description)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That playerId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return profile, nil
}

func (database *HeroBallDatabase) GetPlayerGame(playerId int32, gameId int32) (*pb.PlayerGame, error) {

	/* get stats to begin with */
	pgStats, err := database.GetPlayerGameStats(playerId, gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player game stats: %v", err)
	}

	/* get the game */
	game, err := database.GetGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game: %v", err)
	}

	return &pb.PlayerGame{
		Game:  game,
		Stats: pgStats,
	}, nil
}

func (database *HeroBallDatabase) GetPlayerGameStats(playerId int32, gameId int32) (*pb.PlayerGameStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	/* TODO QUERY get team and statsId */
	var teamId int32
	var statsId int32

	err := database.db.QueryRow(`
		SELECT
			TeamId,
			StatsId
		FROM
			PlayerGames
		WHERE PlayerId = $1 AND GameId = $2`, playerId, gameId).Scan(&teamId, &statsId)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That player did not play in the game")
	}

	if err != nil {
		return nil, err
	}

	/* get stats */
	stats, err := database.GetStats(statsId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player stats: %v", err)
	}

	pgStats := &pb.PlayerGameStats{
		PlayerId: playerId,
		TeamId:   teamId,
		Stats:    stats,
	}

	return pgStats, nil
}

func (database *HeroBallDatabase) GetStats(statsId int32) (*pb.Stats, error) {

	stats := &pb.Stats{}

	err := database.db.QueryRow(`
		SELECT
			Stats.StatsId,
			Stats.TwoPointFGA,
			Stats.TwoPointFGM,
			Stats.ThreePointFGA, 
			Stats.ThreePointFGM,
			Stats.FreeThrowsAttempted,
			Stats.FreeThrowsMade,
			Stats.OffensiveRebounds,
			Stats.DefensiveRebounds,
			Stats.Assists,
			Stats.Blocks,
			Stats.Steals,
			Stats.Turnovers,
			Stats.RegularFoulsForced,
			Stats.RegularFoulsCommitted,
			Stats.TechnicalFoulsCommitted,
			Stats.MinutesPlayed
		FROM
			Stats
		WHERE StatsId = $1`,
		statsId).Scan(
		&stats.StatsId,
		&stats.TwoPointFGA,
		&stats.TwoPointFGM,
		&stats.ThreePointFGA,
		&stats.ThreePointFGM,
		&stats.FreeThrowsAttempted,
		&stats.FreeThrowsMade,
		&stats.OffensiveRebounds,
		&stats.DefensiveRebounds,
		&stats.Assists,
		&stats.Blocks,
		&stats.Steals,
		&stats.Turnovers,
		&stats.RegularFoulsForced,
		&stats.RegularFoulsCommitted,
		&stats.TechnicalFoulsCommitted,
		&stats.MinutesPlayed)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That statsId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (database *HeroBallDatabase) GetGame(gameId int32) (*pb.Game, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	game := &pb.Game{
		GameId:      gameId,
		HomeTeam:    &pb.Team{},
		AwayTeam:    &pb.Team{},
		Location:    &pb.Location{},
		Competition: &pb.Competition{},
	}

	err := database.db.QueryRow(`
		SELECT
			HomeTeams.TeamId,
			HomeTeams.Name,
			AwayTeams.TeamId,
			AwayTeams.Name,
			Locations.LocationId,
			Locations.Name,
			Competitions.CompetitionId,
			Competitions.Name,
			Competitions.SubCompetition,
			Games.GameTime	
		FROM
			Games
		LEFT JOIN
			Teams HomeTeams ON Games.HomeTeamId = HomeTeams.TeamId
		LEFT JOIN
			Teams AwayTeams ON Games.AwayTeamId = AwayTeams.TeamId			
		LEFT JOIN
			Locations ON Games.LocationId = Locations.LocationId
		LEFT JOIN
			Competitions ON Games.CompetitionId = Competitions.CompetitionId
		WHERE
			GameId = $1`,
		gameId).Scan(
		&game.HomeTeam.TeamId,
		&game.HomeTeam.Name,
		&game.AwayTeam.TeamId,
		&game.AwayTeam.Name,
		&game.Location.LocationId,
		&game.Location.Name,
		&game.Competition.CompetitionId,
		&game.Competition.Name,
		&game.Competition.SubCompetition,
		&game.GameTime)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return game, nil
}

func (database *HeroBallDatabase) GetPlayersInGame(gameId int32) ([]int32, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	playerIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			PlayerId
		FROM
			PlayerGames
		WHERE
			GameId = $1
			`,
		gameId)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var playerId int32

		/* now to scan them all */
		err = rows.Scan(&playerId)

		if err != nil {
			return nil, err
		}

		playerIds = append(playerIds, playerId)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return playerIds, nil
}

/* takes a playerId, return an array of recent games, up to maxnumber */
func (database *HeroBallDatabase) GetRecentPlayerGames(playerId int32, maxCount int32) ([]int32, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if maxCount <= 0 {
		return nil, fmt.Errorf("Invalid maxCount")
	}

	gameIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			Games.GameId
		FROM
			Games
		LEFT JOIN
			PlayerGames ON PlayerGames.GameId = Games.GameId
		WHERE
			PlayerGames.PlayerId = $1
		ORDER BY
			Games.GameTime DESC
		LIMIT $2
			`,
		playerId, maxCount)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That playerId does not exist")
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var gameId int32

		/* now to scan them all */
		err = rows.Scan(&gameId)

		if err != nil {
			return nil, err
		}

		gameIds = append(gameIds, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return gameIds, nil
}
