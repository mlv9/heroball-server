package main

import (
	"database/sql"
	"fmt"

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

func (db *HeroBallDatabase) GetPlayerInfo(playerId int32) (*pb.PlayerInfo, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	info := &pb.PlayerInfo{
		PlayerId:        playerId,
		RecentGameStats: make([]*pb.PlayerGameStats, 0),
	}

	profile, err := db.GetPlayerProfile(playerId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player profile: %v", err)
	}

	info.Profile = profile

	gameIds, err := db.GetRecentPlayerGames(playerId, recentGameCount)

	if err != nil {
		return nil, fmt.Errorf("Error getting recent games for player: %v", err)
	}

	for _, gameId := range gameIds {
		game, err := db.GetPlayerGameStats(playerId, gameId)

		if err != nil {
			return nil, fmt.Errorf("Error getting player game stats: %v", err)
		}

		info.RecentGameStats = append(info.RecentGameStats, game)

	}

	return info, nil
}

func (db *HeroBallDatabase) GetPlayerProfile(playerId int32) (*pb.PlayerProfile, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	profile := &pb.PlayerProfile{}

	err := db.db.QueryRow(`
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
		profile.Name,
		profile.YearStarted,
		profile.Position,
		profile.Description)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That playerId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return profile, nil
}

func (db *HeroBallDatabase) GetStats(playerId int32, gameId int32) (*pb.Stats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	stats := &pb.Stats{}

	err := db.db.QueryRow(`
		SELECT
			Stats.StatsId
			Stats.TwoPointFGA
			Stats.TwoPointFGM
			Stats.ThreePointFGA 
			Stats.ThreePointFGM 
			Stats.FreeThrowsAttempted
			Stats.FreeThrowsMade
			Stats.OffensiveRebounds
			Stats.DefensiveRebounds
			Stats.Assists
			Stats.Blocks
			Stats.Steals
			Stats.Turnovers
			Stats.RegularFoulsForced
			Stats.RegularFoulsCommitted
			Stats.TechnicalFoulsCommitted
			Stats.MinutesPlayed
		FROM
			Players
		LEFT JOIN
			PlayerGames ON PlayerGames.StatsId = Stats.StatsId
		WHERE
			PlayerGames.PlayerId = $1 AND PlayerGames.GameId = $2`,
		playerId, gameId).Scan(
		stats.StatsId,
		stats.TwoPointFGA,
		stats.TwoPointFGM,
		stats.ThreePointFGA,
		stats.ThreePointFGM,
		stats.FreeThrowsAttempted,
		stats.FreeThrowsMade,
		stats.OffensiveRebounds,
		stats.DefensiveRebounds,
		stats.Assists,
		stats.Blocks,
		stats.Steals,
		stats.Turnovers,
		stats.RegularFoulsForced,
		stats.RegularFoulsCommitted,
		stats.TechnicalFoulsCommitted,
		stats.MinutesPlayed)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That player/game combination does not exist")
	}

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (db *HeroBallDatabase) GetPlayerGameStats(playerId int32, gameId int32) (*pb.PlayerGameStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	pgStats := &pb.PlayerGameStats{}

	stats, err := db.GetStats(playerId, gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game stats: %v", err)
	}

	pgStats.Stats = stats

	game, err := db.GetGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game: %v", err)
	}

	pgStats.Game = game

	if err != nil {
		return nil, err
	}

	/* get the team info */
	pgStats.PlayerId = playerId

	var teamId int32

	err = db.db.QueryRow(`
		SELECT
			TeamId
		FROM
			PlayerGames
		WHERE PlayerId = $1 AND GameId = $2`, playerId, gameId).Scan(&teamId)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That player did not play in the game")
	}

	if err != nil {
		return nil, err
	}

	pgStats.TeamId = teamId

	return pgStats, nil
}

func (db *HeroBallDatabase) GetGame(gameId int32) (*pb.Game, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	game := &pb.Game{
		GameId: gameId,
	}

	err := db.db.QueryRow(`
		SELECT
			HomeTeams.TeamId,
			HomeTeams.Name,
			AwayTeams.TeamId,
			AwayTeams.Name,
			Locations.LocationId,
			Locations.Name,
			Competitions.CompetitionId,
			Competitions.Name,
			Games.Datetime	
		FROM
			Games
		LEFT JOIN
			Teams HomeTeams ON Games.HomeTeamId = Teams.TeamId
		LEFT JOIN
			Teams AwayTeams ON Games.AwayTeamId = Teams.TeamId			
		OUTER JOIN
			Locations ON Games.LocationId = Locations.LocationId
		OUTER JOIN
			Competitions ON Games.CompetitionId = Competitions.CompetitionId
		WHERE
			GameId = $1`,
		gameId).Scan(
		game.HomeTeam.TeamId,
		game.HomeTeam.Name,
		game.AwayTeam.TeamId,
		game.AwayTeam.Name,
		game.Location.LocationId,
		game.Location.Name,
		game.Competition.CompetitionId,
		game.Competition.Name,
		game.Datetime)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return game, nil
}

/* takes a playerId, return an array of recent games, up to maxnumber */
func (db *HeroBallDatabase) GetRecentPlayerGames(playerId int32, maxCount int32) ([]int32, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if maxCount <= 0 {
		return nil, fmt.Errorf("Invalid maxCount")
	}

	gameIds := make([]int32, 0)

	rows, err := db.db.Query(`
		SELECT
			GameId
		FROM
			Games
		LEFT JOIN
			PlayerGames ON PlayerGames.GameId = Games.GameId
		WHERE
			PlayerGames.PlayerId = $1
		ORDER BY
			Games.GameDate DESC
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
