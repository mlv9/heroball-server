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

func (database *HeroBallDatabase) GetPlayerInfo(playerId int32) (*pb.PlayerInfo, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	info := &pb.PlayerInfo{
		PlayerId:        playerId,
		RecentGameStats: make([]*pb.PlayerGameStats, 0),
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
		game, err := database.GetPlayerGameStats(playerId, gameId)

		if err != nil {
			return nil, fmt.Errorf("Error getting player game stats: %v", err)
		}

		info.RecentGameStats = append(info.RecentGameStats, game)

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

func (database *HeroBallDatabase) GetStats(playerId int32, gameId int32) (*pb.Stats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

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
			Players
		LEFT JOIN
			PlayerGames ON PlayerGames.PlayerId = Players.PlayerId
		LEFT JOIN
			Stats ON PlayerGames.StatsId = Stats.StatsId
		WHERE
			PlayerGames.PlayerId = $1 AND PlayerGames.GameId = $2`,
		playerId, gameId).Scan(
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
		return nil, fmt.Errorf("That player/game combination does not exist")
	}

	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (database *HeroBallDatabase) GetPlayerGameStats(playerId int32, gameId int32) (*pb.PlayerGameStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	pgStats := &pb.PlayerGameStats{}

	stats, err := database.GetStats(playerId, gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game stats: %v", err)
	}

	pgStats.Stats = stats

	game, err := database.GetGame(gameId)

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

	err = database.db.QueryRow(`
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
		&game.GameTime)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, err
	}

	return game, nil
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
