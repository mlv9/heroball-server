package main

import (
	"database/sql"
	"fmt"

	pb "github.com/heroballapp/server/protobuf"
)

func (database *HeroBallDatabase) getPlayerProfile(playerId int32) (*pb.PlayerProfile, error) {

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

func (database *HeroBallDatabase) getPlayerGame(playerId int32, gameId int32) (*pb.PlayerGame, error) {

	/* get stats to begin with */
	pgStats, err := database.getPlayerGameStats(playerId, gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player game stats: %v", err)
	}

	/* get the game */
	game, err := database.getGame(gameId)

	if err != nil {
		return nil, fmt.Errorf("Error getting game: %v", err)
	}

	return &pb.PlayerGame{
		Game:  game,
		Stats: pgStats,
	}, nil
}

func (database *HeroBallDatabase) getPlayer(playerId int32) (*pb.Player, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	player := &pb.Player{
		PlayerId: playerId,
	}

	err := database.db.QueryRow(`
		SELECT
			Name,
			Position
		FROM 
			Players
		WHERE
			PlayerId = $1
		`, playerId).Scan(&player.Name, &player.Position)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That player does not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error in db: %v", err)
	}

	return player, nil
}

func (database *HeroBallDatabase) getPlayerGameStats(playerId int32, gameId int32) (*pb.PlayerGameStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

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
		return nil, fmt.Errorf("Error in db: %v", err)
	}

	/* get player */
	player, err := database.getPlayer(playerId)

	if err != nil {
		return nil, err
	}

	/* get stats */
	stats, err := database.getStats(statsId)

	if err != nil {
		return nil, fmt.Errorf("Error getting player stats: %v", err)
	}

	pgStats := &pb.PlayerGameStats{
		Player: player,
		TeamId: teamId,
		Stats:  stats,
	}

	return pgStats, nil
}

func (database *HeroBallDatabase) getStats(statsId int32) (*pb.Stats, error) {

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

func (database *HeroBallDatabase) getResultForGame(gameId int32) (*pb.GameResult, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	var homeTeamId int32
	var awayTeamId int32

	err := database.db.QueryRow(`
		SELECT
			HomeTeamId,
			AwayTeamId
		FROM
			Games
		WHERE GameId = $1
	`, gameId).Scan(&homeTeamId, &awayTeamId)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("This game has no result")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting teams in game: %v", err)
	}

	/* now get the points for each */
	homeTeamPoints, err := database.getPointsForTeamInGame(homeTeamId, gameId)

	if err != nil {
		return nil, err
	}

	awayTeamPoints, err := database.getPointsForTeamInGame(awayTeamId, gameId)

	if err != nil {
		return nil, err
	}

	return &pb.GameResult{
		HomeTeamPoints: homeTeamPoints,
		AwayTeamPoints: awayTeamPoints,
	}, nil
}

func (database *HeroBallDatabase) getPointsForTeamInGame(teamId int32, gameId int32) (int32, error) {

	var points int32

	err := database.db.QueryRow(`
		SELECT
			COALESCE(SUM(FreeThrowsMade),0) +
			COALESCE(SUM(TwoPointFGM),0) * 2 +
			COALESCE(SUM(ThreePointFGM),0) * 3
		FROM 
			Stats
		LEFT JOIN PlayerGames ON 
			Stats.StatsId = PlayerGames.StatsId
		GROUP BY
			PlayerGames.GameId, PlayerGames.TeamId
		HAVING 
			PlayerGames.GameId = $2 AND PlayerGames.TeamId = $1`, teamId, gameId).Scan(&points)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("This game is invalid")
	}

	if err != nil {
		return 0, fmt.Errorf("Error getting points for team in game: %v", err)
	}

	return points, nil
}

func (database *HeroBallDatabase) getCompetition(competitionId int32) (*pb.Competition, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	comp := &pb.Competition{}

	err := database.db.QueryRow(`
		SELECT
			Name,
			SubCompetition
		FROM
			Competitions
		WHERE CompetitionId = $1
	`, competitionId).Scan(&comp.Name, &comp.SubCompetition)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("CompetitionId does not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting comp: %v", err)
	}

	return comp, nil
}

func (database *HeroBallDatabase) getGame(gameId int32) (*pb.Game, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	game := &pb.Game{
		GameId:   gameId,
		HomeTeam: &pb.Team{},
		AwayTeam: &pb.Team{},
		Location: &pb.Location{},
	}

	var competitionId int32

	err := database.db.QueryRow(`
		SELECT
			HomeTeams.TeamId,
			HomeTeams.Name,
			AwayTeams.TeamId,
			AwayTeams.Name,
			Locations.LocationId,
			Locations.Name,
			Games.CompetitionId
			Games.GameTime	
		FROM
			Games
		LEFT JOIN
			Teams HomeTeams ON Games.HomeTeamId = HomeTeams.TeamId
		LEFT JOIN
			Teams AwayTeams ON Games.AwayTeamId = AwayTeams.TeamId			
		LEFT JOIN
			Locations ON Games.LocationId = Locations.LocationId
		WHERE
			GameId = $1`,
		gameId).Scan(
		&game.HomeTeam.TeamId,
		&game.HomeTeam.Name,
		&game.AwayTeam.TeamId,
		&game.AwayTeam.Name,
		&game.Location.LocationId,
		&game.Location.Name,
		&competitionId,
		&game.GameTime)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error in db: %v")
	}

	/* get the game result */
	result, err := database.getResultForGame(gameId)

	if err != nil {
		return nil, err
	}

	game.Result = result

	/* get the competition */
	comp, err := database.getCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	game.Competition = comp

	return game, nil
}

func (database *HeroBallDatabase) getPlayersInGame(gameId int32) ([]int32, error) {

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
func (database *HeroBallDatabase) getRecentPlayerGames(playerId int32, maxCount int32) ([]int32, error) {

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

/* takes a competitionId, return an array of recent games, up to maxnumber */
func (database *HeroBallDatabase) getRecentCompetitionGames(competitionId int32, maxCount int32) ([]int32, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
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
			PlayerGames.CompetitionId = $1
		ORDER BY
			Games.GameTime DESC
		LIMIT $2
			`,
		competitionId, maxCount)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That competitionId does not exist")
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

func (database *HeroBallDatabase) getCompetitionLocations(competitionId int32) ([]int32, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	rows, err := database.db.Query(`
		SELECT
			DISTINCT LocationId
		FROM 
			Games
		WHERE 
			CompetitionId = $1
	`, competitionId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting competition locations: %v", err)
	}

	locationIds := make([]int32, 0)

	for rows.Next() {

		var locationId int32
		err = rows.Scan(&locationId)

		if err != nil {
			return nil, fmt.Errorf("Error getting location: %v", err)
		}

		locationIds = append(locationIds, locationId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error from rows: %v", err)
	}

	return locationIds, nil
}

func (database *HeroBallDatabase) getCompetitionTeams(competitionId int32) ([]int32, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	rows, err := database.db.Query(`
		SELECT 
			DISTINCT HomeTeamId AS TeamId
		FROM 
			Games
		WHERE 
			CompetitionId = $1
		UNION SELECT 
			DISTINCT AwayTeamId
		FROM 
			Games
		WHERE 
			CompetitionId = $1
	`, competitionId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting competition teams: %v", err)
	}

	teamIds := make([]int32, 0)

	for rows.Next() {

		var teamId int32
		err = rows.Scan(&teamId)

		if err != nil {
			return nil, fmt.Errorf("Error getting location: %v", err)
		}

		teamIds = append(teamIds, teamId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error from rows: %v", err)
	}

	return teamIds, nil
}

func (database *HeroBallDatabase) getTeams(teamIds []int32) ([]*pb.Team, error) {

	if teamIds == nil {
		return nil, fmt.Errorf("Invalid teamIds")
	}

	rows, err := database.db.Query(`
		SELECT
			Name
		FROM
			Teams
		WHERE
			TeamId = IN($1)
	`, teamIds)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Teams do not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting teams: %v", err)
	}

	teams := make([]*pb.Team, 0)

	for rows.Next() {

		team := pb.Team{}

		err = rows.Scan(&team.Name)

		if err != nil {
			return nil, fmt.Errorf("Error getting team info: %v", err)
		}

		teams = append(teams, &team)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting teams: %v", err)
	}

	return teams, nil
}

func (database *HeroBallDatabase) getLocations(locationIds []int32) ([]*pb.Location, error) {

	if locationIds == nil {
		return nil, fmt.Errorf("Invalid locationIds")
	}

	rows, err := database.db.Query(`
		SELECT
			Name
		FROM
			Locations
		WHERE
			LocationId = IN($1)
	`, locationIds)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Locations do not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting Locations: %v", err)
	}

	locations := make([]*pb.Location, 0)

	for rows.Next() {

		location := pb.Location{}

		err = rows.Scan(&location.Name)

		if err != nil {
			return nil, fmt.Errorf("Error getting location info: %v", err)
		}

		locations = append(locations, &location)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting locations: %v", err)
	}

	return locations, nil
}
