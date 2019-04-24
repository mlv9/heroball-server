package main

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"

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

// func (database *HeroBallDatabase) getPlayerGame(playerId int32, gameId int32) (*pb.PlayerGame, error) {

// 	/* get stats to begin with */
// 	pgStats, err := database.getPlayerGameStats(playerId, gameId)

// 	if err != nil {
// 		return nil, fmt.Errorf("Error getting player game stats: %v", err)
// 	}

// 	/* get the game */
// 	game, err := database.getGame(gameId)

// 	if err != nil {
// 		return nil, fmt.Errorf("Error getting game: %v", err)
// 	}

// 	return &pb.PlayerGame{
// 		Game:  game,
// 		Stats: pgStats,
// 	}, nil
// }

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

	var statsId int32
	pteam := &pb.PlayerTeam{
		Team: &pb.Team{},
	}

	err := database.db.QueryRow(`
		SELECT
			PlayerGames.TeamId,
			Teams.Name,
			PlayerGames.JerseyNumber,
			PlayerGames.StatsId
		FROM
			PlayerGames
		LEFT JOIN
			Teams ON Teams.TeamId = PlayerGames.TeamId
		WHERE PlayerId = $1 AND GameId = $2`, playerId, gameId).Scan(
		&pteam.Team.TeamId,
		&pteam.Team.Name,
		&pteam.JerseyNumber,
		&statsId)

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
	stats, err := database.getStatsTotals([]int32{statsId})

	if err != nil {
		return nil, fmt.Errorf("Error getting player stats: %v", err)
	}

	pgStats := &pb.PlayerGameStats{
		Player: player,
		Team:   pteam,
		Stats:  stats,
	}

	return pgStats, nil
}

func (database *HeroBallDatabase) getPlayerTotalStatsForGames(playerId int32, gameIds []int32) (*pb.Stats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	rows, err := database.db.Query(`
		SELECT 
			StatsId
		FROM
			PlayerGames
		WHERE
			PlayerId = $1
		`, playerId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting stats for player: %v", err)
	}

	statsIds := make([]int32, 0)

	for rows.Next() {

		var statsId int32

		err = rows.Scan(&statsId)

		if err != nil {
			return nil, fmt.Errorf("Error scanning statsId: %v", err)
		}

		/* now get the game stats */
		statsIds = append(statsIds, statsId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting stats: %v", err)
	}

	/* now get values */
	totalStats, err := database.getStatsTotals(statsIds)

	if err != nil {
		return nil, fmt.Errorf("Error getting stats: %v", err)
	}

	return totalStats, nil
}

func (database *HeroBallDatabase) getAllGamesByPlayer(playerId int32) ([]int32, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	rows, err := database.db.Query(`
		SELECT 
			GameId
		FROM
			PlayerGames
		WHERE
			PlayerId = $1
		`, playerId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting games by player: %v", err)
	}

	gameIds := make([]int32, 0)

	for rows.Next() {

		var gameId int32

		err = rows.Scan(&gameId)

		if err != nil {
			return nil, fmt.Errorf("Error scanning gameId: %v", err)
		}

		/* now get the game stats */
		gameIds = append(gameIds, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting games: %v", err)
	}

	return gameIds, nil
}

func (database *HeroBallDatabase) getStatsTotals(statsIds []int32) (*pb.Stats, error) {

	totalStats := &pb.Stats{}

	rows, err := database.db.Query(`
		SELECT
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
		WHERE StatsId = ANY($1)`,
		pq.Array(statsIds))

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("The statsId does not exist")
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		stats := &pb.Stats{}

		err = rows.Scan(
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

		if err != nil {
			return nil, fmt.Errorf("Error scanning stats: %v", err)
		}

		/* append to totals */
		totalStats.TwoPointFGA += stats.TwoPointFGA
		totalStats.TwoPointFGM += stats.TwoPointFGM
		totalStats.ThreePointFGA += stats.ThreePointFGA
		totalStats.ThreePointFGM += stats.ThreePointFGM
		totalStats.FreeThrowsAttempted += stats.FreeThrowsAttempted
		totalStats.FreeThrowsMade += stats.FreeThrowsMade
		totalStats.OffensiveRebounds += stats.OffensiveRebounds
		totalStats.DefensiveRebounds += stats.DefensiveRebounds
		totalStats.Assists += stats.Assists
		totalStats.Blocks += stats.Blocks
		totalStats.Steals += stats.Steals
		totalStats.Turnovers += stats.Turnovers
		totalStats.RegularFoulsForced += stats.RegularFoulsForced
		totalStats.RegularFoulsCommitted += stats.RegularFoulsCommitted
		totalStats.TechnicalFoulsCommitted += stats.TechnicalFoulsCommitted
		totalStats.MinutesPlayed += stats.MinutesPlayed
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting total stats: %v", err)
	}

	return totalStats, nil
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

	comp := &pb.Competition{
		League: &pb.League{},
	}

	err := database.db.QueryRow(`
		SELECT
			Leagues.LeagueId,
			Leagues.Name,
			Leagues.Division,
			Competitions.Name
		FROM
			Competitions
		LEFT JOIN
			Leagues ON Competitions.LeagueId = Leagues.LeagueId
		WHERE CompetitionId = $1
	`, competitionId).Scan(
		&comp.League.LeagueId,
		&comp.League.Name,
		&comp.League.Division,
		&comp.Name)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("CompetitionId does not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting comp: %v", err)
	}

	return comp, nil
}

func (database *HeroBallDatabase) getGame(gameId int32) (*pb.Game, error) {

	games, err := database.getGames([]int32{gameId})

	if err != nil {
		return nil, err
	}

	if games == nil || len(games) == 0 {
		return nil, fmt.Errorf("Could not find game")
	}

	if len(games) != 1 {
		return nil, fmt.Errorf("Expecting 1 game, got %v", len(games))
	}

	return games[0], nil
}

func (database *HeroBallDatabase) getLocation(locationId int32) (*pb.Location, error) {

	locations, err := database.getLocations([]int32{locationId})

	if err != nil {
		return nil, err
	}

	if locations == nil || len(locations) == 0 {
		return nil, fmt.Errorf("Could not find location")
	}

	if len(locations) != 1 {
		return nil, fmt.Errorf("Expecting 1 location, got %v", len(locations))
	}

	return locations[0], nil
}

func (database *HeroBallDatabase) getTeam(teamId int32) (*pb.Team, error) {

	teams, err := database.getTeams([]int32{teamId})

	if err != nil {
		return nil, err
	}

	if teams == nil || len(teams) == 0 {
		return nil, fmt.Errorf("Could not find team")
	}

	if len(teams) != 1 {
		return nil, fmt.Errorf("Expecting 1 team, got %v", len(teams))
	}

	return teams[0], nil
}

func (database *HeroBallDatabase) getGames(gameIds []int32) ([]*pb.Game, error) {

	if gameIds == nil {
		return nil, fmt.Errorf("Invalid gameIds")
	}

	rows, err := database.db.Query(`
		SELECT
			Games.GameId,
			HomeTeams.TeamId,
			HomeTeams.Name,
			AwayTeams.TeamId,
			AwayTeams.Name,
			Locations.LocationId,
			Locations.Name,
			Games.CompetitionId,
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
			GameId = ANY($1)`,
		pq.Array(gameIds))

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That gameId does not exist")
	}

	if err != nil {
		return nil, fmt.Errorf("Error in db: %v", err)
	}

	games := make([]*pb.Game, 0)

	for rows.Next() {

		var competitionId int32
		var gameId int32

		game := &pb.Game{
			HomeTeam: &pb.Team{},
			AwayTeam: &pb.Team{},
			Location: &pb.Location{},
		}

		err = rows.Scan(
			&gameId,
			&game.HomeTeam.TeamId,
			&game.HomeTeam.Name,
			&game.AwayTeam.TeamId,
			&game.AwayTeam.Name,
			&game.Location.LocationId,
			&game.Location.Name,
			&competitionId,
			&game.GameTime)

		if err != nil {
			return nil, fmt.Errorf("Error getting games: %v", err)
		}

		game.GameId = gameId

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

		games = append(games, game)
	}

	return games, nil
}

func (database *HeroBallDatabase) getPlayersForTeam(teamId int32) ([]int32, error) {

	if teamId <= 0 {
		return nil, fmt.Errorf("Invalid teamId")
	}

	playerIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			PlayerId
		FROM
			PlayerGames
		WHERE
			TeamId = $1
			`,
		teamId)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("That teamId does not exist")
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
			GameId
		FROM
			Games
		WHERE
			Games.CompetitionId = $1
		ORDER BY
			GameTime DESC
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

func (database *HeroBallDatabase) getAllTeamsForPlayer(playerId int32) ([]*pb.PlayerTeam, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	rows, err := database.db.Query(`
		SELECT
			PlayerGames.TeamId,
			Teams.Name,
			PlayerGames.JerseyNumber
		FROM
			PlayerGames
		LEFT JOIN
			Teams ON PlayerGames.TeamId = Teams.TeamId	
	`)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting teams for player: %v", err)
	}

	teams := make([]*pb.PlayerTeam, 0)

	for rows.Next() {

		playerTeam := &pb.PlayerTeam{
			Team: &pb.Team{},
		}

		err = rows.Scan(
			&playerTeam.Team.TeamId,
			&playerTeam.Team.Name,
			&playerTeam.JerseyNumber)

		if err != nil {
			return nil, fmt.Errorf("Error scanning team: %v", err)
		}

		teams = append(teams, playerTeam)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error from scan: %v", err)
	}

	return teams, nil
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
			TeamId = ANY($1)
	`, pq.Array(teamIds))

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
			LocationId = ANY($1)
	`, pq.Array(locationIds))

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
