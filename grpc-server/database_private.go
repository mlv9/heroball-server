package main

import (
	"database/sql"
	"fmt"
	"sort"

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

func (database *HeroBallDatabase) getPlayerStatsForGame(playerId int32, gameId int32) (*pb.PlayerGameStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	stats, err := database.getPlayerGameStatsByCondition("PlayerGameStats.PlayerId = $1 AND PlayerGameStats.GameId = $2", []interface{}{playerId, gameId})

	if err != nil {
		return nil, err
	}

	if len(stats) != 1 {
		return nil, fmt.Errorf("Error getting player stats for game - unexpected number of returns (%v)", len(stats))
	}

	return stats[0], nil
}

func (database *HeroBallDatabase) getPlayerTotalStatsForAllTime(playerId int32) (*pb.PlayerAggregateStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	playerStats, _, err := database.getAggregateStatsByConditionAndGroupingAndOrder(
		"PlayerGameStats.PlayerId = $1",
		[]interface{}{playerId},
		"GROUP BY PlayerGameStats.PlayerId", "", "", nil, "")

	if err != nil {
		return nil, err
	}

	return playerStats, nil
}

func (database *HeroBallDatabase) getPlayerGameStatsByCondition(conditions string, args []interface{}) ([]*pb.PlayerGameStats, error) {

	joinedStats := make([]*pb.PlayerGameStats, 0)

	rows, err := database.db.Query(fmt.Sprintf(`
		SELECT
			PlayerGameStats.StatsId,
			Leagues.LeagueId,
			Leagues.Name,
			Leagues.Division,
			Games.CompetitionId,
			Competitions.Name,
			Teams.TeamId,
			Teams.Name,
			Players.PlayerId,
			Players.Name,
			Players.Position,
			PlayerGameStats.TwoPointFGA,
			PlayerGameStats.TwoPointFGM,
			PlayerGameStats.ThreePointFGA, 
			PlayerGameStats.ThreePointFGM,
			PlayerGameStats.FreeThrowsAttempted,
			PlayerGameStats.FreeThrowsMade,
			PlayerGameStats.OffensiveRebounds,
			PlayerGameStats.DefensiveRebounds,
			PlayerGameStats.Assists,
			PlayerGameStats.Blocks,
			PlayerGameStats.Steals,
			PlayerGameStats.Turnovers,
			PlayerGameStats.RegularFoulsForced,
			PlayerGameStats.RegularFoulsCommitted,
			PlayerGameStats.TechnicalFoulsCommitted,
			PlayerGameStats.MinutesPlayed
		FROM
			PlayerGameStats
		LEFT JOIN
			Teams ON PlayerGameStats.TeamId = Teams.TeamId
		LEFT JOIN
			Players ON PlayerGameStats.PlayerId = Players.PlayerId
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId			
		LEFT JOIN
			Competitions ON Games.CompetitionId = Competitions.CompetitionId
		LEFT JOIN
			Leagues ON Competitions.LeagueId = Leagues.LeagueId
		WHERE 
			%v`,
		conditions), args...)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		stats := &pb.PlayerGameStats{
			Competition: &pb.Competition{
				League: &pb.League{},
			},
			Team:   &pb.Team{},
			Player: &pb.Player{},
			Stats:  &pb.Stats{},
		}

		err = rows.Scan(
			&stats.StatsId,
			&stats.Competition.League.LeagueId,
			&stats.Competition.League.Name,
			&stats.Competition.League.Division,
			&stats.Competition.CompetitionId,
			&stats.Competition.Name,
			&stats.Team.TeamId,
			&stats.Team.Name,
			&stats.Player.PlayerId,
			&stats.Player.Name,
			&stats.Player.Position,
			&stats.Stats.TwoPointFGA,
			&stats.Stats.TwoPointFGM,
			&stats.Stats.ThreePointFGA,
			&stats.Stats.ThreePointFGM,
			&stats.Stats.FreeThrowsAttempted,
			&stats.Stats.FreeThrowsMade,
			&stats.Stats.OffensiveRebounds,
			&stats.Stats.DefensiveRebounds,
			&stats.Stats.Assists,
			&stats.Stats.Blocks,
			&stats.Stats.Steals,
			&stats.Stats.Turnovers,
			&stats.Stats.RegularFoulsForced,
			&stats.Stats.RegularFoulsCommitted,
			&stats.Stats.TechnicalFoulsCommitted,
			&stats.Stats.MinutesPlayed)

		if err != nil {
			return nil, fmt.Errorf("Error scanning stats: %v", err)
		}

		joinedStats = append(joinedStats, stats)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error getting total stats: %v", err)
	}

	return joinedStats, nil
}

func (database *HeroBallDatabase) getAggregateStatsByConditionAndGroupingAndOrder(whereClause string, whereArgs []interface{}, grouping string, groupReturnedValue string, having string, havingArgs []interface{}, ordering string) (*pb.PlayerAggregateStats, int32, error) {

	/* append to totals */
	aggregateStats := &pb.PlayerAggregateStats{
		TotalStats: &pb.Stats{},
	}

	/* if missing, lets fake it */
	if groupReturnedValue == "" {
		groupReturnedValue = "0,"
	}

	var groupedValue int32

	err := database.db.QueryRow(fmt.Sprintf(`
		SELECT
			%v
			COUNT(PlayerGameStats.StatsId),
			SUM(PlayerGameStats.TwoPointFGA),
			SUM(PlayerGameStats.TwoPointFGM),
			SUM(PlayerGameStats.ThreePointFGA), 
			SUM(PlayerGameStats.ThreePointFGM),
			SUM(PlayerGameStats.FreeThrowsAttempted),
			SUM(PlayerGameStats.FreeThrowsMade),
			SUM(PlayerGameStats.OffensiveRebounds),
			SUM(PlayerGameStats.DefensiveRebounds),
			SUM(PlayerGameStats.Assists),
			SUM(PlayerGameStats.Blocks),
			SUM(PlayerGameStats.Steals),
			SUM(PlayerGameStats.Turnovers),
			SUM(PlayerGameStats.RegularFoulsForced),
			SUM(PlayerGameStats.RegularFoulsCommitted),
			SUM(PlayerGameStats.TechnicalFoulsCommitted),
			SUM(PlayerGameStats.MinutesPlayed)
		FROM
			PlayerGameStats
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId	
		WHERE
			%v
		%v
		%v
		%v`,
		groupReturnedValue, whereClause, grouping, having, ordering), append(whereArgs, havingArgs...)...).Scan(
		&groupedValue,
		&aggregateStats.Count,
		&aggregateStats.TotalStats.TwoPointFGA,
		&aggregateStats.TotalStats.TwoPointFGM,
		&aggregateStats.TotalStats.ThreePointFGA,
		&aggregateStats.TotalStats.ThreePointFGM,
		&aggregateStats.TotalStats.FreeThrowsAttempted,
		&aggregateStats.TotalStats.FreeThrowsMade,
		&aggregateStats.TotalStats.OffensiveRebounds,
		&aggregateStats.TotalStats.DefensiveRebounds,
		&aggregateStats.TotalStats.Assists,
		&aggregateStats.TotalStats.Blocks,
		&aggregateStats.TotalStats.Steals,
		&aggregateStats.TotalStats.Turnovers,
		&aggregateStats.TotalStats.RegularFoulsForced,
		&aggregateStats.TotalStats.RegularFoulsCommitted,
		&aggregateStats.TotalStats.TechnicalFoulsCommitted,
		&aggregateStats.TotalStats.MinutesPlayed)

	if err == sql.ErrNoRows {
		return nil, 0, nil
	}

	if err != nil {
		return nil, 0, err
	}

	return aggregateStats, groupedValue, nil
}

func (database *HeroBallDatabase) getResultForGame(gameId int32) (*pb.GameResult, error) {

	results, err := database.getResultsForGames([]int32{gameId})

	if err != nil {
		return nil, err
	}

	if results == nil || len(results) == 0 {
		return nil, fmt.Errorf("Could not find result")
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("Expecting 1 result, got %v", len(results))
	}

	return results[0], nil
}

func (database *HeroBallDatabase) getResultsForGames(gameIds []int32) ([]*pb.GameResult, error) {

	if gameIds == nil {
		return nil, fmt.Errorf("Invalid gameIds")
	}

	results := make([]*pb.GameResult, 0)

	for _, gameId := range gameIds {

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
		homeTeamStats, err := database.getStatsForTeamInGame(homeTeamId, gameId)

		if err != nil {
			return nil, err
		}

		awayTeamStats, err := database.getStatsForTeamInGame(awayTeamId, gameId)

		if err != nil {
			return nil, err
		}

		results = append(results, &pb.GameResult{
			HomeTeamId:     homeTeamId,
			AwayTeamId:     awayTeamId,
			HomeTeamPoints: (homeTeamStats.TotalStats.ThreePointFGM * 3) + (homeTeamStats.TotalStats.TwoPointFGM * 2) + (homeTeamStats.TotalStats.FreeThrowsMade),
			AwayTeamPoints: (awayTeamStats.TotalStats.ThreePointFGM * 3) + (awayTeamStats.TotalStats.TwoPointFGM * 2) + (awayTeamStats.TotalStats.FreeThrowsMade),
		})
	}

	return results, nil
}

func (database *HeroBallDatabase) getStatsForTeamInGame(teamId int32, gameId int32) (*pb.PlayerAggregateStats, error) {
	stats, _, err := database.getAggregateStatsByConditionAndGroupingAndOrder(
		"PlayerGameStats.TeamId = $1 AND PlayerGameStats.GameId = $2",
		[]interface{}{teamId, gameId},
		"GROUP BY PlayerGameStats.TeamId", "", "", nil, "")

	return stats, err
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
			PlayerGameStats
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
			PlayerGameStats
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

// /* takes a competitionId, return an array of ordered games, from most recent to least recent.  maxCount 0 has no limit */
// func (database *HeroBallDatabase) getCompetitionGames(competitionId int32, maxCount int32) ([]int32, error) {

// 	if competitionId <= 0 {
// 		return nil, fmt.Errorf("Invalid competitionId")
// 	}

// 	if maxCount < 0 {
// 		return nil, fmt.Errorf("Invalid maxCount")
// 	}

// 	limitQuery := ""
// 	queryArgs := []interface{}{competitionId}

// 	/* if zero, we apply no limit */
// 	if maxCount != 0 {
// 		limitQuery = "LIMIT $2"
// 		queryArgs = append(queryArgs, maxCount)
// 	}

// 	gameIds := make([]int32, 0)

// 	rows, err := database.db.Query(fmt.Sprintf(`
// 		SELECT
// 			GameId
// 		FROM
// 			Games
// 		WHERE
// 			Games.CompetitionId = $1
// 		ORDER BY
// 			GameTime DESC
// 		%v`, limitQuery), queryArgs...)

// 	if err == sql.ErrNoRows {
// 		return nil, fmt.Errorf("That competitionId does not exist")
// 	}

// 	if err != nil {
// 		return nil, err
// 	}

// 	for rows.Next() {

// 		var gameId int32

// 		/* now to scan them all */
// 		err = rows.Scan(&gameId)

// 		if err != nil {
// 			return nil, err
// 		}

// 		gameIds = append(gameIds, gameId)
// 	}

// 	err = rows.Err()

// 	if err != nil {
// 		return nil, err
// 	}

// 	return gameIds, nil
// }

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
			PlayerGameStats.TeamId,
			Teams.Name,
			PlayerGameStats.JerseyNumber
		FROM
			PlayerGameStats
		LEFT JOIN
			Teams ON PlayerGameStats.TeamId = Teams.TeamId	
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

func (database *HeroBallDatabase) getStandingsForCompetition(competitionId int32) ([]*pb.CompetitionTeam, error) {

	/* get games in compeittion */

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	results, err := database.getResultsForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	/* now to create an ordered list of teams */
	teamsMap := make(map[int32]*pb.CompetitionTeam)

	for _, result := range results {

		_, exists := teamsMap[result.HomeTeamId]

		if !exists {

			/* get the team */
			team, err := database.getTeam(result.HomeTeamId)

			if err != nil {
				return nil, err
			}

			teamsMap[result.HomeTeamId] = &pb.CompetitionTeam{
				Team: team,
			}
		}

		if result.HomeTeamPoints > result.AwayTeamPoints {
			teamsMap[result.HomeTeamId].Won++
		} else if result.HomeTeamPoints < result.AwayTeamPoints {
			teamsMap[result.HomeTeamId].Lost++
		} else {
			teamsMap[result.HomeTeamId].Drawn++
		}

		/* and away team */
		_, exists = teamsMap[result.AwayTeamId]

		if !exists {

			/* get the team */
			team, err := database.getTeam(result.AwayTeamId)

			if err != nil {
				return nil, err
			}

			teamsMap[result.AwayTeamId] = &pb.CompetitionTeam{
				Team: team,
			}
		}

		if result.AwayTeamPoints > result.HomeTeamPoints {
			teamsMap[result.AwayTeamId].Won++
		} else if result.AwayTeamPoints < result.HomeTeamPoints {
			teamsMap[result.AwayTeamId].Lost++
		} else {
			teamsMap[result.AwayTeamId].Drawn++
		}
	}

	/* now turn the teams map into an ordered list */
	standings := make([]*pb.CompetitionTeam, 0)

	/* into a list */
	for _, team := range teamsMap {
		standings = append(standings, team)
	}

	/* sorted on wins for the moment */
	sort.Slice(standings, func(i, j int) bool {
		return standings[i].Won > standings[j].Won
	})

	return standings, nil
}

func (database *HeroBallDatabase) getResultsForCompetition(competitionId int32) ([]*pb.GameResult, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	/* get all the games in a competition */

	rows, err := database.db.Query(`
		SELECT DISTINCT
			GameId
		FROM
			Games
		WHERE
			CompetitionId = $1		
		`, competitionId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting results for comp: %v", err)
	}

	gameIds := make([]int32, 0)

	for rows.Next() {

		var gameId int32

		err = rows.Scan(&gameId)

		if err != nil {
			return nil, fmt.Errorf("Error scanning gameId: %v", err)
		}

		gameIds = append(gameIds, gameId)
	}

	return database.getResultsForGames(gameIds)
}
