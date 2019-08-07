package main

import (
	"database/sql"
	"fmt"
	"math"
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

func (database *HeroBallDatabase) getPlayerById(playerId int32) (*pb.Player, error) {

	players, err := database.getPlayersById([]int32{playerId})

	if err != nil {
		return nil, err
	}

	if players == nil || len(players) == 0 {
		return nil, fmt.Errorf("Could not find player")
	}

	if len(players) != 1 {
		return nil, fmt.Errorf("Expecting 1 player, got %v", len(players))
	}

	return players[0], nil
}

func (database *HeroBallDatabase) getPlayersById(playerIds []int32) ([]*pb.Player, error) {

	if len(playerIds) < 1 {
		return nil, fmt.Errorf("Must supply a playerId")
	}

	rows, err := database.db.Query(`
		SELECT
			PlayerId,
			Name,
			Position
		FROM 
			Players
		WHERE
			PlayerId = ANY($1)
		`, pq.Array(playerIds))

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error in db: %v", err)
	}

	players := make([]*pb.Player, 0)

	/* now scan them all */
	for rows.Next() {

		player := &pb.Player{}

		err = rows.Scan(
			&player.PlayerId,
			&player.Name,
			&player.Position)

		if err != nil {
			return nil, fmt.Errorf("Error scanning player: %v", err)
		}

		players = append(players, player)

	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("error on db scan: %v", err)
	}

	return players, nil
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

func (database *HeroBallDatabase) getPlayerTotalStatsForTeam(playerId int32, teamId int32) (*pb.PlayerAggregateStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	playerStats, playerIds, err := database.getAggregateStatsByConditionAndGroupingAndOrderAndLimitAndOffset(
		"PlayerGameStats.PlayerId = $1 AND PlayerGameStats.TeamId = $2",
		[]interface{}{playerId, teamId},
		"GROUP BY PlayerGameStats.PlayerId", "PlayerGameStats.PlayerId", "", nil, "", 1, 0)

	if err != nil {
		return nil, err
	}

	if playerStats == nil || len(playerStats) < 1 || len(playerIds) < 1 {
		return nil, nil
	}

	player, err := database.getPlayerById(playerIds[0])

	return &pb.PlayerAggregateStats{
		Stats:  playerStats[0],
		Player: player,
	}, nil
}

func (database *HeroBallDatabase) getPlayerTotalStatsForAllTime(playerId int32) (*pb.PlayerAggregateStats, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	playerStats, playerIds, err := database.getAggregateStatsByConditionAndGroupingAndOrderAndLimitAndOffset(
		"PlayerGameStats.PlayerId = $1",
		[]interface{}{playerId},
		"GROUP BY PlayerGameStats.PlayerId", "PlayerGameStats.PlayerId", "", nil, "", 1, 0)

	if err != nil {
		return nil, err
	}

	if playerStats == nil || len(playerStats) < 1 || len(playerIds) < 1 {
		return nil, nil
	}

	player, err := database.getPlayerById(playerIds[0])

	return &pb.PlayerAggregateStats{
		Stats:  playerStats[0],
		Player: player,
	}, nil
}

func (database *HeroBallDatabase) getPlayerGameStatsByCondition(conditions string, args []interface{}) ([]*pb.PlayerGameStats, error) {

	joinedStats := make([]*pb.PlayerGameStats, 0)

	rows, err := database.db.Query(fmt.Sprintf(`
		SELECT
			PlayerGameStats.StatsId,
			PlayerGameStats.GameId,
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
			Team:   &pb.Team{},
			Player: &pb.Player{},
			Stats:  &pb.Stats{},
		}

		err = rows.Scan(
			&stats.StatsId,
			&stats.GameId,
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

func (database *HeroBallDatabase) getAggregateStatsByConditionAndGroupingAndOrderAndLimitAndOffset(whereClause string, whereArgs []interface{}, grouping string, groupReturnedKey string, having string, havingArgs []interface{}, ordering string, limit int32, offset int32) ([]*pb.Stats, []int32, error) {

	/* if missing, lets fake it */
	if groupReturnedKey == "" {
		groupReturnedKey = "0"
	}

	groupedKeys := make([]int32, 0)

	/* append to totals */
	allStats := make([]*pb.Stats, 0)

	rows, err := database.db.Query(fmt.Sprintf(`
		SELECT
			%v,
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
			%v
			LIMIT %v
			OFFSET %v`,
		groupReturnedKey, whereClause, grouping, having, ordering, limit, offset), append(whereArgs, havingArgs...)...)

	if err == sql.ErrNoRows {
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	for rows.Next() {

		stats := &pb.Stats{}

		var groupedKey int32

		err = rows.Scan(&groupedKey,
			&stats.GameCount,
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
			return nil, nil, err
		}

		allStats = append(allStats, stats)
		groupedKeys = append(groupedKeys, groupedKey)
	}

	err = rows.Err()

	if err != nil {
		return nil, nil, err
	}

	return allStats, groupedKeys, nil
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

		if homeTeamStats == nil {
			return nil, fmt.Errorf("Was not able to find home teamId %v stats for gameId %v", homeTeamId, gameId)
		}

		awayTeamStats, err := database.getStatsForTeamInGame(awayTeamId, gameId)

		if err != nil {
			return nil, err
		}

		if awayTeamStats == nil {
			return nil, fmt.Errorf("Was not able to find away teamId %v stats for gameId %v", awayTeamId, gameId)
		}

		results = append(results, &pb.GameResult{
			HomeTeamId:     homeTeamId,
			AwayTeamId:     awayTeamId,
			HomeTeamPoints: (homeTeamStats.ThreePointFGM * 3) + (homeTeamStats.TwoPointFGM * 2) + (homeTeamStats.FreeThrowsMade),
			AwayTeamPoints: (awayTeamStats.ThreePointFGM * 3) + (awayTeamStats.TwoPointFGM * 2) + (awayTeamStats.FreeThrowsMade),
		})
	}

	return results, nil
}

func (database *HeroBallDatabase) getStatsForTeamInGame(teamId int32, gameId int32) (*pb.Stats, error) {
	stats, _, err := database.getAggregateStatsByConditionAndGroupingAndOrderAndLimitAndOffset(
		"PlayerGameStats.TeamId = $1 AND PlayerGameStats.GameId = $2",
		[]interface{}{teamId, gameId},
		"GROUP BY PlayerGameStats.TeamId", "PlayerGameStats.TeamId", "", nil, "", 1, 0)

	if err != nil {
		return nil, err
	}

	if stats == nil || len(stats) < 1 {
		return nil, nil
	}

	return stats[0], nil
}

func (database *HeroBallDatabase) getCompetitionById(competitionId int32) (*pb.Competition, error) {

	comps, err := database.getCompetitionsById([]int32{competitionId})

	if err != nil {
		return nil, err
	}

	if comps == nil || len(comps) == 0 {
		return nil, fmt.Errorf("Could not find game")
	}

	if len(comps) != 1 {
		return nil, fmt.Errorf("Expecting 1 game, got %v", len(comps))
	}

	return comps[0], nil
}

func (database *HeroBallDatabase) getCompetitionsById(competitionIds []int32) ([]*pb.Competition, error) {

	if competitionIds == nil {
		return nil, fmt.Errorf("Invalid competitionIds - must supply at least one")
	}

	rows, err := database.db.Query(`
	SELECT
		Competitions.CompetitionId,
		Leagues.LeagueId,
		Leagues.Name,
		Leagues.Division,
		Competitions.Name
	FROM
		Competitions
	LEFT JOIN
		Leagues ON Competitions.LeagueId = Leagues.LeagueId
	WHERE 
		CompetitionId = ANY($1)
	`, pq.Array(competitionIds))

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error in db: %v", err)
	}

	comps := make([]*pb.Competition, 0)

	for rows.Next() {

		comp := &pb.Competition{
			League: &pb.League{},
		}

		err = rows.Scan(
			&comp.CompetitionId,
			&comp.League.LeagueId,
			&comp.League.Name,
			&comp.League.Division,
			&comp.Name)

		if err != nil {
			return nil, fmt.Errorf("Error scanning comp: %v", err)
		}

		comps = append(comps, comp)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error on final scan: %v", err)
	}

	return comps, nil
}

func (database *HeroBallDatabase) getGameById(gameId int32) (*pb.Game, error) {

	games, err := database.getGamesById([]int32{gameId})

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

func (database *HeroBallDatabase) getTeamById(teamId int32) (*pb.Team, error) {

	teams, err := database.getTeamsById([]int32{teamId})

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

func (database *HeroBallDatabase) getGamesById(gameIds []int32) ([]*pb.Game, error) {

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
			GameId = ANY($1)
		ORDER BY Games.GameTime DESC`,
		pq.Array(gameIds))

	if err == sql.ErrNoRows {
		return nil, nil
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
		comp, err := database.getCompetitionById(competitionId)

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
			DISTINCT PlayerId
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

/* returns a list of gameIds, from most recent to least recent */
func (database *HeroBallDatabase) getGameIdsForCompetition(competitionId int32) ([]int32, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	/* get all the games in a competition */
	rows, err := database.db.Query(`
		SELECT
			GameId,
			GameTime
		FROM
			Games
		WHERE
			CompetitionId = $1
		ORDER BY
			GameTime DESC
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
		var gameTime string

		err = rows.Scan(&gameId, &gameTime)

		if err != nil {
			return nil, fmt.Errorf("Error scanning gameId: %v", err)
		}

		gameIds = append(gameIds, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error following scan: %v", err)
	}

	return gameIds, nil
}

func (database *HeroBallDatabase) getGameIdsForPlayer(playerId int32) ([]int32, error) {
	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	rows, err := database.db.Query(`
	SELECT
		Games.GameId,
		Games.GameTime
	FROM
		Games
	LEFT JOIN
		PlayerGameStats ON Games.GameId = PlayerGameStats.GameId
	WHERE
		PlayerGameStats.PlayerId = $1
	`, playerId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	games := make([]int32, 0)

	for rows.Next() {

		var gameId int32
		var gameTime string

		err = rows.Scan(&gameId, &gameTime)

		if err != nil {
			return nil, fmt.Errorf("Error scanning games: %v", err)
		}

		games = append(games, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error following scan: %v", err)
	}

	return games, nil

}

/* returns a list of gameIds, from most recent to least recent */
func (database *HeroBallDatabase) getGameIdsForTeam(teamId int32) ([]int32, error) {

	if teamId <= 0 {
		return nil, fmt.Errorf("Invalid teamId")
	}

	rows, err := database.db.Query(`
		SELECT
			GameId,
			GameTime
		FROM
			Games
		WHERE
			HomeTeamId = $1
		UNION
		SELECT
			GameId,
			GameTime
		FROM
			Games
		WHERE
			AwayTeamId = $1
		ORDER BY
			GameTime DESC
	`, teamId)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	games := make([]int32, 0)

	for rows.Next() {

		var gameId int32
		var gameTime string

		err = rows.Scan(&gameId, &gameTime)

		if err != nil {
			return nil, fmt.Errorf("Error scanning games: %v", err)
		}

		games = append(games, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error following scan: %v", err)
	}

	return games, nil
}

func (database *HeroBallDatabase) getPlayersInGame(gameId int32) ([]int32, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	playerIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			DISTINCT PlayerId
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

/* returns the team that has been played with the most */
func (database *HeroBallDatabase) getPlayersTeamInCompetition(playerId int32, competitionId int32) (int32, error) {

	if playerId <= 0 {
		return 0, fmt.Errorf("Invalid playerId")
	}

	if competitionId <= 0 {
		return 0, fmt.Errorf("Invalid competitionId")
	}

	rows, err := database.db.Query(`
		SELECT
			DISTINCT PlayerGameStats.TeamId,
			COUNT(PlayerGameStats.TeamId)
		FROM
			PlayerGameStats
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId
		WHERE
			PlayerGameStats.PlayerId = $1 AND Games.CompetitionId = $2
		GROUP BY
			PlayerGameStats.TeamId
		`, playerId, competitionId)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("Error getting player teams in comp: %v", err)
	}

	var team struct {
		TeamId int32
		Count  int32
	}

	for rows.Next() {

		var id int32
		var count int32

		err = rows.Scan(
			&id,
			&count)

		if err != nil {
			return 0, fmt.Errorf("Error scanning team: %v", err)
		}

		if count > team.Count {
			team.Count = count
			team.TeamId = id
		}
	}

	err = rows.Err()

	if err != nil {
		return 0, fmt.Errorf("Error on scan: %v", err)
	}

	return team.TeamId, nil
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

func (database *HeroBallDatabase) getAllCompetitions() ([]*pb.Competition, error) {

	/* get all the competitionIds */
	compIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			CompetitionId
		FROM
			Competitions
	`)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var compId int32

		err = rows.Scan(&compId)

		if err != nil {
			return nil, err
		}

		compIds = append(compIds, compId)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return database.getCompetitionsById(compIds)
}

func (database *HeroBallDatabase) getAllPlayers() ([]*pb.Player, error) {

	playerIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			PlayerId
		FROM
			Players
	`)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var playerId int32

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

	return database.getPlayersById(playerIds)
}

func (database *HeroBallDatabase) getAllTeams() ([]*pb.Team, error) {
	teamIds := make([]int32, 0)

	rows, err := database.db.Query(`
		SELECT
			TeamId
		FROM
			Teams
	`)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {

		var teamId int32

		err = rows.Scan(&teamId)

		if err != nil {
			return nil, err
		}

		teamIds = append(teamIds, teamId)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return database.getTeamsById(teamIds)
}

func (database *HeroBallDatabase) getAllTeamsForPlayer(playerId int32) ([]*pb.PlayerTeam, error) {

	if playerId <= 0 {
		return nil, fmt.Errorf("Invalid playerId")
	}

	rows, err := database.db.Query(`
		SELECT
			DISTINCT PlayerGameStats.TeamId,
			Games.CompetitionId,
			Teams.Name
		FROM
			PlayerGameStats
		LEFT JOIN
			Teams ON Teams.TeamId = PlayerGameStats.TeamId
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId
		WHERE 
			PlayerGameStats.PlayerId = $1
		`, playerId)

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

		var compId int32

		err = rows.Scan(
			&playerTeam.Team.TeamId,
			&compId,
			&playerTeam.Team.Name)

		if err != nil {
			return nil, fmt.Errorf("Error scanning team: %v", err)
		}

		comp, err := database.getCompetitionById(compId)

		if err != nil {
			return nil, err
		}

		playerTeam.Competition = comp

		playerStats, err := database.getPlayerTotalStatsForTeam(playerId, playerTeam.Team.TeamId)

		if err != nil {
			return nil, err
		}

		playerTeam.AggregateStats = playerStats

		teams = append(teams, playerTeam)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error from scan: %v", err)
	}

	/* now get all jersey numbers for that player in the team */
	for _, team := range teams {

		rows, err = database.db.Query(`
			SELECT
				DISTINCT JerseyNumber
			FROM
				PlayerGameStats
			WHERE
				PlayerId = $1 AND TeamId = $2
		`, playerId, team.Team.TeamId)

		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Error getting jersey numbers for player %v of team %v", playerId, team.Team.TeamId)
		}

		if err != nil {
			return nil, err
		}

		for rows.Next() {

			var jNum int32

			err = rows.Scan(&jNum)

			if err != nil {
				return nil, fmt.Errorf("Error scanning jersey number: %v", err)
			}

			team.JerseyNumbers = append(team.JerseyNumbers, jNum)
		}

		err = rows.Err()

		if err != nil {
			return nil, fmt.Errorf("Error scanning jersey: %v", err)
		}
	}

	return teams, nil
}

func (database *HeroBallDatabase) getTeamsById(teamIds []int32) ([]*pb.Team, error) {

	if teamIds == nil {
		return nil, fmt.Errorf("Invalid teamIds")
	}

	rows, err := database.db.Query(`
		SELECT
			TeamId,
			Name
		FROM
			Teams
		WHERE
			TeamId = ANY($1)
	`, pq.Array(teamIds))

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting teams: %v", err)
	}

	teams := make([]*pb.Team, 0)

	for rows.Next() {

		team := pb.Team{}

		err = rows.Scan(&team.TeamId, &team.Name)

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
			LocationId,
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

		err = rows.Scan(&location.LocationId, &location.Name)

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
			team, err := database.getTeamById(result.HomeTeamId)

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
			team, err := database.getTeamById(result.AwayTeamId)

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

	gameIds, err := database.getGameIdsForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	return database.getResultsForGames(gameIds)
}

func (database *HeroBallDatabase) getCompetitionRoundCount(competitionId int32) (int32, error) {

	var gameCount int32
	var teamCount int32

	if competitionId <= 0 {
		return 0, fmt.Errorf("Invalid competitionId")
	}

	err := database.db.QueryRow(`
		SELECT
			COUNT(Games.GameId)
		FROM
			Games
		WHERE
			CompetitionId = $1	
	`, competitionId).Scan(&gameCount)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	/* get team count */
	err = database.db.QueryRow(`
		SELECT
			COUNT(DISTINCT Games.HomeTeamId)
		FROM
			Games
		WHERE
			CompetitionId = $1
		UNION
		SELECT
			COUNT(DISTINCT Games.AwayTeamId)
		FROM
			Games
		WHERE
			CompetitionId = $1
	`, competitionId).Scan(&teamCount)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	/* round count is games divided by teams, floored */
	rounds := math.Floor(float64(gameCount) / float64(teamCount))

	return int32(rounds), nil

}

func (database *HeroBallDatabase) getTeamGameCount(teamId int32) (int32, error) {

	var teamGameCount int32

	if teamId <= 0 {
		return 0, fmt.Errorf("Invalid teamId")
	}

	err := database.db.QueryRow(`
		SELECT
			COUNT(DISTINCT PlayerGameStats.GameId)
		FROM
			PlayerGameStats
		WHERE
			TeamId = $1
	`, teamId).Scan(&teamGameCount)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return teamGameCount, nil

}

func (database *HeroBallDatabase) getCompetitionForTeam(teamId int32) (int32, error) {

	if teamId <= 0 {
		return 0, fmt.Errorf("Invalid teamId")
	}

	var competitionId int32

	err := database.db.QueryRow(`
		SELECT
			CompetitionId
		FROM
			PlayerGameStats
		LEFT JOIN
			Games ON Games.GameId = PlayerGameStats.GameId
		ORDER BY
			Games.GameTime ASC
		LIMIT 1
		`).Scan(&competitionId)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("Team does not exist")
	}

	if err != nil {
		return 0, fmt.Errorf("Error getting comp for team: %v", err)
	}

	return competitionId, nil
}

// func (database *HeroBallDatabase) getStatsLeadersForTeam(teamId int32) (*pb.BasicStatsLeaders, error) {

// 	competitionId, err := database.getCompetitionForTeam(teamId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	teamGameCount, err := database.getTeamGameCount(teamId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	requiredGameCount := int32(math.Ceil(float64(teamGameCount) / 3))

// 	return database.getStatsLeaders(competitionId, requiredGameCount, "PlayerGameStats.TeamId = $1", []interface{}{teamId})
// }

// func (database *HeroBallDatabase) getStatsLeadersForCompetition(competitionId int32) (*pb.BasicStatsLeaders, error) {

// 	roundsInComp, err := database.getCompetitionRoundCount(competitionId)

// 	if err != nil {
// 		return nil, err
// 	}

// 	requiredGameCount := int32(math.Ceil(float64(roundsInComp) / 3))

// 	return database.getStatsLeaders(competitionId, requiredGameCount, "Games.CompetitionId = $1", []interface{}{competitionId})
// }
