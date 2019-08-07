package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
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

	team, err := database.getTeamById(teamId)

	if err != nil {
		return nil, err
	}

	teamInfo.Team = team

	gameCursor, err := database.GetGamesCursor(0, recentGameCount, &pb.GamesFilter{
		TeamIds: []int32{teamId},
	})

	if err != nil {
		return nil, err
	}

	teamInfo.RecentGames = gameCursor

	var maxTeamSize int32 = 30

	playersCursor, err := database.GetPlayersCursor(0, maxTeamSize, &pb.PlayersFilter{
		TeamIds: []int32{teamId},
	})

	if err != nil {
		return nil, err
	}

	teamInfo.Players = playersCursor

	/* get the comp */
	compId, err := database.getCompetitionForTeam(teamId)

	if err != nil {
		return nil, err
	}

	competition, err := database.getCompetitionById(compId)

	if err != nil {
		return nil, err
	}

	teamInfo.Competition = competition

	return teamInfo, nil
}

func (database *HeroBallDatabase) GetCompetitionInfo(competitionId int32) (*pb.CompetitionInfo, error) {

	if competitionId <= 0 {
		return nil, fmt.Errorf("Invalid competitionId")
	}

	compInfo := &pb.CompetitionInfo{}

	/* now fill it out */
	comp, err := database.getCompetitionById(competitionId)

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

	gameCursor, err := database.GetGamesCursor(0, recentGameCount, &pb.GamesFilter{
		CompetitionIds: []int32{competitionId},
	})

	if err != nil {
		return nil, err
	}

	compInfo.RecentGames = gameCursor

	return compInfo, nil
}

func (database *HeroBallDatabase) GetStats(request *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {

	forRequest := &pb.ForStatsRequest{
		CompetitionIds: make([]int32, 0),
		TeamIds:        make([]int32, 0),
		PlayerIds:      make([]int32, 0),
	}

	againstRequest := &pb.AgainstStatsRequest{
		CompetitionIds: make([]int32, 0),
		TeamIds:        make([]int32, 0),
	}

	if request.For != nil {
		forRequest = request.GetFor()
	}

	if request.Against == nil {
		againstRequest = request.GetAgainst()
	}

	combinedCompIds := append(forRequest.CompetitionIds, forRequest.CompetitionIds...)

	/* we need to get stats leaders */
	leaders, playerIds, err := database.getAggregateStatsByConditionAndGroupingAndOrderAndLimitAndOffset(
		`(cardinality($1::int[]) IS NULL OR Games.CompetitionId = ANY($1)) AND
		(cardinality($2::int[]) IS NULL OR NOT (PlayerGameStats.TeamId = ANY($2))) AND
		(cardinality($3::int[]) IS NULL OR PlayerGameStats.TeamId = ANY($3)) AND
		(cardinality($4::int[]) IS NULL OR PlayerGameStats.PlayerId = ANY($4))`,
		[]interface{}{
			pq.Array(combinedCompIds),
			pq.Array(againstRequest.TeamIds),
			pq.Array(forRequest.TeamIds),
			pq.Array(forRequest.PlayerIds)},
		"GROUP BY PlayerGameStats.PlayerId",
		"PlayerGameStats.PlayerId",
		"HAVING COUNT(PlayerGameStats.StatsId) >= $5",
		[]interface{}{request.MinimumGames},
		`ORDER BY 
			((COALESCE(SUM(PlayerGameStats.ThreePointFGM)*3, 0) + 
			COALESCE(SUM(PlayerGameStats.TwoPointFGM)*2, 0) + 
			COALESCE(SUM(PlayerGameStats.FreeThrowsMade), 0)) / COUNT(PlayerGameStats.StatsId))
		DESC`, request.GetCount(), request.GetOffset())

	if err != nil {
		return nil, err
	}

	if len(playerIds) < 1 {
		return &pb.GetStatsResponse{}, nil
	}

	players, err := database.getPlayersById(playerIds)

	if err != nil {
		return nil, err
	}

	playerMap := make(map[int32]*pb.Player)

	/* into map */
	for _, p := range players {
		playerMap[p.PlayerId] = p
	}

	playerStats := make([]*pb.PlayerAggregateStats, 0)

	for i, playerStatLine := range leaders {
		playerStats = append(playerStats, &pb.PlayerAggregateStats{
			Player: playerMap[playerIds[i]],
			Stats:  playerStatLine,
		})
	}

	return &pb.GetStatsResponse{
		AggregateStats: playerStats,
	}, nil
}

func (database *HeroBallDatabase) GetHeroBallMetadata(request *pb.GetHeroBallMetadataRequest) (*pb.HeroBallMetadata, error) {

	md := &pb.HeroBallMetadata{}

	if request.Competitions {
		/* we need competitions, teams, players */
		competitions, err := database.getAllCompetitions()

		if err != nil {
			return nil, fmt.Errorf("Error getting competitions: %v", err)
		}

		md.Competitions = competitions
	}

	if request.Teams {
		teams, err := database.getAllTeams()

		if err != nil {
			return nil, fmt.Errorf("Error getting teams: %v", err)
		}
		md.Teams = teams
	}

	if request.Players {
		players, err := database.getAllPlayers()

		if err != nil {
			return nil, fmt.Errorf("Error getting players: %v", err)
		}

		md.Players = players
	}

	return md, nil
}

func (database *HeroBallDatabase) GetGameInfo(gameId int32) (*pb.GameInfo, error) {

	if gameId <= 0 {
		return nil, fmt.Errorf("Invalid gameId")
	}

	gameInfo := &pb.GameInfo{}

	game, err := database.getGameById(gameId)

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

	info.AggregateStats = totalStats

	gameCursor, err := database.GetGamesCursor(0, recentGameCount, &pb.GamesFilter{
		PlayerIds: []int32{playerId},
	})

	if err != nil {
		return nil, err
	}

	info.RecentGames = gameCursor

	recentStats := make([]*pb.PlayerGameStats, 0)

	/* if there are recent games, get player stats too */
	if len(info.RecentGames.Games) != 0 {
		for _, game := range info.RecentGames.Games {

			playerStats, err := database.getPlayerStatsForGame(playerId, game.GameId)

			if err != nil {
				return nil, err
			}

			recentStats = append(recentStats, playerStats)
		}

		info.RecentStats = recentStats
	}

	return info, nil
}

func (database *HeroBallDatabase) GetPlayersCursor(offset int32, count int32, filter *pb.PlayersFilter) (*pb.PlayersCursor, error) {

	if offset < 0 {
		return nil, fmt.Errorf("Invalid offset, must be zero (ignored) or greater")
	}

	if count <= 0 {
		return nil, fmt.Errorf("Invalid count, must be greater than zero")
	}

	var totalPlayers int32

	err := database.db.QueryRow(`
		SELECT
			COUNT(DISTINCT Players.PlayerId)
		FROM
			Players
		LEFT JOIN
			PlayerGameStats ON Players.PlayerId = PlayerGameStats.PlayerId
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId
		WHERE
			(cardinality($1::int[]) IS NULL OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2::int[]) IS NULL OR PlayerGameStats.TeamId = ANY($2))`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetTeamIds())).Scan(&totalPlayers)

	if err != nil {
		return nil, fmt.Errorf("Error getting player count for cursor: %v", err)
	}

	/* if the count is less than offset, return */
	if offset > totalPlayers {
		return nil, fmt.Errorf("Requesting (%v) past the end of the result set length (%v)", offset, totalPlayers)
	}

	/* if no matches, return */
	if totalPlayers == 0 {
		log.Printf("Returning 0 players for filter %v", filter)
		return &pb.PlayersCursor{
			Filter: filter,
			Total:  0,
		}, nil
	}

	/* get the playerIds */
	rows, err := database.db.Query(`
		SELECT
			DISTINCT
			Players.PlayerId,
			Players.Name
		FROM
			Players
		LEFT JOIN
			PlayerGameStats ON Players.PlayerId = PlayerGameStats.PlayerId
		LEFT JOIN
			Games ON PlayerGameStats.GameId = Games.GameId
		WHERE
			(cardinality($1::int[]) IS NULL OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2::int[]) IS NULL OR PlayerGameStats.TeamId = ANY($2))
		ORDER BY
			Players.Name DESC
		LIMIT $3
		OFFSET $4
		`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetTeamIds()),
		count,
		offset)

	/* this shouldn't hit as we previously did a count */
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Count mismatch error 63")
	}

	if err != nil {
		return nil, fmt.Errorf("Error getting players: %v", err)
	}

	/* otherwise scan the players required */
	playerIds := make([]int32, 0)

	for rows.Next() {

		var playerId int32
		var name string

		err = rows.Scan(&playerId, &name)

		if err != nil {
			return nil, fmt.Errorf("Error scanning players: %v", err)
		}

		playerIds = append(playerIds, playerId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error following scan: %v", err)
	}

	/* now lets get the players */
	/* TODO work out what this call should be... */
	players, err := database.getPlayersById(playerIds)

	/* return offset and gameIds len */
	if err != nil {
		return nil, err
	}

	/* calculate next offset */
	nextOffset := offset + int32(len(players))

	if nextOffset > totalPlayers {
		nextOffset = totalPlayers
	}

	log.Printf("Returning %v players for request from filter %+v for count %v from offset %v", len(players), filter, count, offset)

	return &pb.PlayersCursor{
		Total:      totalPlayers,
		NextOffset: nextOffset,
		Players:    players,
		Filter:     filter,
	}, nil
}

/* TODO seperate query if null filter, will be much cheaper */
func (database *HeroBallDatabase) GetGamesCursor(offset int32, count int32, filter *pb.GamesFilter) (*pb.GamesCursor, error) {

	/* get the count across the filter */
	if offset < 0 {
		return nil, fmt.Errorf("Invalid offset, must be zero or greater")
	}

	if count <= 0 {
		return nil, fmt.Errorf("Invalid count, must be greater than zero")
	}

	/* lets validate any dates */
	date := filter.GetDate()
	dateParsed := pq.NullTime{}
	var err error

	if date != nil {
		pDate, err := time.Parse("2006-01-02", fmt.Sprintf("%04d-%02d-%02d", date.Year, date.Month, date.Day))

		if err != nil {
			return nil, fmt.Errorf("Error parsing date: %v", err)
		}

		dateParsed.Time = pDate
		dateParsed.Valid = true
	}

	var totalGames int32

	/* get the count - potentially expensive for each cursor page... */
	err = database.db.QueryRow(`
		SELECT
			COUNT(DISTINCT Games.GameId)
		FROM
			Games
		LEFT JOIN
			PlayerGameStats ON Games.GameId = PlayerGameStats.GameId
		WHERE
			(cardinality($1::int[]) IS NULL OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2::int[]) IS NULL OR PlayerGameStats.PlayerId = ANY($2)) AND
			(cardinality($3::int[]) IS NULL OR (Games.HomeTeamId = ANY($3) OR Games.AwayTeamId = ANY($3))) AND
			(COALESCE($4 > current_timestamp) IS NULL OR (Games.GameTime >= $4 AND Games.GameTime < $4 + interval '1 day'))`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetPlayerIds()),
		pq.Array(filter.GetTeamIds()),
		dateParsed).Scan(&totalGames)

	if err != nil {
		return nil, fmt.Errorf("Error getting game count for cursor: %v", err)
	}

	/* if the count is less than offset, return */
	if offset > totalGames {
		return nil, fmt.Errorf("Requesting (%v) past the end of the result set length (%v)", offset, totalGames)
	}

	/* if no matches, return */
	if totalGames == 0 {
		log.Printf("Returning 0 games for filter %v", filter)
		return &pb.GamesCursor{
			Filter: filter,
			Total:  0,
		}, nil
	}

	/* get the gameIds */
	rows, err := database.db.Query(`
		SELECT
			DISTINCT
			Games.GameId,
			Games.GameTime
		FROM
			Games
		LEFT JOIN
			PlayerGameStats ON Games.GameId = PlayerGameStats.GameId
		WHERE
			(cardinality($1::int[]) IS NULL OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2::int[]) IS NULL OR PlayerGameStats.PlayerId = ANY($2)) AND
			(cardinality($3::int[]) IS NULL OR (Games.HomeTeamId = ANY($3) OR Games.AwayTeamId = ANY($3))) AND
			(COALESCE($4 > current_timestamp) IS NULL OR (Games.GameTime >= $4 AND Games.GameTime < $4 + interval '1 day'))
		ORDER BY
			Games.GameTime DESC
		LIMIT $5
		OFFSET $6
	`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetPlayerIds()),
		pq.Array(filter.GetTeamIds()),
		dateParsed,
		count,
		offset)

	/* this shouldn't hit */
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Count mismatch error 62")
	}

	if err != nil {
		return nil, err
	}

	/* otherwise scan the gameIds required */
	gameIds := make([]int32, 0)

	for rows.Next() {

		var gameId int32
		var gameTime string

		err = rows.Scan(&gameId, &gameTime)

		if err != nil {
			return nil, fmt.Errorf("Error scanning games: %v", err)
		}

		gameIds = append(gameIds, gameId)
	}

	err = rows.Err()

	if err != nil {
		return nil, fmt.Errorf("Error following scan: %v", err)
	}

	/* get the games */
	games, err := database.getGamesById(gameIds)

	/* return offset and gameIds len */
	if err != nil {
		return nil, err
	}

	/* calculate next offset */
	nextOffset := offset + int32(len(games))

	if nextOffset > totalGames {
		nextOffset = totalGames
	}

	log.Printf("Returning %v games for request from filter %+v for count %v from offset %v", len(games), filter, count, offset)

	return &pb.GamesCursor{
		Total:      totalGames,
		NextOffset: nextOffset,
		Games:      games,
		Filter:     filter,
	}, nil
}
