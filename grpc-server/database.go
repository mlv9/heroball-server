package main

import (
	"database/sql"
	"fmt"
	"log"

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

	competition, err := database.getCompetitionById(compId)

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

	statLeaders, err := database.getStatsLeadersForCompetition(competitionId)

	if err != nil {
		return nil, err
	}

	compInfo.StatsLeaders = statLeaders

	gameCursor, err := database.GetGamesCursor(0, recentGameCount, &pb.GamesFilter{
		CompetitionIds: []int32{competitionId},
	})

	if err != nil {
		return nil, err
	}

	compInfo.RecentGames = gameCursor

	return compInfo, nil
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

	info.StatsAllTime = totalStats

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

/* TODO seperate query if null filter, will be much cheaper */
func (database *HeroBallDatabase) GetGamesCursor(offset int32, count int32, filter *pb.GamesFilter) (*pb.GamesCursor, error) {

	/* get the count across the filter */
	if offset < 0 {
		return nil, fmt.Errorf("Invalid offset")
	}

	if count < 0 {
		return nil, fmt.Errorf("Invalid count")
	}

	var totalGames int32

	log.Printf("PlayerArray: %v", pq.Array(filter.GetPlayerIds()))

	/* get the count - potentially expensive for each cursor page... */
	err := database.db.QueryRow(`
		SELECT
			COUNT(Games.GameId)
		FROM
			Games
		LEFT JOIN
			PlayerGameStats ON Games.GameId = PlayerGameStats.GameId
		WHERE
			(cardinality($1) = 0 OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2) = 0 OR PlayerGameStats.PlayerId = ANY($2)) AND
			(cardinality($3) = 0 OR (Games.HomeTeamId = ANY($3) OR Games.AwayTeamId = ANY($3)))
	`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetPlayerIds()),
		pq.Array(filter.GetTeamIds())).Scan(&totalGames)

	if err != nil {
		return nil, fmt.Errorf("Error getting game count for cursor: %v", err)
	}

	/* if the count is less than offset, return */
	if offset < totalGames {
		return nil, fmt.Errorf("Requesting (%v) past the end of the result set length (%v)", offset, totalGames)
	}

	/* if no matches, return */
	if totalGames == 0 {
		return &pb.GamesCursor{
			Total: 0,
		}, nil
	}

	/* get the gameIds */
	rows, err := database.db.Query(`
		SELECT
			Games.GameId
		FROM
			Games
		LEFT JOIN
			PlayerGameStats ON Games.GameId = PlayerGameStats.GameId
		WHERE
			(cardinality($1) = 0 OR Games.CompetitionId = ANY($1)) AND
			(cardinality($2) = 0 OR PlayerGameStats.PlayerId = ANY($2)) AND
			(cardinality($3) = 0 OR (Games.HomeTeamId = ANY($3) OR Games.AwayTeamId = ANY($3)))
		ORDER BY
			GameTime DESC
		LIMIT $4 
		OFFSET $5
	`,
		pq.Array(filter.GetCompetitionIds()),
		pq.Array(filter.GetPlayerIds()),
		pq.Array(filter.GetTeamIds()),
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

		err = rows.Scan(&gameId)

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

	return &pb.GamesCursor{
		Total:      totalGames,
		NextOffset: nextOffset,
		Games:      games,
	}, nil
}
