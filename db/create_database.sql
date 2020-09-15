CREATE TYPE playerposition AS ENUM(
    'guard', 
    'point-guard', 
    'shooting-guard', 
    'small-forward', 
    'forward', 
    'power-forward', 
    'center');

CREATE TABLE Leagues (
    LeagueId SERIAL PRIMARY KEY,
    Name text NOT NULL,
    Division text not null
);

CREATE TABLE Competitions (
    CompetitionId SERIAL PRIMARY KEY,
    LeagueId SERIAL NOT NULL REFERENCES Leagues(LeagueId),
    Name text NOT NULL
);

CREATE TABLE Teams (
    TeamId SERIAL PRIMARY KEY,
    Name text NOT NULL
);

CREATE TABLE Locations (
    LocationId SERIAL PRIMARY KEY,
    Name text NOT NULL 
);

CREATE TABLE Players (
    PlayerId SERIAL PRIMARY KEY,
    Name text NOT NULL,
    Position playerposition NOT NULL,
    Email text NOT NULL,
    YearStarted int,
    Description text
);

CREATE TABLE Games (
    GameId SERIAL PRIMARY KEY,
    CompetitionId SERIAL NOT NULL REFERENCES Competitions(CompetitionId),
    LocationId SERIAL NOT NULL REFERENCES Locations(LocationId),
    HomeTeamId SERIAL NOT NULL REFERENCES Teams(TeamId) CHECK (HomeTeamId != AwayTeamId),
    AwayTeamId SERIAL NOT NULL REFERENCES Teams(TeamId),
    GameTime TIMESTAMP NOT NULL
);

CREATE TABLE PlayerGameStats (
    StatsId SERIAL PRIMARY KEY,
    TeamId SERIAL NOT NULL REFERENCES Teams(TeamId),
    GameId SERIAL NOT NULL REFERENCES Games(GameId),
    PlayerId SERIAL NOT NULL REFERENCES Players(PlayerId),
    JerseyNumber int NOT NULL,
    TwoPointFGA int DEFAULT 0,
    TwoPointFGM int DEFAULT 0 CONSTRAINT two_point_validation CHECK (TwoPointFGA >= TwoPointFGM),
    ThreePointFGA int DEFAULT 0,
    ThreePointFGM int DEFAULT 0 CONSTRAINT three_point_validation CHECK (ThreePointFGA >= ThreePointFGM),
    FreeThrowsAttempted int DEFAULT 0,
    FreeThrowsMade int DEFAULT 0 CONSTRAINT free_throw_validation CHECK (FreeThrowsAttempted >= FreeThrowsMade),
    OffensiveRebounds int DEFAULT 0,
    DefensiveRebounds int DEFAULT 0,
    Assists int DEFAULT 0,
    Blocks int DEFAULT 0,
    Steals int DEFAULT 0,
    Turnovers int DEFAULT 0,
    RegularFoulsForced int DEFAULT 0,
    RegularFoulsCommitted int DEFAULT 0 CONSTRAINT regular_fouls_committed_validation CHECK (5 >= RegularFoulsCommitted),
    TechnicalFoulsCommitted int DEFAULT 0 CONSTRAINT technical_fouls_committed_validation CHECK (2 >= TechnicalFoulsCommitted),
    MinutesPlayed int DEFAULT 0
);

DROP FUNCTION TotalPoints;
CREATE FUNCTION TotalPoints(threes bigint, twos bigint, freeThrows bigint, out totalPoints bigint)
AS $$ SELECT 
    COALESCE(threes*3, 0) + 
    COALESCE(twos*2, 0) + 
    COALESCE(freeThrows, 0) $$
LANGUAGE SQL;

DROP MATERIALIZED VIEW GameScoresView;

CREATE MATERIALIZED VIEW GameScoresView AS
    SELECT
        GameId,
        CompetitionId,
        (SELECT TotalPoints(SUM(PlayerGameStats.ThreePointFGM), SUM(PlayerGameStats.TwoPointFGM), SUM(PlayerGameStats.FreeThrowsMade)) FROM PlayerGameStats WHERE TeamId = HomeTeamId AND PlayerGameStats.GameId = Games.GameId) As HomeTeamPoints,
        (SELECT TotalPoints(SUM(PlayerGameStats.ThreePointFGM), SUM(PlayerGameStats.TwoPointFGM), SUM(PlayerGameStats.FreeThrowsMade)) FROM PlayerGameStats WHERE TeamId = AwayTeamId AND PlayerGameStats.GameId = Games.GameId) As AwayTeamPoints
    FROM
        Games;

DROP MATERIALIZED VIEW CompetitionStandingsView;

CREATE MATERIALIZED VIEW CompetitionStandingsView AS
    SELECT
        CompetitionTeams.CompetitionId,
        CompetitionTeams.TeamId,
        (SELECT Name FROM Teams WHERE Teams.TeamId = CompetitionTeams.TeamId) As TeamName,
        (
            SELECT
                COUNT(GameScoresView.GameId)
            FROM
                Games LEFT JOIN 
                GameScoresView ON Games.GameId = GameScoresView.GameId
            WHERE
                (Games.AwayTeamId = CompetitionTeams.TeamId AND GameScoresView.AwayTeamPoints > GameScoresView.HomeTeamPoints) OR 
                (Games.HomeTeamId = CompetitionTeams.TeamId AND GameScoresView.HomeTeamPoints > GameScoresView.AwayTeamPoints)
        ) AS GamesWon,
        (
            SELECT
                COUNT(GameScoresView.GameId)
            FROM
                Games LEFT JOIN 
                GameScoresView ON Games.GameId = GameScoresView.GameId
            WHERE
                (Games.AwayTeamId = CompetitionTeams.TeamId AND GameScoresView.AwayTeamPoints = GameScoresView.HomeTeamPoints) OR 
                (Games.HomeTeamId = CompetitionTeams.TeamId AND GameScoresView.HomeTeamPoints = GameScoresView.AwayTeamPoints)
        ) AS GamesDrawn,
        (
            SELECT
                COUNT(GameScoresView.GameId)
            FROM
                Games LEFT JOIN 
                GameScoresView ON Games.GameId = GameScoresView.GameId
            WHERE
                (Games.AwayTeamId = CompetitionTeams.TeamId AND GameScoresView.AwayTeamPoints < GameScoresView.HomeTeamPoints) OR 
                (Games.HomeTeamId = CompetitionTeams.TeamId AND GameScoresView.HomeTeamPoints < GameScoresView.AwayTeamPoints)
        ) AS GamesLost
        FROM
            (SELECT CompetitionId, HomeTeamId As TeamId FROM Games UNION SELECT CompetitionId, AwayTeamId As TeamId FROM Games) As CompetitionTeams;

REFRESH MATERIALIZED VIEW CompetitionStandingsView;

