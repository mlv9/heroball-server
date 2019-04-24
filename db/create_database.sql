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
    HomeTeamId SERIAL NOT NULL REFERENCES Teams(TeamId),
    AwayTeamId SERIAL NOT NULL REFERENCES Teams(TeamId),
    GameTime TIMESTAMP NOT NULL
);

CREATE TABLE PlayerGameStats (
    StatsId SERIAL PRIMARY KEY,
    CompetitionId SERIAL NOT NULL REFERENCES Competitions(CompetitionId),
    TeamId SERIAL NOT NULL REFERENCES Teams(TeamId),
    GameId SERIAL NOT NULL REFERENCES Games(GameId),
    PlayerId SERIAL NOT NULL REFERENCES Players(PlayerId),
    JerseyNumber int NOT NULL,
    TwoPointFGA int DEFAULT 0,
    TwoPointFGM int DEFAULT 0 CHECK (TwoPointFGA >= TwoPointFGM),
    ThreePointFGA int DEFAULT 0,
    ThreePointFGM int DEFAULT 0 CHECK (ThreePointFGA >= ThreePointFGM),
    FreeThrowsAttempted int DEFAULT 0,
    FreeThrowsMade int DEFAULT 0 CHECK (FreeThrowsAttempted >= FreeThrowsMade),
    OffensiveRebounds int DEFAULT 0,
    DefensiveRebounds int DEFAULT 0,
    Assists int DEFAULT 0,
    Blocks int DEFAULT 0,
    Steals int DEFAULT 0,
    Turnovers int DEFAULT 0,
    RegularFoulsForced int DEFAULT 0,
    RegularFoulsCommitted int DEFAULT 0 CHECK (5 >= RegularFoulsCommitted),
    TechnicalFoulsCommitted int DEFAULT 0 CHECK (2 >= TechnicalFoulsCommitted),
    MinutesPlayed int DEFAULT 0
);
