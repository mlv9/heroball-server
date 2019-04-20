INSERT INTO Competitions (Name, SubCompetition) VALUES
    ("Fredcom", "B Division");

INSERT INTO Teams (Name) VALUES
    ("Cannons");

INSERT INTO Locations (Name) VALUES
    ("AIS");

INSERT INTO Players (Name, Position, Email, YearStarted, Description) VALUES
    ("Matthew", 'guard', "something@email.com", 1998, "Some Description");

INSERT INTO PlayerTeams (PlayerId, TeamId, JerseyNumber) VALUES
    (1, 1, 9);

INSERT INTO Stats (
    TwoPointFGA,
    TwoPointFGM,
    ThreePointFGA,
    ThreePointFGM,
    FreeThrowsAttempted,
    FreeThrowsMade,
    OffensiveRebounds,
    DefensiveRebounds,
    Assists,
    Blocks,
    Steals,
    Turnovers,
    RegularFoulsForced,
    RegularFoulsCommitted,
    TechnicalFoulsCommitted,
    MinutesPlayed
) VALUES (5,10,1,3,2,2,2,6,2,1,1,3,2,4,29);

INSERT INTO Games (
    CompetitionId, 
    LocationId, 
    HomeTeamId, 
    AwayTeamId, 
    GameDate, 
    GameTime
) VALUES (1,1,1,1,'2019-3-20','02:03:04');

INSERT INTO PlayerGames (PlayerId, GameId, TeamId, StatsId)
    VALUES (1, 1, 1, 1);