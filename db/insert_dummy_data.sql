INSERT INTO Competitions (Name, SubCompetition) VALUES
    ('Fredcom', 'B Division'),
    ('Fredcom', 'A Division');

INSERT INTO Teams (Name) VALUES
    ('Cannons'),
    ('Owls');

INSERT INTO Locations (Name) VALUES
    ('AIS');

INSERT INTO Players (Name, Position, Email, YearStarted, Description) VALUES
    ('Steven', 'center', 'something2@email.com', 1999, 'Some Description For Steven'),
    ('Matthew', 'guard', 'something@email.com', 1998, 'Some Description');

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
) VALUES (5,10,1,3,2,2,2,6,2,1,1,3,2,4,0,29);

INSERT INTO Games (
    CompetitionId, 
    LocationId, 
    HomeTeamId, 
    AwayTeamId, 
    GameTime
) VALUES (1,1,1,2,'2019-3-20'::timestamp);

INSERT INTO PlayerGames (PlayerId, GameId, TeamId, StatsId, JerseyNumber)
    VALUES (1, 1, 1, 1, 9);