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
    ('Jake', 'point-guard', 'something3@email.com', 2007, 'Some Description For Jake'),
    ('Andrew', 'small-forward', 'something4@email.com', 2012, 'Some Description For Andrew'),
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
) VALUES 
    (5,10,1,3,2,2,2,6,2,1,1,3,2,4,0,29),
    (3,11,2,4,2,4,2,6,2,2,2,1,4,2,1,35),
    (1,4,0,3,0,0,2,6,0,0,0,2,1,0,0,40),
    (2,10,0,0,1,2,3,4,5,2,4,3,4,2,1,21);

INSERT INTO Games (
    CompetitionId, 
    LocationId, 
    HomeTeamId, 
    AwayTeamId, 
    GameTime
) VALUES (1,1,1,2,'2019-3-20'::timestamp);

INSERT INTO PlayerGames (PlayerId, GameId, TeamId, StatsId, JerseyNumber)
    VALUES 
        (1, 1, 1, 1, 9),
        (2, 1, 1, 2, 8),
        (3, 1, 2, 3, 11),
        (4, 1, 2, 4, 2);