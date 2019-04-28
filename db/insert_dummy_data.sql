INSERT INTO Leagues (Name, Division) VALUES
    ('Fredcom', 'B Division'),
    ('Fredcom', 'A Division');

INSERT INTO Competitions (Name, LeagueId) VALUES
    ('Winter 2019', 1),
    ('Winter 2019', 2);

INSERT INTO Teams (Name) VALUES
    ('Cannons'),
    ('Owls');

INSERT INTO Locations (Name) VALUES
    ('AIS');

INSERT INTO Players (Name, Position, Email, YearStarted, Description) VALUES
    ('Steven', 'center', 'something2@email.com', 1999, 'Some Description For Steven'),
    ('Jake', 'point-guard', 'something3@email.com', 2007, 'Some Description For Jake'),
    ('Andrew', 'small-forward', 'something4@email.com', 2012, 'Some Description For Andrew'),
    ('Peter', 'small-forward', 'something4@email.com', 2012, 'Some Description For Peter'),
    ('John', 'small-forward', 'something4@email.com', 2012, 'Some Description For John'),
    ('Aaron', 'power-forward', 'something4@email.com', 2012, 'Some Description For Aaron'),
    ('Mike', 'shooting-guard', 'something4@email.com', 2012, 'Some Description For Mike'),
    ('Jamaal', 'small-forward', 'something4@email.com', 2012, 'Some Description For Jamaal'),
    ('Craig S', 'guard', 'something@email.com', 1998, 'Some Description'),
    ('Matthew', 'center', 'something@email.com', 1998, 'Some Description');

INSERT INTO Games (
    CompetitionId, 
    LocationId, 
    HomeTeamId, 
    AwayTeamId, 
    GameTime
) VALUES 
    (1,1,1,2,'2019-3-26'::timestamp),
    (1,1,2,1,'2019-3-21'::timestamp),
    (1,1,1,2,'2019-3-22'::timestamp),
    (1,1,2,1,'2019-3-23'::timestamp),
    (1,1,1,2,'2019-3-22'::timestamp),
    (1,1,2,1,'2019-3-23'::timestamp),
    (1,1,1,2,'2019-3-22'::timestamp),
    (1,1,2,1,'2019-3-23'::timestamp),
    (1,1,1,2,'2019-3-24'::timestamp),
    (1,1,2,1,'2019-3-25'::timestamp);

INSERT INTO PlayerGameStats (
    PlayerId,
    GameId, 
    TeamId, 
    JerseyNumber,
    TwoPointFGM,
    TwoPointFGA,
    ThreePointFGM,
    ThreePointFGA,
    FreeThrowsMade,
    FreeThrowsAttempted,
    OffensiveRebounds,
    DefensiveRebounds,
    Assists,
    Blocks,
    Steals,
    Turnovers,
    RegularFoulsForced,
    RegularFoulsCommitted,
    TechnicalFoulsCommitted,
    MinutesPlayed)
    VALUES 
        (1, 1, 1, 1, 2, 30, 2, 12, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),
        (2, 1, 1, 2, 10, 30, 1, 12, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),
        (3, 1, 1, 3, 8, 30, 0, 12, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),
        (4, 1, 1, 4, 2, 30, 3, 6, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),
        (5, 1, 1, 5, 0, 30, 0, 0, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),
        (1, 2, 1, 8, 3, 11, 2, 4, 2, 4, 2, 6, 2, 2, 2, 1, 4, 2, 1, 35),
        (2, 3, 1, 7, 3, 11, 2, 4, 2, 4, 2, 6, 2, 2, 2, 1, 4, 2, 1, 35),
        (3, 4, 1, 6, 3, 11, 2, 4, 2, 4, 2, 6, 2, 2, 2, 1, 4, 2, 1, 35),
        (4, 5, 1, 5, 3, 11, 2, 4, 2, 4, 2, 6, 2, 2, 2, 1, 4, 2, 1, 35),
        (5, 6, 1, 4, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (1, 7, 1, 3, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (1, 8, 1, 3, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (1, 9, 1, 3, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (2, 8, 1, 2, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (3, 9, 1, 1, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (4, 10, 1, 0, 1, 4, 0, 3, 0, 0, 2, 6, 0, 0, 0, 2, 1, 0, 0, 40),
        (6, 1, 2, 1, 2, 10, 0, 2, 1, 2, 3, 4, 5, 1, 1, 0, 4, 2, 1, 21),
        (7, 1, 2, 2, 4, 10, 1, 3, 1, 2, 3, 4, 5, 2, 2, 2, 3, 1, 1, 21),
        (8, 1, 2, 3, 5, 10, 2, 2, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 0, 21),
        (9, 1, 2, 4, 7, 10, 2, 4, 1, 2, 3, 4, 5, 0, 0, 4, 2, 1, 0, 21),
        (10, 1, 2, 5, 9, 15, 0, 2, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (6, 2, 2, 3, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (7, 3, 2, 4, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (8, 4, 2, 5, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (9, 5, 2, 6, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (10, 6, 2, 7, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (6, 7, 2, 8, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (7, 8, 2, 9, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (8, 9, 2, 10, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21),
        (9, 10, 2, 11, 2, 10, 0, 0, 1, 2, 3, 4, 5, 2, 4, 3, 4, 2, 1, 21);
