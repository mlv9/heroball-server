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
    ('Matthew', 'guard', 'something@email.com', 1998, 'Some Description');

INSERT INTO Games (
    CompetitionId, 
    LocationId, 
    HomeTeamId, 
    AwayTeamId, 
    GameTime
) VALUES (1,1,1,2,'2019-3-20'::timestamp);

INSERT INTO PlayerGameStats (
    PlayerId,
    CompetitionId,
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
        (1, 1, 1, 1, 9, 5,10,1,3,2,2,2,6,2,1,1,3,2,2,0,29),
        (2, 1, 1, 1, 8, 3,11,2,4,2,4,2,6,2,2,2,1,4,2,1,35),
        (3, 1, 1, 2, 11, 1,4,0,3,0,0,2,6,0,0,0,2,1,0,0,40),
        (4, 1, 1, 2, 2, 2,10,0,0,1,2,3,4,5,2,4,3,4,2,1,21);
