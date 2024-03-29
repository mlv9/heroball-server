#!/usr/bin/python3
# pip install psycopg2

from datetime import datetime
import psycopg2

def getPlayerName(statLine):
    return statLine[headingsIndex["playDispNm"]]

def getPlayerTeamName(statLine):
    return statLine[headingsIndex["teamAbbr"]]

def getOpposingTeamName(statLine):
    return statLine[headingsIndex["opptAbbr"]]

def getHomeTeam(statLine):
    opptLoc = statLine[headingsIndex["opptLoc"]]
    opptTeam = getOpposingTeamName(statLine)
    altTeam = getPlayerTeamName(statLine)
    if opptLoc == 'Home':
        return opptTeam
    return altTeam

def getAwayTeam(statLine):
    opptLoc = statLine[headingsIndex["opptLoc"]]
    opptTeam = getOpposingTeamName(statLine)
    altTeam = getPlayerTeamName(statLine)
    if opptLoc == 'Away':
        return opptTeam
    return altTeam

def getPlayerPosition(statLine):
    pos = statLine[headingsIndex["playPos"]]
    if pos == "F":
        return 'forward'
    elif pos == "G":
        return 'guard'
    elif pos == 'SG':
        return 'shooting-guard'
    elif pos == 'C':
        return 'center'
    elif pos == 'PF':
        return 'power-forward'
    elif pos == 'PG':
        return 'point-guard'
    elif pos == 'SF':
        return 'small-forward'
    else:
        raise Exception("Player position unknown: " + pos)

def getAST(statLine):
    return statLine[headingsIndex["playAST"]]

def getTO(statLine):
    return statLine[headingsIndex["playTO"]]

def getOREB(statLine):
    return statLine[headingsIndex["playORB"]]

def getDREB(statLine):
    return statLine[headingsIndex["playDRB"]]

def get2PFGA(statLine):
    return statLine[headingsIndex["play2PA"]]

def get2PFGM(statLine):
    return statLine[headingsIndex["play2PM"]]

def get3PFGM(statLine):
    return statLine[headingsIndex["play3PM"]]

def get3PFGA(statLine):
    return statLine[headingsIndex["play3PA"]]

def getFTA(statLine):
    return statLine[headingsIndex["playFTA"]]

def getFTM(statLine):
    return statLine[headingsIndex["playFTM"]]

def getBLK(statLine):
    return statLine[headingsIndex["playBLK"]]

def getMIN(statLine):
    return statLine[headingsIndex["playMin"]]

def getPFC(statLine):
    count = int(statLine[headingsIndex["playPF"]])
    if count > 5:
        count = 5
    return str(count)

def getSTL(statLine):
    return statLine[headingsIndex["playSTL"]]

def getGameDateTime(statLine):
    # '2017-10-17 08:00'
    return datetime.strptime(statLine[headingsIndex['gmDate']] + ' ' + statLine[headingsIndex['gmTime']], '%Y-%m-%d %H:%M')

headingsIndex = {}
statLines = []

fd = open('./2017-18_playerBoxScore.csv', 'r').read()
headings = fd.split('\n')[0].split(',')
statLines = fd.split('\n')[1:]
for i, val in enumerate(headings):
    headingsIndex[val] = i

# get teams and players
teams = {}
players = {}

for line in statLines:
    lineArr = line.split(',')
    if len(lineArr) < 2:
        continue

    name = getPlayerName(lineArr)
    position = getPlayerPosition(lineArr)
    teamName = getPlayerTeamName(lineArr)

    players[name] = {
        "Name": name,
        "Position": position,
        "Email": "player@nba.com",
        "YearStarted": "2000",
        "Description": "Player in 2017-18 NBA Season"
    }

    teams[teamName] = {"Name": teamName}

try:
    # now insert into our DB
    connection = psycopg2.connect(user = "postgres",
                                password = "postgres",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "postgres")
    print(connection)
    with connection.cursor() as cursor:
        insert_team_query = "INSERT INTO Teams (Name) VALUES (%s)"

        for team in teams:
            print("Inserting team " + teams[team]["Name"])
            cursor.execute(insert_team_query, (teams[team]["Name"],))

        insert_player_query = "INSERT INTO Players (Name, Position, Email, YearStarted, Description) VALUES (%s, %s, %s, %s, %s);"

        for player in players:
            print("Inserting player " + players[player]["Name"])
            cursor.execute(insert_player_query, (players[player]["Name"], players[player]["Position"], players[player]["Email"], players[player]["YearStarted"], players[player]["Description"]))

        expected_insert_count = len(players) + len(teams)

        connection.commit()
except Exception as error:
    if(connection):
        print("Failed to insert record into table", error)

# we need to build a map of each game to avoid duplicates
games = {}
for line in statLines:
    lineArr = line.split(',')
    if len(lineArr) < 2:
        continue

    homeTeam = getHomeTeam(lineArr)
    awayTeam = getAwayTeam(lineArr)
    gameTime = getGameDateTime(lineArr)

    gameKey = homeTeam + awayTeam + str(gameTime)

    games[gameKey] = {"HomeTeam": homeTeam, "AwayTeam": awayTeam, "GameTime": gameTime}

# insert games
with connection.cursor() as cursor:
    for game in games:
        #do insert into the DB
        cursor.execute("SELECT TeamId FROM Teams WHERE Name = %s;", (games[game]["HomeTeam"],))
        homeTeamId = cursor.fetchone()[0]
        cursor.execute("SELECT TeamId FROM Teams WHERE Name = %s;", (games[game]["AwayTeam"],))
        awayTeamId = cursor.fetchone()[0]

        print("Inserting game between " + games[game]["HomeTeam"] + " and " + games[game]["AwayTeam"] + " on " + str(games[game]["GameTime"]))
        cursor.execute("INSERT INTO Games (CompetitionId, LocationId, HomeTeamId, AwayTeamId, GameTime) VALUES (%s, %s, %s, %s, %s)", (1, 1, homeTeamId, awayTeamId, games[game]["GameTime"]))
        connection.commit()


# now insert each PlayerGameStat
stats = {}
with connection.cursor() as cursor:
    for line in statLines:
        lineArr = line.split(',')
        if len(lineArr) < 2:
            continue

        homeTeamName = getHomeTeam(lineArr)
        awayTeamName = getAwayTeam(lineArr)
        gameTime = getGameDateTime(lineArr)
        playerName = getPlayerName(lineArr)
        playerTeamName = getPlayerTeamName(lineArr)
    
        # oh dear, so much to do
        cursor.execute("""INSERT INTO PlayerGameStats (
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
                (
                    (SELECT PlayerId FROM Players WHERE Name = %s LIMIT 1), 
                    (SELECT 
                        GameId 
                    FROM 
                        Games 
                    WHERE 
                        HomeTeamId = (SELECT TeamId FROM Teams WHERE Name = %s LIMIT 1) AND 
                        AwayTeamId = (SELECT TeamId FROM Teams WHERE Name = %s LIMIT 1) AND 
                        GameTime = %s LIMIT 1),
                    (SELECT TeamId FROM Teams WHERE Name = %s LIMIT 1),
                    '0', -- jersey
                    %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (playerName, homeTeamName, awayTeamName, gameTime, playerTeamName, get2PFGM(lineArr), get2PFGA(lineArr), get3PFGM(lineArr), get3PFGA(lineArr), getFTM(lineArr), getFTA(lineArr), getOREB(lineArr), getDREB(lineArr), getAST(lineArr), getBLK(lineArr), getSTL(lineArr), getTO(lineArr), 0, getPFC(lineArr), 0, getMIN(lineArr)))
        connection.commit()

    # , 1, 1, 1, 2, 30, 2, 12, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),



