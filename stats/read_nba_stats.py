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
    return int(statLine[headingsIndex["playAST"]])

def getTO(statLine):
    return int(statLine[headingsIndex["playTO"]])

def getOREB(statLine):
    return int(statLine[headingsIndex["playORB"]])

def getDREB(statLine):
    return int(statLine[headingsIndex["playDRB"]])

def get2PFGA(statLine):
    return int(statLine[headingsIndex["play2PA"]])

def get2PFGM(statLine):
    return int(statLine[headingsIndex["play2PM"]])

def get3PFGA(statLine):
    return int(statLine[headingsIndex["play3PA"]])

def get2PFGM(statLine):
    return int(statLine[headingsIndex["play3PM"]])

def getFTA(statLine):
    return int(statLine[headingsIndex["playFTA"]])

def getFTM(statLine):
    return int(statLine[headingsIndex["playFTM"]])

def getBLK(statLine):
    return int(statLine[headingsIndex["playBLK"]])

def getMIN(statLine):
    return int(statLine[headingsIndex["playMIN"]])

def getPFC(statLine):
    count = int(statLine[headingsIndex["playPF"]])
    if count > 5:
        count = 5
    return count

def getSTL(statLine):
    return int(statLine[headingsIndex["playSTL"]])

def getGameDateTime(statLine):
    # 17/10/2017 8:00:00 am
    return datetime.strptime(statLine[headingsIndex['gmDate']] + ' ' + statLine[headingsIndex['gmTime']], '%d/%m/%Y %H:%M:%S %p')

headingsIndex = {}
statLines = []

def initDataSet():
    fd = open('./2017-18_playerBoxScore.csv', 'r').read()
    headings = fd.split('\n')[0].split(',')
    statLines = fd.split('\n')[1:]
    for i, val in enumerate(headings):
        headingsIndex[val] = i

def initTeamsAndPlayers():
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
            "Name":name,
            "Position": position,
            "Email": "player@nba.com",
            "YearStarted": 2000,
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
        with connection.cursor() as cursor:
            insert_team_query = "INSERT INTO Teams (Name) VALUES (%s);"

            for team in teams:
                cursor.execute(insert_team_query, (team["Name"]))

            insert_player_query = "INSERT INTO Players (Name, Position, Email, YearStarted, Description) VALUES (%s, %s, %s, %s, %s);"

      	    for player in players:
      	            cursor.execute(insert_player_query, (player["Name"], player["Position"], player["Email"], player["YearStarted"], player["Description"]))

       	    expected_insert_count = len(players) + len(teams)

            if cursor.rowcount != expected_insert_count:
                    raise Exception("Expected " , expected_insert_count , " inserts, got " , cursor.rowcount)

    except Exception as error:
        if(connection):
            print("Failed to insert record into table", error)

def initGames():
    # we need to build a map of each game and insert the record
    games = {}

    for line in statLines:
        lineArr = line.split(',')
        if len(lineArr) < 2:
            continue

        homeTeam = getHomeTeam(line)
        awayTeam = getAwayTeam(line)
        location = 'AIS'
        competition = 'NBA 2017-18'
        gameTime = getGameDateTime(line)

        #do insert into the DB


# def loadGameStatsIntoDB():

#     homeTeam = 
#     awayTeam = 
#     gameDateTime = 
    
    # oh dear, so much to do
    # INSERT INTO PlayerGameStats (
    #     PlayerId,
    #     GameId, 
    #     TeamId, 
    #     JerseyNumber,
    #     TwoPointFGM,
    #     TwoPointFGA,
    #     ThreePointFGM,
    #     ThreePointFGA,
    #     FreeThrowsMade,
    #     FreeThrowsAttempted,
    #     OffensiveRebounds,
    #     DefensiveRebounds,
    #     Assists,
    #     Blocks,
    #     Steals,
    #     Turnovers,
    #     RegularFoulsForced,
    #     RegularFoulsCommitted,
    #     TechnicalFoulsCommitted,
    #     MinutesPlayed)
    #     VALUES 
    #         (
#                  (SELECT PlayerId FROM Players WHERE Name = %s), 
#                  (SELECT 
#                       GameId 
#                  FROM 
#                       Games 
#                   WHERE 
#                       AwayTeamId = (SELECT TeamId FROM Teams WHERE Name = %s) AND 
#                       HomeTeamId = (SELECT TeamId FROM Teams WHERE Name = %s) AND 
#                       GameTime = %s),
#                   (SELECT TeamId FROM Teams WHERE Name = %s),
#                   0, -- jersey number, lets fake it for now
#                   %s,


    # , 1, 1, 1, 2, 30, 2, 12, 2, 2, 2, 6, 2, 1, 1, 3, 2, 2, 0, 29),





initDataSet()
initTeamsAndPlayers()
initGames()

