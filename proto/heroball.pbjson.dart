///
//  Generated code. Do not modify.
//  source: heroball.proto
///
// ignore_for_file: camel_case_types,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name

const Player$json = const {
  '1': 'Player',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
    const {'1': 'Name', '3': 2, '4': 1, '5': 9, '10': 'Name'},
    const {'1': 'Position', '3': 3, '4': 1, '5': 9, '10': 'Position'},
  ],
};

const PlayerStats$json = const {
  '1': 'PlayerStats',
  '2': const [
    const {'1': 'Email', '3': 2, '4': 1, '5': 9, '10': 'Email'},
    const {'1': 'Password', '3': 3, '4': 1, '5': 9, '10': 'Password'},
    const {'1': 'CreatedTimestamp', '3': 4, '4': 1, '5': 3, '10': 'CreatedTimestamp'},
  ],
};

const Competition$json = const {
  '1': 'Competition',
  '2': const [
    const {'1': 'CompetitionId', '3': 1, '4': 1, '5': 5, '10': 'CompetitionId'},
    const {'1': 'Name', '3': 2, '4': 1, '5': 9, '10': 'Name'},
    const {'1': 'Division', '3': 3, '4': 1, '5': 9, '10': 'Division'},
  ],
};

const Team$json = const {
  '1': 'Team',
  '2': const [
    const {'1': 'TeamId', '3': 1, '4': 1, '5': 5, '10': 'TeamId'},
    const {'1': 'Name', '3': 2, '4': 1, '5': 9, '10': 'Name'},
  ],
};

const Location$json = const {
  '1': 'Location',
  '2': const [
    const {'1': 'LocationId', '3': 1, '4': 1, '5': 5, '10': 'LocationId'},
    const {'1': 'Name', '3': 2, '4': 1, '5': 9, '10': 'Name'},
  ],
};

const Date$json = const {
  '1': 'Date',
  '2': const [
    const {'1': 'year', '3': 1, '4': 1, '5': 5, '10': 'year'},
    const {'1': 'month', '3': 2, '4': 1, '5': 5, '10': 'month'},
    const {'1': 'day', '3': 3, '4': 1, '5': 5, '10': 'day'},
  ],
};

const Time$json = const {
  '1': 'Time',
  '2': const [
    const {'1': 'hour', '3': 1, '4': 1, '5': 5, '10': 'hour'},
    const {'1': 'minute', '3': 2, '4': 1, '5': 5, '10': 'minute'},
  ],
};

const Game$json = const {
  '1': 'Game',
  '2': const [
    const {'1': 'GameId', '3': 1, '4': 1, '5': 5, '10': 'GameId'},
    const {'1': 'PlayerStats', '3': 2, '4': 3, '5': 11, '6': '.pb.Game.PlayerStatsEntry', '10': 'PlayerStats'},
    const {'1': 'HomeTeam', '3': 3, '4': 1, '5': 11, '6': '.pb.Team', '10': 'HomeTeam'},
    const {'1': 'AwayTeam', '3': 4, '4': 1, '5': 11, '6': '.pb.Team', '10': 'AwayTeam'},
    const {'1': 'Location', '3': 5, '4': 1, '5': 11, '6': '.pb.Location', '10': 'Location'},
    const {'1': 'Competition', '3': 6, '4': 1, '5': 11, '6': '.pb.Competition', '10': 'Competition'},
    const {'1': 'Date', '3': 7, '4': 1, '5': 11, '6': '.pb.Date', '10': 'Date'},
    const {'1': 'Time', '3': 8, '4': 1, '5': 11, '6': '.pb.Time', '10': 'Time'},
  ],
  '3': const [Game_PlayerStatsEntry$json],
};

const Game_PlayerStatsEntry$json = const {
  '1': 'PlayerStatsEntry',
  '2': const [
    const {'1': 'key', '3': 1, '4': 1, '5': 5, '10': 'key'},
    const {'1': 'value', '3': 2, '4': 1, '5': 11, '6': '.pb.PlayerGameStats', '10': 'value'},
  ],
  '7': const {'7': true},
};

const Stats$json = const {
  '1': 'Stats',
  '2': const [
    const {'1': 'StatsId', '3': 1, '4': 1, '5': 5, '10': 'StatsId'},
    const {'1': 'TwoPointFGA', '3': 2, '4': 1, '5': 5, '10': 'TwoPointFGA'},
    const {'1': 'TwoPointFGM', '3': 3, '4': 1, '5': 5, '10': 'TwoPointFGM'},
    const {'1': 'ThreePointFMA', '3': 4, '4': 1, '5': 5, '10': 'ThreePointFMA'},
    const {'1': 'ThreePointFGM', '3': 5, '4': 1, '5': 5, '10': 'ThreePointFGM'},
    const {'1': 'FreeThrowsAttempted', '3': 6, '4': 1, '5': 5, '10': 'FreeThrowsAttempted'},
    const {'1': 'FreeThrowsMade', '3': 7, '4': 1, '5': 5, '10': 'FreeThrowsMade'},
    const {'1': 'OffensiveRebounds', '3': 8, '4': 1, '5': 5, '10': 'OffensiveRebounds'},
    const {'1': 'DefensiveRebounds', '3': 9, '4': 1, '5': 5, '10': 'DefensiveRebounds'},
    const {'1': 'Assists', '3': 10, '4': 1, '5': 5, '10': 'Assists'},
    const {'1': 'Blocks', '3': 11, '4': 1, '5': 5, '10': 'Blocks'},
    const {'1': 'Steals', '3': 12, '4': 1, '5': 5, '10': 'Steals'},
    const {'1': 'Turnovers', '3': 13, '4': 1, '5': 5, '10': 'Turnovers'},
    const {'1': 'RegularFoulsForced', '3': 14, '4': 1, '5': 5, '10': 'RegularFoulsForced'},
    const {'1': 'RegularFoulsCommitted', '3': 15, '4': 1, '5': 5, '10': 'RegularFoulsCommitted'},
    const {'1': 'TechnicalFoulsCommitted', '3': 16, '4': 1, '5': 5, '10': 'TechnicalFoulsCommitted'},
    const {'1': 'MinutesPlayed', '3': 17, '4': 1, '5': 5, '10': 'MinutesPlayed'},
  ],
};

const PlayerGameStats$json = const {
  '1': 'PlayerGameStats',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
    const {'1': 'GameId', '3': 2, '4': 1, '5': 5, '10': 'GameId'},
    const {'1': 'TeamId', '3': 3, '4': 1, '5': 5, '10': 'TeamId'},
    const {'1': 'Stats', '3': 4, '4': 1, '5': 11, '6': '.pb.Stats', '10': 'Stats'},
  ],
};

const PlayerAverageStats$json = const {
  '1': 'PlayerAverageStats',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
    const {'1': 'GameIds', '3': 2, '4': 3, '5': 5, '10': 'GameIds'},
    const {'1': 'Stats', '3': 4, '4': 1, '5': 11, '6': '.pb.Stats', '10': 'Stats'},
  ],
};

const AverageStatsSelector$json = const {
  '1': 'AverageStatsSelector',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
    const {'1': 'StartDate', '3': 2, '4': 1, '5': 11, '6': '.pb.Date', '10': 'StartDate'},
    const {'1': 'EndDate', '3': 3, '4': 1, '5': 11, '6': '.pb.Date', '10': 'EndDate'},
    const {'1': 'TeamIdFor', '3': 4, '4': 1, '5': 5, '10': 'TeamIdFor'},
    const {'1': 'TeamIdAgainst', '3': 5, '4': 1, '5': 5, '10': 'TeamIdAgainst'},
    const {'1': 'CompetitionId', '3': 6, '4': 1, '5': 5, '10': 'CompetitionId'},
  ],
};

const GameStatsSelector$json = const {
  '1': 'GameStatsSelector',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
    const {'1': 'GameId', '3': 2, '4': 1, '5': 5, '10': 'GameId'},
  ],
};

const Empty$json = const {
  '1': 'Empty',
};

const GetCompetitionsResponse$json = const {
  '1': 'GetCompetitionsResponse',
  '2': const [
    const {'1': 'Competitions', '3': 1, '4': 3, '5': 11, '6': '.pb.Competition', '10': 'Competitions'},
  ],
};

const GetTeamsForCompetitionResponse$json = const {
  '1': 'GetTeamsForCompetitionResponse',
  '2': const [
    const {'1': 'Teams', '3': 1, '4': 3, '5': 11, '6': '.pb.Team', '10': 'Teams'},
  ],
};

const GetGamesForPlayerResponse$json = const {
  '1': 'GetGamesForPlayerResponse',
  '2': const [
    const {'1': 'Games', '3': 1, '4': 3, '5': 11, '6': '.pb.Game', '10': 'Games'},
  ],
};

const GetGamesForTeamResponse$json = const {
  '1': 'GetGamesForTeamResponse',
  '2': const [
    const {'1': 'Games', '3': 1, '4': 3, '5': 11, '6': '.pb.Game', '10': 'Games'},
  ],
};

const GetTeamsForCompetitionRequest$json = const {
  '1': 'GetTeamsForCompetitionRequest',
  '2': const [
    const {'1': 'CompetitionId', '3': 1, '4': 1, '5': 5, '10': 'CompetitionId'},
  ],
};

const GetTeamRequest$json = const {
  '1': 'GetTeamRequest',
  '2': const [
    const {'1': 'TeamId', '3': 1, '4': 1, '5': 5, '10': 'TeamId'},
  ],
};

const GetGamesForTeamRequest$json = const {
  '1': 'GetGamesForTeamRequest',
  '2': const [
    const {'1': 'TeamId', '3': 1, '4': 1, '5': 5, '10': 'TeamId'},
    const {'1': 'CompetitionId', '3': 2, '4': 1, '5': 5, '10': 'CompetitionId'},
  ],
};

const GetPlayerRequest$json = const {
  '1': 'GetPlayerRequest',
  '2': const [
    const {'1': 'PlayerId', '3': 1, '4': 1, '5': 5, '10': 'PlayerId'},
  ],
};

const GetGameRequest$json = const {
  '1': 'GetGameRequest',
  '2': const [
    const {'1': 'GameId', '3': 1, '4': 1, '5': 5, '10': 'GameId'},
  ],
};

const GetGamesForPlayerRequest$json = const {
  '1': 'GetGamesForPlayerRequest',
  '2': const [
    const {'1': 'GamesId', '3': 1, '4': 1, '5': 5, '10': 'GamesId'},
    const {'1': 'TeamId', '3': 2, '4': 1, '5': 5, '10': 'TeamId'},
  ],
};

const GetGamesOnDateRequest$json = const {
  '1': 'GetGamesOnDateRequest',
  '2': const [
    const {'1': 'Date', '3': 1, '4': 1, '5': 11, '6': '.pb.Date', '10': 'Date'},
  ],
};

const GetGamesOnDateResponse$json = const {
  '1': 'GetGamesOnDateResponse',
  '2': const [
    const {'1': 'Games', '3': 1, '4': 3, '5': 11, '6': '.pb.Game', '10': 'Games'},
  ],
};

