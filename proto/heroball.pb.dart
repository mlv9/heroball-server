///
//  Generated code. Do not modify.
//  source: heroball.proto
///
// ignore_for_file: camel_case_types,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name

import 'dart:core' as $core show bool, Deprecated, double, int, List, Map, override, String;

import 'package:fixnum/fixnum.dart';
import 'package:protobuf/protobuf.dart' as $pb;

class Player extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Player', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..aOS(2, 'name')
    ..aOS(3, 'position')
    ..hasRequiredFields = false
  ;

  Player() : super();
  Player.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Player.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Player clone() => Player()..mergeFromMessage(this);
  Player copyWith(void Function(Player) updates) => super.copyWith((message) => updates(message as Player));
  $pb.BuilderInfo get info_ => _i;
  static Player create() => Player();
  Player createEmptyInstance() => create();
  static $pb.PbList<Player> createRepeated() => $pb.PbList<Player>();
  static Player getDefault() => _defaultInstance ??= create()..freeze();
  static Player _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);

  $core.String get name => $_getS(1, '');
  set name($core.String v) { $_setString(1, v); }
  $core.bool hasName() => $_has(1);
  void clearName() => clearField(2);

  $core.String get position => $_getS(2, '');
  set position($core.String v) { $_setString(2, v); }
  $core.bool hasPosition() => $_has(2);
  void clearPosition() => clearField(3);
}

class PlayerStats extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('PlayerStats', package: const $pb.PackageName('pb'))
    ..aOS(2, 'email')
    ..aOS(3, 'password')
    ..aInt64(4, 'createdTimestamp')
    ..hasRequiredFields = false
  ;

  PlayerStats() : super();
  PlayerStats.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  PlayerStats.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  PlayerStats clone() => PlayerStats()..mergeFromMessage(this);
  PlayerStats copyWith(void Function(PlayerStats) updates) => super.copyWith((message) => updates(message as PlayerStats));
  $pb.BuilderInfo get info_ => _i;
  static PlayerStats create() => PlayerStats();
  PlayerStats createEmptyInstance() => create();
  static $pb.PbList<PlayerStats> createRepeated() => $pb.PbList<PlayerStats>();
  static PlayerStats getDefault() => _defaultInstance ??= create()..freeze();
  static PlayerStats _defaultInstance;

  $core.String get email => $_getS(0, '');
  set email($core.String v) { $_setString(0, v); }
  $core.bool hasEmail() => $_has(0);
  void clearEmail() => clearField(2);

  $core.String get password => $_getS(1, '');
  set password($core.String v) { $_setString(1, v); }
  $core.bool hasPassword() => $_has(1);
  void clearPassword() => clearField(3);

  Int64 get createdTimestamp => $_getI64(2);
  set createdTimestamp(Int64 v) { $_setInt64(2, v); }
  $core.bool hasCreatedTimestamp() => $_has(2);
  void clearCreatedTimestamp() => clearField(4);
}

class Competition extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Competition', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'competitionId', $pb.PbFieldType.O3)
    ..aOS(2, 'name')
    ..aOS(3, 'division')
    ..hasRequiredFields = false
  ;

  Competition() : super();
  Competition.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Competition.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Competition clone() => Competition()..mergeFromMessage(this);
  Competition copyWith(void Function(Competition) updates) => super.copyWith((message) => updates(message as Competition));
  $pb.BuilderInfo get info_ => _i;
  static Competition create() => Competition();
  Competition createEmptyInstance() => create();
  static $pb.PbList<Competition> createRepeated() => $pb.PbList<Competition>();
  static Competition getDefault() => _defaultInstance ??= create()..freeze();
  static Competition _defaultInstance;

  $core.int get competitionId => $_get(0, 0);
  set competitionId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasCompetitionId() => $_has(0);
  void clearCompetitionId() => clearField(1);

  $core.String get name => $_getS(1, '');
  set name($core.String v) { $_setString(1, v); }
  $core.bool hasName() => $_has(1);
  void clearName() => clearField(2);

  $core.String get division => $_getS(2, '');
  set division($core.String v) { $_setString(2, v); }
  $core.bool hasDivision() => $_has(2);
  void clearDivision() => clearField(3);
}

class Team extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Team', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'teamId', $pb.PbFieldType.O3)
    ..aOS(2, 'name')
    ..hasRequiredFields = false
  ;

  Team() : super();
  Team.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Team.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Team clone() => Team()..mergeFromMessage(this);
  Team copyWith(void Function(Team) updates) => super.copyWith((message) => updates(message as Team));
  $pb.BuilderInfo get info_ => _i;
  static Team create() => Team();
  Team createEmptyInstance() => create();
  static $pb.PbList<Team> createRepeated() => $pb.PbList<Team>();
  static Team getDefault() => _defaultInstance ??= create()..freeze();
  static Team _defaultInstance;

  $core.int get teamId => $_get(0, 0);
  set teamId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasTeamId() => $_has(0);
  void clearTeamId() => clearField(1);

  $core.String get name => $_getS(1, '');
  set name($core.String v) { $_setString(1, v); }
  $core.bool hasName() => $_has(1);
  void clearName() => clearField(2);
}

class Location extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Location', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'locationId', $pb.PbFieldType.O3)
    ..aOS(2, 'name')
    ..hasRequiredFields = false
  ;

  Location() : super();
  Location.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Location.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Location clone() => Location()..mergeFromMessage(this);
  Location copyWith(void Function(Location) updates) => super.copyWith((message) => updates(message as Location));
  $pb.BuilderInfo get info_ => _i;
  static Location create() => Location();
  Location createEmptyInstance() => create();
  static $pb.PbList<Location> createRepeated() => $pb.PbList<Location>();
  static Location getDefault() => _defaultInstance ??= create()..freeze();
  static Location _defaultInstance;

  $core.int get locationId => $_get(0, 0);
  set locationId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasLocationId() => $_has(0);
  void clearLocationId() => clearField(1);

  $core.String get name => $_getS(1, '');
  set name($core.String v) { $_setString(1, v); }
  $core.bool hasName() => $_has(1);
  void clearName() => clearField(2);
}

class Date extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Date', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'year', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'month', $pb.PbFieldType.O3)
    ..a<$core.int>(3, 'day', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  Date() : super();
  Date.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Date.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Date clone() => Date()..mergeFromMessage(this);
  Date copyWith(void Function(Date) updates) => super.copyWith((message) => updates(message as Date));
  $pb.BuilderInfo get info_ => _i;
  static Date create() => Date();
  Date createEmptyInstance() => create();
  static $pb.PbList<Date> createRepeated() => $pb.PbList<Date>();
  static Date getDefault() => _defaultInstance ??= create()..freeze();
  static Date _defaultInstance;

  $core.int get year => $_get(0, 0);
  set year($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasYear() => $_has(0);
  void clearYear() => clearField(1);

  $core.int get month => $_get(1, 0);
  set month($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasMonth() => $_has(1);
  void clearMonth() => clearField(2);

  $core.int get day => $_get(2, 0);
  set day($core.int v) { $_setSignedInt32(2, v); }
  $core.bool hasDay() => $_has(2);
  void clearDay() => clearField(3);
}

class Time extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Time', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'hour', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'minute', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  Time() : super();
  Time.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Time.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Time clone() => Time()..mergeFromMessage(this);
  Time copyWith(void Function(Time) updates) => super.copyWith((message) => updates(message as Time));
  $pb.BuilderInfo get info_ => _i;
  static Time create() => Time();
  Time createEmptyInstance() => create();
  static $pb.PbList<Time> createRepeated() => $pb.PbList<Time>();
  static Time getDefault() => _defaultInstance ??= create()..freeze();
  static Time _defaultInstance;

  $core.int get hour => $_get(0, 0);
  set hour($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasHour() => $_has(0);
  void clearHour() => clearField(1);

  $core.int get minute => $_get(1, 0);
  set minute($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasMinute() => $_has(1);
  void clearMinute() => clearField(2);
}

class Game extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Game', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'gameId', $pb.PbFieldType.O3)
    ..m<$core.int, PlayerGameStats>(2, 'playerStats', 'Game.PlayerStatsEntry',$pb.PbFieldType.O3, $pb.PbFieldType.OM, PlayerGameStats.create, null, null , const $pb.PackageName('pb'))
    ..a<Team>(3, 'homeTeam', $pb.PbFieldType.OM, Team.getDefault, Team.create)
    ..a<Team>(4, 'awayTeam', $pb.PbFieldType.OM, Team.getDefault, Team.create)
    ..a<Location>(5, 'location', $pb.PbFieldType.OM, Location.getDefault, Location.create)
    ..a<Competition>(6, 'competition', $pb.PbFieldType.OM, Competition.getDefault, Competition.create)
    ..a<Date>(7, 'date', $pb.PbFieldType.OM, Date.getDefault, Date.create)
    ..a<Time>(8, 'time', $pb.PbFieldType.OM, Time.getDefault, Time.create)
    ..hasRequiredFields = false
  ;

  Game() : super();
  Game.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Game.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Game clone() => Game()..mergeFromMessage(this);
  Game copyWith(void Function(Game) updates) => super.copyWith((message) => updates(message as Game));
  $pb.BuilderInfo get info_ => _i;
  static Game create() => Game();
  Game createEmptyInstance() => create();
  static $pb.PbList<Game> createRepeated() => $pb.PbList<Game>();
  static Game getDefault() => _defaultInstance ??= create()..freeze();
  static Game _defaultInstance;

  $core.int get gameId => $_get(0, 0);
  set gameId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasGameId() => $_has(0);
  void clearGameId() => clearField(1);

  $core.Map<$core.int, PlayerGameStats> get playerStats => $_getMap(1);

  Team get homeTeam => $_getN(2);
  set homeTeam(Team v) { setField(3, v); }
  $core.bool hasHomeTeam() => $_has(2);
  void clearHomeTeam() => clearField(3);

  Team get awayTeam => $_getN(3);
  set awayTeam(Team v) { setField(4, v); }
  $core.bool hasAwayTeam() => $_has(3);
  void clearAwayTeam() => clearField(4);

  Location get location => $_getN(4);
  set location(Location v) { setField(5, v); }
  $core.bool hasLocation() => $_has(4);
  void clearLocation() => clearField(5);

  Competition get competition => $_getN(5);
  set competition(Competition v) { setField(6, v); }
  $core.bool hasCompetition() => $_has(5);
  void clearCompetition() => clearField(6);

  Date get date => $_getN(6);
  set date(Date v) { setField(7, v); }
  $core.bool hasDate() => $_has(6);
  void clearDate() => clearField(7);

  Time get time => $_getN(7);
  set time(Time v) { setField(8, v); }
  $core.bool hasTime() => $_has(7);
  void clearTime() => clearField(8);
}

class Stats extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Stats', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'statsId', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'twoPointFGA', $pb.PbFieldType.O3)
    ..a<$core.int>(3, 'twoPointFGM', $pb.PbFieldType.O3)
    ..a<$core.int>(4, 'threePointFMA', $pb.PbFieldType.O3)
    ..a<$core.int>(5, 'threePointFGM', $pb.PbFieldType.O3)
    ..a<$core.int>(6, 'freeThrowsAttempted', $pb.PbFieldType.O3)
    ..a<$core.int>(7, 'freeThrowsMade', $pb.PbFieldType.O3)
    ..a<$core.int>(8, 'offensiveRebounds', $pb.PbFieldType.O3)
    ..a<$core.int>(9, 'defensiveRebounds', $pb.PbFieldType.O3)
    ..a<$core.int>(10, 'assists', $pb.PbFieldType.O3)
    ..a<$core.int>(11, 'blocks', $pb.PbFieldType.O3)
    ..a<$core.int>(12, 'steals', $pb.PbFieldType.O3)
    ..a<$core.int>(13, 'turnovers', $pb.PbFieldType.O3)
    ..a<$core.int>(14, 'regularFoulsForced', $pb.PbFieldType.O3)
    ..a<$core.int>(15, 'regularFoulsCommitted', $pb.PbFieldType.O3)
    ..a<$core.int>(16, 'technicalFoulsCommitted', $pb.PbFieldType.O3)
    ..a<$core.int>(17, 'minutesPlayed', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  Stats() : super();
  Stats.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Stats.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Stats clone() => Stats()..mergeFromMessage(this);
  Stats copyWith(void Function(Stats) updates) => super.copyWith((message) => updates(message as Stats));
  $pb.BuilderInfo get info_ => _i;
  static Stats create() => Stats();
  Stats createEmptyInstance() => create();
  static $pb.PbList<Stats> createRepeated() => $pb.PbList<Stats>();
  static Stats getDefault() => _defaultInstance ??= create()..freeze();
  static Stats _defaultInstance;

  $core.int get statsId => $_get(0, 0);
  set statsId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasStatsId() => $_has(0);
  void clearStatsId() => clearField(1);

  $core.int get twoPointFGA => $_get(1, 0);
  set twoPointFGA($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasTwoPointFGA() => $_has(1);
  void clearTwoPointFGA() => clearField(2);

  $core.int get twoPointFGM => $_get(2, 0);
  set twoPointFGM($core.int v) { $_setSignedInt32(2, v); }
  $core.bool hasTwoPointFGM() => $_has(2);
  void clearTwoPointFGM() => clearField(3);

  $core.int get threePointFMA => $_get(3, 0);
  set threePointFMA($core.int v) { $_setSignedInt32(3, v); }
  $core.bool hasThreePointFMA() => $_has(3);
  void clearThreePointFMA() => clearField(4);

  $core.int get threePointFGM => $_get(4, 0);
  set threePointFGM($core.int v) { $_setSignedInt32(4, v); }
  $core.bool hasThreePointFGM() => $_has(4);
  void clearThreePointFGM() => clearField(5);

  $core.int get freeThrowsAttempted => $_get(5, 0);
  set freeThrowsAttempted($core.int v) { $_setSignedInt32(5, v); }
  $core.bool hasFreeThrowsAttempted() => $_has(5);
  void clearFreeThrowsAttempted() => clearField(6);

  $core.int get freeThrowsMade => $_get(6, 0);
  set freeThrowsMade($core.int v) { $_setSignedInt32(6, v); }
  $core.bool hasFreeThrowsMade() => $_has(6);
  void clearFreeThrowsMade() => clearField(7);

  $core.int get offensiveRebounds => $_get(7, 0);
  set offensiveRebounds($core.int v) { $_setSignedInt32(7, v); }
  $core.bool hasOffensiveRebounds() => $_has(7);
  void clearOffensiveRebounds() => clearField(8);

  $core.int get defensiveRebounds => $_get(8, 0);
  set defensiveRebounds($core.int v) { $_setSignedInt32(8, v); }
  $core.bool hasDefensiveRebounds() => $_has(8);
  void clearDefensiveRebounds() => clearField(9);

  $core.int get assists => $_get(9, 0);
  set assists($core.int v) { $_setSignedInt32(9, v); }
  $core.bool hasAssists() => $_has(9);
  void clearAssists() => clearField(10);

  $core.int get blocks => $_get(10, 0);
  set blocks($core.int v) { $_setSignedInt32(10, v); }
  $core.bool hasBlocks() => $_has(10);
  void clearBlocks() => clearField(11);

  $core.int get steals => $_get(11, 0);
  set steals($core.int v) { $_setSignedInt32(11, v); }
  $core.bool hasSteals() => $_has(11);
  void clearSteals() => clearField(12);

  $core.int get turnovers => $_get(12, 0);
  set turnovers($core.int v) { $_setSignedInt32(12, v); }
  $core.bool hasTurnovers() => $_has(12);
  void clearTurnovers() => clearField(13);

  $core.int get regularFoulsForced => $_get(13, 0);
  set regularFoulsForced($core.int v) { $_setSignedInt32(13, v); }
  $core.bool hasRegularFoulsForced() => $_has(13);
  void clearRegularFoulsForced() => clearField(14);

  $core.int get regularFoulsCommitted => $_get(14, 0);
  set regularFoulsCommitted($core.int v) { $_setSignedInt32(14, v); }
  $core.bool hasRegularFoulsCommitted() => $_has(14);
  void clearRegularFoulsCommitted() => clearField(15);

  $core.int get technicalFoulsCommitted => $_get(15, 0);
  set technicalFoulsCommitted($core.int v) { $_setSignedInt32(15, v); }
  $core.bool hasTechnicalFoulsCommitted() => $_has(15);
  void clearTechnicalFoulsCommitted() => clearField(16);

  $core.int get minutesPlayed => $_get(16, 0);
  set minutesPlayed($core.int v) { $_setSignedInt32(16, v); }
  $core.bool hasMinutesPlayed() => $_has(16);
  void clearMinutesPlayed() => clearField(17);
}

class PlayerGameStats extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('PlayerGameStats', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'gameId', $pb.PbFieldType.O3)
    ..a<$core.int>(3, 'teamId', $pb.PbFieldType.O3)
    ..a<Stats>(4, 'stats', $pb.PbFieldType.OM, Stats.getDefault, Stats.create)
    ..hasRequiredFields = false
  ;

  PlayerGameStats() : super();
  PlayerGameStats.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  PlayerGameStats.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  PlayerGameStats clone() => PlayerGameStats()..mergeFromMessage(this);
  PlayerGameStats copyWith(void Function(PlayerGameStats) updates) => super.copyWith((message) => updates(message as PlayerGameStats));
  $pb.BuilderInfo get info_ => _i;
  static PlayerGameStats create() => PlayerGameStats();
  PlayerGameStats createEmptyInstance() => create();
  static $pb.PbList<PlayerGameStats> createRepeated() => $pb.PbList<PlayerGameStats>();
  static PlayerGameStats getDefault() => _defaultInstance ??= create()..freeze();
  static PlayerGameStats _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);

  $core.int get gameId => $_get(1, 0);
  set gameId($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasGameId() => $_has(1);
  void clearGameId() => clearField(2);

  $core.int get teamId => $_get(2, 0);
  set teamId($core.int v) { $_setSignedInt32(2, v); }
  $core.bool hasTeamId() => $_has(2);
  void clearTeamId() => clearField(3);

  Stats get stats => $_getN(3);
  set stats(Stats v) { setField(4, v); }
  $core.bool hasStats() => $_has(3);
  void clearStats() => clearField(4);
}

class PlayerAverageStats extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('PlayerAverageStats', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..p<$core.int>(2, 'gameIds', $pb.PbFieldType.P3)
    ..a<Stats>(4, 'stats', $pb.PbFieldType.OM, Stats.getDefault, Stats.create)
    ..hasRequiredFields = false
  ;

  PlayerAverageStats() : super();
  PlayerAverageStats.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  PlayerAverageStats.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  PlayerAverageStats clone() => PlayerAverageStats()..mergeFromMessage(this);
  PlayerAverageStats copyWith(void Function(PlayerAverageStats) updates) => super.copyWith((message) => updates(message as PlayerAverageStats));
  $pb.BuilderInfo get info_ => _i;
  static PlayerAverageStats create() => PlayerAverageStats();
  PlayerAverageStats createEmptyInstance() => create();
  static $pb.PbList<PlayerAverageStats> createRepeated() => $pb.PbList<PlayerAverageStats>();
  static PlayerAverageStats getDefault() => _defaultInstance ??= create()..freeze();
  static PlayerAverageStats _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);

  $core.List<$core.int> get gameIds => $_getList(1);

  Stats get stats => $_getN(2);
  set stats(Stats v) { setField(4, v); }
  $core.bool hasStats() => $_has(2);
  void clearStats() => clearField(4);
}

class AverageStatsSelector extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('AverageStatsSelector', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..a<Date>(2, 'startDate', $pb.PbFieldType.OM, Date.getDefault, Date.create)
    ..a<Date>(3, 'endDate', $pb.PbFieldType.OM, Date.getDefault, Date.create)
    ..a<$core.int>(4, 'teamIdFor', $pb.PbFieldType.O3)
    ..a<$core.int>(5, 'teamIdAgainst', $pb.PbFieldType.O3)
    ..a<$core.int>(6, 'competitionId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  AverageStatsSelector() : super();
  AverageStatsSelector.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  AverageStatsSelector.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  AverageStatsSelector clone() => AverageStatsSelector()..mergeFromMessage(this);
  AverageStatsSelector copyWith(void Function(AverageStatsSelector) updates) => super.copyWith((message) => updates(message as AverageStatsSelector));
  $pb.BuilderInfo get info_ => _i;
  static AverageStatsSelector create() => AverageStatsSelector();
  AverageStatsSelector createEmptyInstance() => create();
  static $pb.PbList<AverageStatsSelector> createRepeated() => $pb.PbList<AverageStatsSelector>();
  static AverageStatsSelector getDefault() => _defaultInstance ??= create()..freeze();
  static AverageStatsSelector _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);

  Date get startDate => $_getN(1);
  set startDate(Date v) { setField(2, v); }
  $core.bool hasStartDate() => $_has(1);
  void clearStartDate() => clearField(2);

  Date get endDate => $_getN(2);
  set endDate(Date v) { setField(3, v); }
  $core.bool hasEndDate() => $_has(2);
  void clearEndDate() => clearField(3);

  $core.int get teamIdFor => $_get(3, 0);
  set teamIdFor($core.int v) { $_setSignedInt32(3, v); }
  $core.bool hasTeamIdFor() => $_has(3);
  void clearTeamIdFor() => clearField(4);

  $core.int get teamIdAgainst => $_get(4, 0);
  set teamIdAgainst($core.int v) { $_setSignedInt32(4, v); }
  $core.bool hasTeamIdAgainst() => $_has(4);
  void clearTeamIdAgainst() => clearField(5);

  $core.int get competitionId => $_get(5, 0);
  set competitionId($core.int v) { $_setSignedInt32(5, v); }
  $core.bool hasCompetitionId() => $_has(5);
  void clearCompetitionId() => clearField(6);
}

class GameStatsSelector extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GameStatsSelector', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'gameId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GameStatsSelector() : super();
  GameStatsSelector.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GameStatsSelector.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GameStatsSelector clone() => GameStatsSelector()..mergeFromMessage(this);
  GameStatsSelector copyWith(void Function(GameStatsSelector) updates) => super.copyWith((message) => updates(message as GameStatsSelector));
  $pb.BuilderInfo get info_ => _i;
  static GameStatsSelector create() => GameStatsSelector();
  GameStatsSelector createEmptyInstance() => create();
  static $pb.PbList<GameStatsSelector> createRepeated() => $pb.PbList<GameStatsSelector>();
  static GameStatsSelector getDefault() => _defaultInstance ??= create()..freeze();
  static GameStatsSelector _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);

  $core.int get gameId => $_get(1, 0);
  set gameId($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasGameId() => $_has(1);
  void clearGameId() => clearField(2);
}

class Empty extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('Empty', package: const $pb.PackageName('pb'))
    ..hasRequiredFields = false
  ;

  Empty() : super();
  Empty.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  Empty.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  Empty clone() => Empty()..mergeFromMessage(this);
  Empty copyWith(void Function(Empty) updates) => super.copyWith((message) => updates(message as Empty));
  $pb.BuilderInfo get info_ => _i;
  static Empty create() => Empty();
  Empty createEmptyInstance() => create();
  static $pb.PbList<Empty> createRepeated() => $pb.PbList<Empty>();
  static Empty getDefault() => _defaultInstance ??= create()..freeze();
  static Empty _defaultInstance;
}

class GetCompetitionsResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetCompetitionsResponse', package: const $pb.PackageName('pb'))
    ..pc<Competition>(1, 'competitions', $pb.PbFieldType.PM,Competition.create)
    ..hasRequiredFields = false
  ;

  GetCompetitionsResponse() : super();
  GetCompetitionsResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetCompetitionsResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetCompetitionsResponse clone() => GetCompetitionsResponse()..mergeFromMessage(this);
  GetCompetitionsResponse copyWith(void Function(GetCompetitionsResponse) updates) => super.copyWith((message) => updates(message as GetCompetitionsResponse));
  $pb.BuilderInfo get info_ => _i;
  static GetCompetitionsResponse create() => GetCompetitionsResponse();
  GetCompetitionsResponse createEmptyInstance() => create();
  static $pb.PbList<GetCompetitionsResponse> createRepeated() => $pb.PbList<GetCompetitionsResponse>();
  static GetCompetitionsResponse getDefault() => _defaultInstance ??= create()..freeze();
  static GetCompetitionsResponse _defaultInstance;

  $core.List<Competition> get competitions => $_getList(0);
}

class GetTeamsForCompetitionResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetTeamsForCompetitionResponse', package: const $pb.PackageName('pb'))
    ..pc<Team>(1, 'teams', $pb.PbFieldType.PM,Team.create)
    ..hasRequiredFields = false
  ;

  GetTeamsForCompetitionResponse() : super();
  GetTeamsForCompetitionResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetTeamsForCompetitionResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetTeamsForCompetitionResponse clone() => GetTeamsForCompetitionResponse()..mergeFromMessage(this);
  GetTeamsForCompetitionResponse copyWith(void Function(GetTeamsForCompetitionResponse) updates) => super.copyWith((message) => updates(message as GetTeamsForCompetitionResponse));
  $pb.BuilderInfo get info_ => _i;
  static GetTeamsForCompetitionResponse create() => GetTeamsForCompetitionResponse();
  GetTeamsForCompetitionResponse createEmptyInstance() => create();
  static $pb.PbList<GetTeamsForCompetitionResponse> createRepeated() => $pb.PbList<GetTeamsForCompetitionResponse>();
  static GetTeamsForCompetitionResponse getDefault() => _defaultInstance ??= create()..freeze();
  static GetTeamsForCompetitionResponse _defaultInstance;

  $core.List<Team> get teams => $_getList(0);
}

class GetGamesForPlayerResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesForPlayerResponse', package: const $pb.PackageName('pb'))
    ..pc<Game>(1, 'games', $pb.PbFieldType.PM,Game.create)
    ..hasRequiredFields = false
  ;

  GetGamesForPlayerResponse() : super();
  GetGamesForPlayerResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesForPlayerResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesForPlayerResponse clone() => GetGamesForPlayerResponse()..mergeFromMessage(this);
  GetGamesForPlayerResponse copyWith(void Function(GetGamesForPlayerResponse) updates) => super.copyWith((message) => updates(message as GetGamesForPlayerResponse));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesForPlayerResponse create() => GetGamesForPlayerResponse();
  GetGamesForPlayerResponse createEmptyInstance() => create();
  static $pb.PbList<GetGamesForPlayerResponse> createRepeated() => $pb.PbList<GetGamesForPlayerResponse>();
  static GetGamesForPlayerResponse getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesForPlayerResponse _defaultInstance;

  $core.List<Game> get games => $_getList(0);
}

class GetGamesForTeamResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesForTeamResponse', package: const $pb.PackageName('pb'))
    ..pc<Game>(1, 'games', $pb.PbFieldType.PM,Game.create)
    ..hasRequiredFields = false
  ;

  GetGamesForTeamResponse() : super();
  GetGamesForTeamResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesForTeamResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesForTeamResponse clone() => GetGamesForTeamResponse()..mergeFromMessage(this);
  GetGamesForTeamResponse copyWith(void Function(GetGamesForTeamResponse) updates) => super.copyWith((message) => updates(message as GetGamesForTeamResponse));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesForTeamResponse create() => GetGamesForTeamResponse();
  GetGamesForTeamResponse createEmptyInstance() => create();
  static $pb.PbList<GetGamesForTeamResponse> createRepeated() => $pb.PbList<GetGamesForTeamResponse>();
  static GetGamesForTeamResponse getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesForTeamResponse _defaultInstance;

  $core.List<Game> get games => $_getList(0);
}

class GetTeamsForCompetitionRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetTeamsForCompetitionRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'competitionId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetTeamsForCompetitionRequest() : super();
  GetTeamsForCompetitionRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetTeamsForCompetitionRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetTeamsForCompetitionRequest clone() => GetTeamsForCompetitionRequest()..mergeFromMessage(this);
  GetTeamsForCompetitionRequest copyWith(void Function(GetTeamsForCompetitionRequest) updates) => super.copyWith((message) => updates(message as GetTeamsForCompetitionRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetTeamsForCompetitionRequest create() => GetTeamsForCompetitionRequest();
  GetTeamsForCompetitionRequest createEmptyInstance() => create();
  static $pb.PbList<GetTeamsForCompetitionRequest> createRepeated() => $pb.PbList<GetTeamsForCompetitionRequest>();
  static GetTeamsForCompetitionRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetTeamsForCompetitionRequest _defaultInstance;

  $core.int get competitionId => $_get(0, 0);
  set competitionId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasCompetitionId() => $_has(0);
  void clearCompetitionId() => clearField(1);
}

class GetTeamRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetTeamRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'teamId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetTeamRequest() : super();
  GetTeamRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetTeamRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetTeamRequest clone() => GetTeamRequest()..mergeFromMessage(this);
  GetTeamRequest copyWith(void Function(GetTeamRequest) updates) => super.copyWith((message) => updates(message as GetTeamRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetTeamRequest create() => GetTeamRequest();
  GetTeamRequest createEmptyInstance() => create();
  static $pb.PbList<GetTeamRequest> createRepeated() => $pb.PbList<GetTeamRequest>();
  static GetTeamRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetTeamRequest _defaultInstance;

  $core.int get teamId => $_get(0, 0);
  set teamId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasTeamId() => $_has(0);
  void clearTeamId() => clearField(1);
}

class GetGamesForTeamRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesForTeamRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'teamId', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'competitionId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetGamesForTeamRequest() : super();
  GetGamesForTeamRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesForTeamRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesForTeamRequest clone() => GetGamesForTeamRequest()..mergeFromMessage(this);
  GetGamesForTeamRequest copyWith(void Function(GetGamesForTeamRequest) updates) => super.copyWith((message) => updates(message as GetGamesForTeamRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesForTeamRequest create() => GetGamesForTeamRequest();
  GetGamesForTeamRequest createEmptyInstance() => create();
  static $pb.PbList<GetGamesForTeamRequest> createRepeated() => $pb.PbList<GetGamesForTeamRequest>();
  static GetGamesForTeamRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesForTeamRequest _defaultInstance;

  $core.int get teamId => $_get(0, 0);
  set teamId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasTeamId() => $_has(0);
  void clearTeamId() => clearField(1);

  $core.int get competitionId => $_get(1, 0);
  set competitionId($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasCompetitionId() => $_has(1);
  void clearCompetitionId() => clearField(2);
}

class GetPlayerRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetPlayerRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'playerId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetPlayerRequest() : super();
  GetPlayerRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetPlayerRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetPlayerRequest clone() => GetPlayerRequest()..mergeFromMessage(this);
  GetPlayerRequest copyWith(void Function(GetPlayerRequest) updates) => super.copyWith((message) => updates(message as GetPlayerRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetPlayerRequest create() => GetPlayerRequest();
  GetPlayerRequest createEmptyInstance() => create();
  static $pb.PbList<GetPlayerRequest> createRepeated() => $pb.PbList<GetPlayerRequest>();
  static GetPlayerRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetPlayerRequest _defaultInstance;

  $core.int get playerId => $_get(0, 0);
  set playerId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasPlayerId() => $_has(0);
  void clearPlayerId() => clearField(1);
}

class GetGameRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGameRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'gameId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetGameRequest() : super();
  GetGameRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGameRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGameRequest clone() => GetGameRequest()..mergeFromMessage(this);
  GetGameRequest copyWith(void Function(GetGameRequest) updates) => super.copyWith((message) => updates(message as GetGameRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetGameRequest create() => GetGameRequest();
  GetGameRequest createEmptyInstance() => create();
  static $pb.PbList<GetGameRequest> createRepeated() => $pb.PbList<GetGameRequest>();
  static GetGameRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetGameRequest _defaultInstance;

  $core.int get gameId => $_get(0, 0);
  set gameId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasGameId() => $_has(0);
  void clearGameId() => clearField(1);
}

class GetGamesForPlayerRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesForPlayerRequest', package: const $pb.PackageName('pb'))
    ..a<$core.int>(1, 'gamesId', $pb.PbFieldType.O3)
    ..a<$core.int>(2, 'teamId', $pb.PbFieldType.O3)
    ..hasRequiredFields = false
  ;

  GetGamesForPlayerRequest() : super();
  GetGamesForPlayerRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesForPlayerRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesForPlayerRequest clone() => GetGamesForPlayerRequest()..mergeFromMessage(this);
  GetGamesForPlayerRequest copyWith(void Function(GetGamesForPlayerRequest) updates) => super.copyWith((message) => updates(message as GetGamesForPlayerRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesForPlayerRequest create() => GetGamesForPlayerRequest();
  GetGamesForPlayerRequest createEmptyInstance() => create();
  static $pb.PbList<GetGamesForPlayerRequest> createRepeated() => $pb.PbList<GetGamesForPlayerRequest>();
  static GetGamesForPlayerRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesForPlayerRequest _defaultInstance;

  $core.int get gamesId => $_get(0, 0);
  set gamesId($core.int v) { $_setSignedInt32(0, v); }
  $core.bool hasGamesId() => $_has(0);
  void clearGamesId() => clearField(1);

  $core.int get teamId => $_get(1, 0);
  set teamId($core.int v) { $_setSignedInt32(1, v); }
  $core.bool hasTeamId() => $_has(1);
  void clearTeamId() => clearField(2);
}

class GetGamesOnDateRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesOnDateRequest', package: const $pb.PackageName('pb'))
    ..a<Date>(1, 'date', $pb.PbFieldType.OM, Date.getDefault, Date.create)
    ..hasRequiredFields = false
  ;

  GetGamesOnDateRequest() : super();
  GetGamesOnDateRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesOnDateRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesOnDateRequest clone() => GetGamesOnDateRequest()..mergeFromMessage(this);
  GetGamesOnDateRequest copyWith(void Function(GetGamesOnDateRequest) updates) => super.copyWith((message) => updates(message as GetGamesOnDateRequest));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesOnDateRequest create() => GetGamesOnDateRequest();
  GetGamesOnDateRequest createEmptyInstance() => create();
  static $pb.PbList<GetGamesOnDateRequest> createRepeated() => $pb.PbList<GetGamesOnDateRequest>();
  static GetGamesOnDateRequest getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesOnDateRequest _defaultInstance;

  Date get date => $_getN(0);
  set date(Date v) { setField(1, v); }
  $core.bool hasDate() => $_has(0);
  void clearDate() => clearField(1);
}

class GetGamesOnDateResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo('GetGamesOnDateResponse', package: const $pb.PackageName('pb'))
    ..pc<Game>(1, 'games', $pb.PbFieldType.PM,Game.create)
    ..hasRequiredFields = false
  ;

  GetGamesOnDateResponse() : super();
  GetGamesOnDateResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromBuffer(i, r);
  GetGamesOnDateResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) : super.fromJson(i, r);
  GetGamesOnDateResponse clone() => GetGamesOnDateResponse()..mergeFromMessage(this);
  GetGamesOnDateResponse copyWith(void Function(GetGamesOnDateResponse) updates) => super.copyWith((message) => updates(message as GetGamesOnDateResponse));
  $pb.BuilderInfo get info_ => _i;
  static GetGamesOnDateResponse create() => GetGamesOnDateResponse();
  GetGamesOnDateResponse createEmptyInstance() => create();
  static $pb.PbList<GetGamesOnDateResponse> createRepeated() => $pb.PbList<GetGamesOnDateResponse>();
  static GetGamesOnDateResponse getDefault() => _defaultInstance ??= create()..freeze();
  static GetGamesOnDateResponse _defaultInstance;

  $core.List<Game> get games => $_getList(0);
}

