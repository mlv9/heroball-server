///
//  Generated code. Do not modify.
//  source: heroball.proto
///
// ignore_for_file: camel_case_types,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name

import 'dart:async' as $async;

import 'package:grpc/service_api.dart' as $grpc;

import 'dart:core' as $core show int, String, List;

import 'heroball.pb.dart';
export 'heroball.pb.dart';

class HeroBallServiceClient extends $grpc.Client {
  static final _$getCompetitions =
      $grpc.ClientMethod<Empty, GetCompetitionsResponse>(
          '/pb.HeroBallService/GetCompetitions',
          (Empty value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              GetCompetitionsResponse.fromBuffer(value));
  static final _$getTeamsForCompetition = $grpc.ClientMethod<
          GetTeamsForCompetitionRequest, GetTeamsForCompetitionResponse>(
      '/pb.HeroBallService/GetTeamsForCompetition',
      (GetTeamsForCompetitionRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          GetTeamsForCompetitionResponse.fromBuffer(value));
  static final _$getTeam = $grpc.ClientMethod<GetTeamRequest, Team>(
      '/pb.HeroBallService/GetTeam',
      (GetTeamRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) => Team.fromBuffer(value));
  static final _$getGamesForTeam =
      $grpc.ClientMethod<GetGamesForTeamRequest, GetGamesForTeamResponse>(
          '/pb.HeroBallService/GetGamesForTeam',
          (GetGamesForTeamRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              GetGamesForTeamResponse.fromBuffer(value));
  static final _$getPlayer = $grpc.ClientMethod<GetPlayerRequest, Player>(
      '/pb.HeroBallService/GetPlayer',
      (GetPlayerRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) => Player.fromBuffer(value));
  static final _$getGamesOnDate =
      $grpc.ClientMethod<GetGamesOnDateRequest, GetGamesOnDateResponse>(
          '/pb.HeroBallService/GetGamesOnDate',
          (GetGamesOnDateRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              GetGamesOnDateResponse.fromBuffer(value));
  static final _$getGame = $grpc.ClientMethod<GetPlayerRequest, Game>(
      '/pb.HeroBallService/GetGame',
      (GetPlayerRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) => Game.fromBuffer(value));
  static final _$getGamesForPlayer =
      $grpc.ClientMethod<GetGamesForPlayerRequest, GetGamesForPlayerResponse>(
          '/pb.HeroBallService/GetGamesForPlayer',
          (GetGamesForPlayerRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              GetGamesForPlayerResponse.fromBuffer(value));
  static final _$getPlayerGameStats =
      $grpc.ClientMethod<GameStatsSelector, PlayerGameStats>(
          '/pb.HeroBallService/GetPlayerGameStats',
          (GameStatsSelector value) => value.writeToBuffer(),
          ($core.List<$core.int> value) => PlayerGameStats.fromBuffer(value));
  static final _$getPlayerAverageStats =
      $grpc.ClientMethod<AverageStatsSelector, PlayerAverageStats>(
          '/pb.HeroBallService/GetPlayerAverageStats',
          (AverageStatsSelector value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              PlayerAverageStats.fromBuffer(value));
  static final _$addPlayerGameStats =
      $grpc.ClientMethod<PlayerGameStats, Empty>(
          '/pb.HeroBallService/AddPlayerGameStats',
          (PlayerGameStats value) => value.writeToBuffer(),
          ($core.List<$core.int> value) => Empty.fromBuffer(value));

  HeroBallServiceClient($grpc.ClientChannel channel,
      {$grpc.CallOptions options})
      : super(channel, options: options);

  $grpc.ResponseFuture<GetCompetitionsResponse> getCompetitions(Empty request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getCompetitions, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<GetTeamsForCompetitionResponse> getTeamsForCompetition(
      GetTeamsForCompetitionRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getTeamsForCompetition, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<Team> getTeam(GetTeamRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(_$getTeam, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<GetGamesForTeamResponse> getGamesForTeam(
      GetGamesForTeamRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getGamesForTeam, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<Player> getPlayer(GetPlayerRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(_$getPlayer, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<GetGamesOnDateResponse> getGamesOnDate(
      GetGamesOnDateRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getGamesOnDate, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<Game> getGame(GetPlayerRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(_$getGame, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<GetGamesForPlayerResponse> getGamesForPlayer(
      GetGamesForPlayerRequest request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getGamesForPlayer, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<PlayerGameStats> getPlayerGameStats(
      GameStatsSelector request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getPlayerGameStats, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<PlayerAverageStats> getPlayerAverageStats(
      AverageStatsSelector request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$getPlayerAverageStats, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }

  $grpc.ResponseFuture<Empty> addPlayerGameStats(PlayerGameStats request,
      {$grpc.CallOptions options}) {
    final call = $createCall(
        _$addPlayerGameStats, $async.Stream.fromIterable([request]),
        options: options);
    return $grpc.ResponseFuture(call);
  }
}

abstract class HeroBallServiceBase extends $grpc.Service {
  $core.String get $name => 'pb.HeroBallService';

  HeroBallServiceBase() {
    $addMethod($grpc.ServiceMethod<Empty, GetCompetitionsResponse>(
        'GetCompetitions',
        getCompetitions_Pre,
        false,
        false,
        ($core.List<$core.int> value) => Empty.fromBuffer(value),
        (GetCompetitionsResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GetTeamsForCompetitionRequest,
            GetTeamsForCompetitionResponse>(
        'GetTeamsForCompetition',
        getTeamsForCompetition_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            GetTeamsForCompetitionRequest.fromBuffer(value),
        (GetTeamsForCompetitionResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GetTeamRequest, Team>(
        'GetTeam',
        getTeam_Pre,
        false,
        false,
        ($core.List<$core.int> value) => GetTeamRequest.fromBuffer(value),
        (Team value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<GetGamesForTeamRequest, GetGamesForTeamResponse>(
            'GetGamesForTeam',
            getGamesForTeam_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                GetGamesForTeamRequest.fromBuffer(value),
            (GetGamesForTeamResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GetPlayerRequest, Player>(
        'GetPlayer',
        getPlayer_Pre,
        false,
        false,
        ($core.List<$core.int> value) => GetPlayerRequest.fromBuffer(value),
        (Player value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<GetGamesOnDateRequest, GetGamesOnDateResponse>(
            'GetGamesOnDate',
            getGamesOnDate_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                GetGamesOnDateRequest.fromBuffer(value),
            (GetGamesOnDateResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GetPlayerRequest, Game>(
        'GetGame',
        getGame_Pre,
        false,
        false,
        ($core.List<$core.int> value) => GetPlayerRequest.fromBuffer(value),
        (Game value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GetGamesForPlayerRequest,
            GetGamesForPlayerResponse>(
        'GetGamesForPlayer',
        getGamesForPlayer_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            GetGamesForPlayerRequest.fromBuffer(value),
        (GetGamesForPlayerResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<GameStatsSelector, PlayerGameStats>(
        'GetPlayerGameStats',
        getPlayerGameStats_Pre,
        false,
        false,
        ($core.List<$core.int> value) => GameStatsSelector.fromBuffer(value),
        (PlayerGameStats value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<AverageStatsSelector, PlayerAverageStats>(
        'GetPlayerAverageStats',
        getPlayerAverageStats_Pre,
        false,
        false,
        ($core.List<$core.int> value) => AverageStatsSelector.fromBuffer(value),
        (PlayerAverageStats value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<PlayerGameStats, Empty>(
        'AddPlayerGameStats',
        addPlayerGameStats_Pre,
        false,
        false,
        ($core.List<$core.int> value) => PlayerGameStats.fromBuffer(value),
        (Empty value) => value.writeToBuffer()));
  }

  $async.Future<GetCompetitionsResponse> getCompetitions_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getCompetitions(call, await request);
  }

  $async.Future<GetTeamsForCompetitionResponse> getTeamsForCompetition_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getTeamsForCompetition(call, await request);
  }

  $async.Future<Team> getTeam_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getTeam(call, await request);
  }

  $async.Future<GetGamesForTeamResponse> getGamesForTeam_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getGamesForTeam(call, await request);
  }

  $async.Future<Player> getPlayer_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getPlayer(call, await request);
  }

  $async.Future<GetGamesOnDateResponse> getGamesOnDate_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getGamesOnDate(call, await request);
  }

  $async.Future<Game> getGame_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getGame(call, await request);
  }

  $async.Future<GetGamesForPlayerResponse> getGamesForPlayer_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getGamesForPlayer(call, await request);
  }

  $async.Future<PlayerGameStats> getPlayerGameStats_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getPlayerGameStats(call, await request);
  }

  $async.Future<PlayerAverageStats> getPlayerAverageStats_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return getPlayerAverageStats(call, await request);
  }

  $async.Future<Empty> addPlayerGameStats_Pre(
      $grpc.ServiceCall call, $async.Future request) async {
    return addPlayerGameStats(call, await request);
  }

  $async.Future<GetCompetitionsResponse> getCompetitions(
      $grpc.ServiceCall call, Empty request);
  $async.Future<GetTeamsForCompetitionResponse> getTeamsForCompetition(
      $grpc.ServiceCall call, GetTeamsForCompetitionRequest request);
  $async.Future<Team> getTeam($grpc.ServiceCall call, GetTeamRequest request);
  $async.Future<GetGamesForTeamResponse> getGamesForTeam(
      $grpc.ServiceCall call, GetGamesForTeamRequest request);
  $async.Future<Player> getPlayer(
      $grpc.ServiceCall call, GetPlayerRequest request);
  $async.Future<GetGamesOnDateResponse> getGamesOnDate(
      $grpc.ServiceCall call, GetGamesOnDateRequest request);
  $async.Future<Game> getGame($grpc.ServiceCall call, GetPlayerRequest request);
  $async.Future<GetGamesForPlayerResponse> getGamesForPlayer(
      $grpc.ServiceCall call, GetGamesForPlayerRequest request);
  $async.Future<PlayerGameStats> getPlayerGameStats(
      $grpc.ServiceCall call, GameStatsSelector request);
  $async.Future<PlayerAverageStats> getPlayerAverageStats(
      $grpc.ServiceCall call, AverageStatsSelector request);
  $async.Future<Empty> addPlayerGameStats(
      $grpc.ServiceCall call, PlayerGameStats request);
}
