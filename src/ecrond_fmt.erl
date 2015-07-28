-module(ecrond_fmt).  

-export([compile/1]).

-define(ERR_SYNTAX, cron_syntax_error).
-define(ERR_INVALID, cron_invalid_field).

-define(ALL, "^[*]$").
-define(NUM, "^[0-9]+$").
-define(LIST, "^[0-9]+$").
-define(RANGE, "^([0-9]+)-([0-9]+)$").
-define(ALL_STEP, "^[*][/]([0-9]+)$").
-define(RANGE_STEP, "^([0-9]+)-([0-9]+)[/]([0-9]+)$").

%% list items can be one of belows(other serves as guard)
-define(UNIT_PATTERNS, [{num,        ?NUM},
                        {range,      ?RANGE},
                        {all_step,   ?ALL_STEP},
                        {range_step, ?RANGE_STEP},
                        {other,      other}]).

-define(FIELD_ALL, "^ *[*] *$").
-define(FIELD_NUM, "^ *[0-9]+ *$").
-define(FIELD_LIST, "^ *[-*/0-9]+(,[-*/0-9]+)* *$").
-define(FIELD_PATTERNS, [{field_all, ?FIELD_ALL},
                         {field_num, ?FIELD_NUM},
                         {field_list, ?FIELD_LIST},
                         {other, other}]).
%% names of months
-define(JAN, "\\bjan\\b").
-define(FEB, "\\bfeb\\b").
-define(MAR, "\\bmar\\b").
-define(APR, "\\bapr\\b").
-define(MAY, "\\bmay\\b").
-define(JUN, "\\bjun\\b").
-define(JUL, "\\bjul\\b").
-define(AUG, "\\baug\\b").
-define(SEPT, "\\bsept\\b").
-define(SEP, "\\bsep\\b").
-define(OCT, "\\boct\\b").
-define(NOV, "\\bnov\\b").
-define(DEC, "\\bdec\\b").

-define(MONTH_MAPS, [{?JAN, "1"}, 
                     {?FEB, "2"},
                     {?MAR, "3"},
                     {?APR, "4"},
                     {?MAY, "5"},
                     {?JUN, "6"},
                     {?JUL, "7"},
                     {?AUG, "8"},
                     {?SEPT, "9"},
                     {?SEP, "9"},
                     {?OCT, "10"},
                     {?NOV, "11"},
                     {?DEC, "12"}]).

%% names of days of week
-define(MON, "\\bmon\\b").
-define(TUE, "\\btue\\b").
-define(WED, "\\bwed\\b").
-define(THU, "\\bthu\\b").
-define(FRI, "\\bfri\\b").
-define(SAT, "\\bsat\\b").
-define(SUN, "\\bsun\\b").

-define(WEEK_MAPS, [{?MON, "1"},
                    {?TUE, "2"},
                    {?WED, "3"},
                    {?THU, "4"},
                    {?FRI, "5"},
                    {?SAT, "6"},
                    {?SUN, "0"}]).



compile(Str) when is_binary(Str)->
    compile(binary_to_list(Str));
compile(Str) when is_list(Str) ->
    case string:tokens(Str, " \t") of
        [Min, Hour, Dom, Month, Dow] ->
            {cron, {
               compile_spec(Min, {0, 59}),
               compile_spec(Hour, {0, 23}),
               compile_spec(Dom, {1, 31}),
               compile_spec(normalize(Month), {1, 12}),
               compile_spec(normalize(Dow), {0, 7})
              }};
        _ ->
            throw({?ERR_SYNTAX, "expected 5 fields"})
    end.


compile_spec(Str, {SpecStart, SpecEnd}) when is_list(Str), is_integer(SpecStart), 
                                             is_integer(SpecEnd), (SpecStart =< SpecEnd) ->
    case field_type(Str) of
        {field_all, _} ->
            all;
        {field_num, _} ->
            case list_to_integer(Str) of
                I when I >= SpecStart, I =< SpecEnd ->
                    [I];
                _ ->
                    throw({?ERR_INVALID, io_lib:format("cron invalid field: '~s'", [Str])})
            end;
        {field_list, _} ->
            UnitStrs = string:tokens(Str, ","),
            Units = lists:foldl(
                      fun(U, Acc) ->
                              case unit_type(U) of
                                  {num, _} ->
                                      [{num, list_to_integer(U)} | Acc];
                                  {range, [_, {S0, S1}, {E0, E1}]} ->
                                      Start = list_to_integer(string:substr(U, S0 + 1, S1)),
                                      End = list_to_integer(string:substr(U, (E0 + 1), E1)),
                                      [{range, {Start, End}} | Acc];
                                  {all_step, [_, {S0, S1}]} ->
                                      Step = list_to_integer(string:substr(U, S0 + 1, S1)),
                                      [{all_step, Step} | Acc];
                                  {range_step, [_, {S0, S1},{E0, E1},{Step0, Step1}]} ->
                                      Start = list_to_integer(string:substr(U, S0 + 1, S1)),
                                      End = list_to_integer(string:substr(U, E0 + 1, E1)),
                                      Step = list_to_integer(string:substr(U, Step0 + 1, Step1)),
                                      [{range_step, {Start, End, Step}} | Acc];
                                  {other, _} ->
                                      throw({?ERR_SYNTAX, io_lib:format("cron syntax error near '~s'", [Str])})
                              end
                      end, [], UnitStrs),
            io:format("Units: ~p~n", [Units]),
            case union(Units, {SpecStart, SpecEnd}) of
                [] ->
                    throw({?ERR_INVALID, io_lib:format("invalid field: '~s'", [Str])});
                Res ->
                    Res
            end;
        {other, _} ->
            throw({?ERR_SYNTAX, io_lib:format("cron syntax error near '~s'", [Str])})
    end.

union(UnitSpecs, {R1, R2}) when R1 =< R2 ->
    lists:foldl(
      fun(U, Acc)->
              case U of
                  {num, Num} when Num =< R2 andalso Num >= R1 ->
                      merge([Num], Acc);
                  {range, {S0, E0}} when S0 =< E0 ->
                      S1 = case S0 < R1 of
                               true ->
                                   R1;
                               false ->
                                   S0
                           end,
                      E1 = case E0 > R2 of
                               true ->
                                   R2;
                               false ->
                                   E0
                           end,
                      case S1 > E1 of
                          true ->
                              Acc;
                          false ->
                              merge(lists:seq(S1, E1), Acc)
                      end;
                  {all_step, Step} ->
                      merge(lists:seq(R1, R2, Step), Acc);
                  {range_step, {S0, E0, Step}} when S0 =< E0 ->
                      S1 = case S0 < R1 of
                               true ->
                                   R1;
                               false ->
                                   S0
                                   end,
                      E1 = case E0 > R2 of
                               true ->
                                   R2;
                               false ->
                                   E0
                                   end,
                      case S1 > E1 of
                          true ->
                              Acc;
                          false ->
                              merge(lists:seq(S1, E1, Step), Acc)
                      end;
                  _->
                      Acc
              end
      end, [], UnitSpecs).

cmp(A, B) when is_integer(A) , is_integer(B) ->
    A =< B.
sort(L) ->
    lists:usort(fun cmp/2, L).

merge(A, B) when is_list(A), is_list(B) ->
    lists:umerge(fun cmp/2, sort(A), sort(B)).


normalize(S0) ->
    S1 = string:to_lower(S0),
    lists:foldl(fun({Pat, Val}, Acc) ->
                        re:replace(Acc, Pat, Val,[global,{return, list}])
                     end, S1, ?MONTH_MAPS ++ ?WEEK_MAPS).
field_type(S) ->
    get_type(S, ?FIELD_PATTERNS).
unit_type(S) ->
    get_type(S, ?UNIT_PATTERNS).

get_type(S, Patterns)->
    try
        lists:foreach(
          fun({other, other})->
                  throw({return, {other, []}});
             ({Type, Pattern}) ->
                  case re:run(S, Pattern) of
                      {match, Res} ->
                          throw({return, {Type, Res}});
                      _->
                          ok
                  end
          end,Patterns)
    catch
        throw:{return, T}->
            T
    end.



-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

compile_test() ->
    ?assertException(throw, {?ERR_SYNTAX, _}, compile( "fei aieja 2323 aiefe")),
    ?assertException(throw, {?ERR_INVALID, _}, compile( "90 * * * *")),
    ?assertException(throw, {?ERR_SYNTAX, _}, compile( "* * 8 * ")),
    ?assertException(throw, {?ERR_SYNTAX, _}, compile( "* * jan tue")),
    ?assertException(throw, {?ERR_SYNTAX, _}, compile("* * * 1jan sun2")),
    ?assertEqual({cron, {all, [0,1,2,3,4,5,6,7,8,12,16,20], [1,5,9,13,17,21,25,29],all, [1,2,3,4,5]}}, compile("* 8-23/4,0-8 */4 * mon-fri")).

compile_spec_test() ->
    ?assertError(function_clause, compile_spec(<<"sfeief">>, {1,2})),
    ?assertError(function_clause, compile_spec("fiejfaejfie", {90, 1})),
    ?assertException(throw, {?ERR_SYNTAX, _}, compile_spec("feiaie ifja", {0, 59})),
    ?assertException(throw, {?ERR_SYNTAX, _}, compile_spec("* * 23* * *", {0,59})),
    ?assertException(throw, {?ERR_INVALID, _}, compile_spec("90", {0, 59})),
    ?assertException(throw, {?ERR_INVALID, _}, compile_spec("13-25", {1, 12})),
    ?assertEqual([1,2,3,4,5,6,7,8,9,10], compile_spec("1-10", {0, 59})),
    ?assertEqual(all, compile_spec("*", {0, 59})),
    ?assertEqual([1,2,3, 11, 21, 31, 40,42, 44, 46, 57, 58, 59], compile_spec("1-3,1-31/10,40-47/2,57-70", {0,59})),
    ?assertEqual([1,4,7,10], compile_spec("*/3", {1,12})).

union_test() ->
    ?assertEqual([], union([], {1,2})),
    ?assertError(function_clause, union([{num, 3}], {90,1})),
    ?assertEqual([1], union([{num, 1}], {0, 5})),
    ?assertEqual([], union([{range, {1, 30}}], {40, 50})),
    ?assertEqual([], union([{range_step, {1, 30, 3}}], {100, 200})),
    ?assertEqual([1,11,21,31,41,51], union([{all_step, 10}], {1, 60})),
    ?assertEqual([1,2,3,4,5,6,11,15,21,31,41,51], 
                 union([{all_step, 10}, {range, {1, 6}}, {num, 15}, {range_step, {11, 31, 10}}], {1, 60})).

merge_test() ->
    ?assertEqual([1,2,3,6,7,9], merge([3,6,7,9], [1,6,3,9,2])).


field_type_test() ->
    ?assertMatch({other, _}, field_type("fei")),
    ?assertMatch({other, _}, field_type("feia,fei,afe23,3-23,232/23,23-24/23")),
    ?assertMatch({field_list, _}, field_type("23,2-53,2-34/2,23-2")),
    ?assertMatch({field_all, _}, field_type("*")),
    ?assertMatch({field_num, _}, field_type("2342")).

unit_type_test()->
    ?assertMatch({other, _}, unit_type("fie23")),
    ?assertMatch({num, _}, unit_type("34")),
    ?assertMatch({num, _}, unit_type("983")),
    ?assertMatch({range, _}, unit_type("23-34")),
    ?assertMatch({range, _}, unit_type("904-22")),
    ?assertMatch({all_step, _}, unit_type("*/23")),
    ?assertMatch({range_step, _}, unit_type("34-42/34")),
    ?assertEqual({range, [{0, 5}, {0,2}, {3, 2}]}, unit_type("13-24")).
normalize_test()->
    ?assertEqual("1", normalize("jan")),
    ?assertEqual("2", normalize("feb")),
    ?assertEqual("3,4,5,6,7,8,9,9,10,11,12", normalize("mar,apr,may,jun,jul,aug,sept,sep,oct,nov,dec")),
    ?assertEqual("0,1,2,3,4,5,6", normalize("sun,mon,tue,wed,thu,fri,sat")),
    ?assertEqual("1,1,1,4,4", normalize("mon,mon,mon,thu,thu")),
    ?assertEqual("1,jan2,2", normalize("jan,jan2,tue")).

-endif.
