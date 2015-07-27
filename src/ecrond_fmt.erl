-module(ecrond_fmt).  

-include("ecrond.hrl").


-export([compile/1]).

-define(ALL, "^ *[*] *$").
-define(NUM, "^ *[0-9]+ *$").
-define(LIST, "^ *[0-9]+ *$").
-define(RANGE, "^ *([0-9]+)-([0-9]+) *$").
-define(ALL_STEP, "^ *[*][/]([0-9]+) *$").
-define(RANGE_STEP, "^ *([0-9]+)-([0-9]+)[/]([0-9]+) *$").

%% list items can be one of belows(other serves as guard)
-define(UNIT_PATTERNS, [{num,        ?NUM},
                        {range,      ?RANGE},
                        {all_step,   ?ALL_STEP},
                        {range_step, ?RANGE_STEP},
                        {other,      other}]).

-define(FIELD_ALL, "^ *[*] *$").
-define(FIELD_NUM, "^ *[0-9]+ *$").
-define(FIELD_LIST, "^ *[-*/0-9]+(,[-*/0-9]+)+ *$").
-define(FIELD_PATTERNS, [{field_all, ?FIELD_ALL},
                         {field_num, ?FIELD_NUM},
                         {field_list, ?FIELD_LIST},
                         {other, other}]).
%% names of months
-define(JAN, "jan").
-define(FEB, "feb").
-define(MAR, "mar").
-define(APR, "apr").
-define(MAY, "may").
-define(JUN, "jun").
-define(JUL, "jul").
-define(AUG, "aug").
-define(SEPT, "sept").
-define(SEP, "sep").
-define(OCT, "oct").
-define(NOV, "nov").
-define(DEC, "dec").

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
-define(MON, "mon").
-define(TUE, "tue").
-define(WED, "wed").
-define(THU, "thu").
-define(FRI, "fri").
-define(SAT, "sat").
-define(SUN, "sun").

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
    Str.

compile_spec(Str, {SpecStart, SpecEnd}) ->
    S = normalize(Str),
    case field_type(S) of
        {field_all, _} ->
            all;
        {field_num, _} ->
            [list_to_integer(S)];
        {field_list, _} ->
            UnitStrs = string:tokens(S, ","),
            Units = lists:foldl(
                      fun(S, Acc) ->
                              case unit_type(S) of
                                  {num, _} ->
                                      [{num, list_to_integer(S)} | Acc];
                                  {range, [_, {S0,S1}, {E0, E1}]} ->
                                      Start = list_to_integer(string:sub_string(S, S0, S1)),
                                      End = list_to_integer(string:sub_string(S, E0, E1)),
                                      [{range, {Start, End}} | Acc];
                                  {all_step, [_, {S0, S1}]} ->
                                      Step = list_to_integer(string:sub_string(S, S0, S1)),
                                      [{all_step, Step} | Acc];
                                  {range_step, [_, {S0, S1},{E0, E1},{Step0, Step1}]} ->
                                      Start = list_to_integer(string:sub_string(S, S0, S1)),
                                      End = list_to_integer(string:sub_string(S, E0, E1)),
                                      Step = list_to_integer(string:sub_string(S, Step0, Step1)),
                                      [{range_step, {Start, End, Step}}];
                                  {other, _} ->
                                      throw({cron_syntax_error, io_lib:format("cron syntax error near '~s'", [Str])})
                              end
                      end, [], UnitStrs),
            merge(Units);
        {other, _} ->
            throw({cron_syntax_error, io_lib:format("cron syntax error near '~s'", [Str])})
    end.

merge(UnitSpecs) ->
    ok.

cmp(A, B) when is_integer(A) , is_integer(B) ->
    A =< B.
sort(L) ->
    lists:usort(fun cmp/2, L).

merge(A, B) when is_list(A), is_list(B) ->
    lists:umerge(fun cmp/2, lists:usort(A), lists:usort(B)).

filter(L, {R0, R1}) when is_list(L) ->
    lists:reverse(lists:foldl(
      fun(I, Acc) when I =< R1, I >= R0 ->
              [I | Acc];
         (_, Acc) ->
              Acc
      end, [], L)).

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
filter_test()->
    ?assertEqual([3,4,5,6,7,8], filter([1,2,3,4,5,6,7,8,9,10,11], {3, 8})).

field_type_test() ->
    ?assertMatch({other, _}, field_type("fei")),
    ?assertMatch({other, _}, field_type("feia,fei,afe23,3-23,232/23,23-24/23")),
    ?assertMatch({field_list, _}, field_type("23,2-53,2-34/2,23-2")),
    ?assertMatch({field_all, _}, field_type("*")),
    ?assertMatch({field_num, _}, field_type("2342")).

unit_type_test()->
    ?assertMatch({other, _}, unit_type("fie23")),
    ?assertMatch({num, _}, unit_type("34 ")),
    ?assertMatch({num, _}, unit_type(" 983")),
    ?assertMatch({range, _}, unit_type("23-34")),
    ?assertMatch({range, _}, unit_type("904-22")),
    ?assertMatch({all_step, _}, unit_type("*/23")),
    ?assertMatch({range_step, _}, unit_type("34-42/34")).
normalize_test()->
    ?assertEqual("1", normalize("jan")),
    ?assertEqual("2", normalize("feb")),
    ?assertEqual("3,4,5,6,7,8,9,9,10,11,12", normalize("mar,apr,may,jun,jul,aug,sept,sep,oct,nov,dec")),
    ?assertEqual("0,1,2,3,4,5,6", normalize("sun,mon,tue,wed,thu,fri,sat")),
    ?assertEqual("1,1,1,4,4", normalize("mon,mon,mon,thu,thu")).

-endif.
