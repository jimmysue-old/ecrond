-module(ecrond_fmt).  
-include("ecrond.hrl").


-export([compile/1]).

-define(ALL, "^ *[*] *$").
-define(NUM, "^ *[0-9]+ *$").
-define(LIST, "^ *[0-9]+ *$").
-define(RANGE, "^ *([0-9]+)-([0-9]+) *$").
-define(ALL_INTERVAL, "^ *[*][/]([0-9]+) *$").
-define(RANGE_INTERVAL, "^ *([0-9]+)-([0-9]+)[/]([0-9]+) *$").


compile(Str) when is_binary(Str)->
    compile(binary_to_list(str));
compile(Str) when is_list(Str) ->
    ok.


-spec compile_cron(Str :: string()) -> cron().
compile_cron(Str) when is_list(Str) ->
    case string:tokens(Str, " ") of
        [Min, Hour, Dom, Month, Dow] ->
            MinSpec = compile_cron_item(Min, {0, 59}),
            ok;
        ->
            throw({})


