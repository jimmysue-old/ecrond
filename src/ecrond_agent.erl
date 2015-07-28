-module(ecrond_agent).

-behaviour(gen_server).

%% API functions
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(TIMER_MAX, 4294967295).
-define(MINUTE_IN_SECONDS, 60).
-define(HOUR_IN_SECONDS, 60 * 60).
-define(DAY_IN_SECONDS, 24 * 60 * 60).

-record(state, {
          name :: term(),
          schedule :: term(),
          task :: mfa(),
          type :: cron | once | interval,
          state :: waiting | running | done | error,
          processor :: pid()
         }).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(Name, Schedule, MFA) ->
    gen_server:start_link(?MODULE, [Name, Schedule, MFA], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, {Type, _} = Schedule, Task]) when is_atom(Type) ->
    process_flag(trap_exit, true),
    set_trigger(Schedule),
    {ok, #state{name = Name, 
                schedule = Schedule, 
                task = Task, 
                state = waiting, 
                type = Type}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(trigger, #state{task = Task} = State) ->
    case Task of
        {Node, Module, Fun, Args} ->
            {noreply, State#state{state = running, processor = spawn_link(Node, Module, Fun, Args)}};
        {Module, Fun, Args} ->
            {noreply, State#state{state = running, processor = spawn_link(Module, Fun, Args)}};
        {Node, Fun} ->
            {noreply, State#state{state = running, processor = spawn_link(Node, Fun)}};
        {Fun} ->
            {noreply, State#state{state = running, processor = spawn_link(Fun)}};
        Fun ->
            {noreply, State#state{state = running, processor = spawn_link(Fun)}}
    end;
handle_info({left, Time}, #state{type = Type, schedule = Schedule} = State) ->
    case Type =:= interval orelse Type =:= once of
        true ->
            send_self_after(Time);
        false ->
            set_trigger(Schedule)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
set_trigger({once, MilliSeconds}) ->
    send_self_after(MilliSeconds);
set_trigger({interval, MilliSeconds}) ->
    send_self_after(MilliSeconds);
set_trigger({cron,Schedule})->
    CurrentDateTime = calendar:universal_time(),
    NextValidDateTime = next_valid_datetime(Schedule, CurrentDateTime),
    SleepFor = time_to_wait_millis(CurrentDateTime, NextValidDateTime),
    send_self_after(SleepFor).
send_self_after(MilliSeconds) ->
    if
        MilliSeconds > ?TIMER_MAX ->
            erlang:send_after(?TIMER_MAX, self(), {left, MilliSeconds - ?TIMER_MAX});
        true ->
            erlang:send_after(MilliSeconds, self(), trigger)
    end.



next_valid_datetime({cron, Schedule}, DateTime) ->
    DateTime1 = advance_seconds(DateTime, ?MINUTE_IN_SECONDS),
    {{Y, Mo, D}, {H, M, _}} = DateTime1,
    DateTime2 = {{Y, Mo, D}, {H, M, 0}},
    next_valid_datetime(not_done, {cron, Schedule}, DateTime2).


next_valid_datetime(done, _, DateTime) ->
    DateTime;
next_valid_datetime(not_done, {cron, Schedule}, DateTime) ->
    {MinuteSpec, HourSpec, DayOfMonthSpec, MonthSpec, DayOfWeekSpec} =
    Schedule,
    {{Year, Month, Day},  {Hour, Minute, _}} = DateTime,
    {Done, Time} =
    case value_valid(MonthSpec, 1, 12, Month) of
        false ->
            case Month of
                12 ->
                    {not_done, {{Year + 1, 1, 1}, {0, 0, 0}}};
                Month ->
                    {not_done, {{Year, Month + 1, 1}, {0, 0, 0}}}
            end;
        true ->
            DayOfWeek = case calendar:day_of_the_week(Year, Month, Day) of
                            7 ->
                                0; % we want 0 to be Sunday not 7
                            DOW ->
                                DOW
                        end,
            DOMValid = value_valid(DayOfMonthSpec, 1, 31, Day),
            DOWValid = value_valid(DayOfWeekSpec, 0, 6, DayOfWeek),
            case (((DayOfMonthSpec /= all) and
                   (DayOfWeekSpec /= all) and
                   (DOMValid or DOWValid)) or (DOMValid and DOWValid)) of
                false ->
                    Temp1 = advance_seconds(DateTime, ?DAY_IN_SECONDS),
                    {{Y, M, D}, {_, _, _}} = Temp1,
                    {not_done, {{Y, M, D}, {0, 0, 0}}};
                true ->
                    case value_valid(HourSpec, 0, 23, Hour) of
                        false ->
                            Temp3 = advance_seconds(DateTime,
                                                    ?HOUR_IN_SECONDS),
                            {{Y, M, D}, {H, _, _}} = Temp3,
                            {not_done, {{Y, M, D}, {H, 0, 0}}};
                        true ->
                            case value_valid(
                                   MinuteSpec, 0, 59, Minute) of
                                false ->
                                    {not_done, advance_seconds(
                                                 DateTime,
                                                 ?MINUTE_IN_SECONDS)};
                                true ->
                                    {done, DateTime}
                            end
                    end
            end
    end,
    next_valid_datetime(Done, {cron, Schedule}, Time).

 value_valid(Spec, Min, Max, Value) when Value >= Min, Value =< Max->
    case Spec of
	all ->
	    true;
	Spec ->
	    ValidValues = extract_integers(Spec, Min, Max),
	    lists:any(fun(Item) ->
			      Item == Value
		      end, ValidValues)
    end.


advance_seconds(DateTime, Seconds) ->
    Seconds1 = calendar:datetime_to_gregorian_seconds(DateTime) + Seconds,
    calendar:gregorian_seconds_to_datetime(Seconds1).


extract_integers(Spec, Min, Max) when Min < Max ->
    extract_integers(Spec, Min, Max, []).

extract_integers([], Min, Max, Acc) ->
    Integers = lists:sort(sets:to_list(sets:from_list(lists:flatten(Acc)))),
    lists:foreach(fun(Int) ->
			  if
			      Int < Min ->
				  throw({error, {out_of_range, {min, Min},
						 {value, Int}}});
			      Int > Max ->
				  throw({error, {out_of_range, {max, Max},
						{value, Int}}});
			      true ->
				  ok
			  end
		  end, Integers),
    Integers;
extract_integers(Spec, Min, Max, Acc) ->
    [H|T] = Spec,
    Values = case H of
		 {range, Lower, Upper} when Lower < Upper ->
		     lists:seq(Lower, Upper);
		 {list, List} ->
		     List;
		 {Lower, Upper} when Lower < Upper ->
		     lists:seq(Lower, Upper);
		 List when is_list(List) ->
		     List;
		 Integer when is_integer(Integer) ->
		     [Integer]
	     end,
    extract_integers(T, Min, Max, [Values|Acc]).


time_to_wait_millis(CurrentDateTime, NextDateTime) ->
    CurrentSeconds = calendar:datetime_to_gregorian_seconds(CurrentDateTime),
    NextSeconds = calendar:datetime_to_gregorian_seconds(NextDateTime),
    SecondsToSleep = NextSeconds - CurrentSeconds,
    SecondsToSleep * 1000.



