
-type name() :: term().

-type cron() :: {cron, {cron_spec(), cron_spec(), cron_spec(), cron_spec(), cron_spec()}}.

-type cron_spec() :: all | list_spec() | range_spec().

-type list_spec() :: [integer()] | {list, [integer()]} | integer().

-type range_spec() :: {integer(), integer()} | {range, {integer(), integer()}}.

