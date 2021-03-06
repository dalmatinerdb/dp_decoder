-module(dp_otsdb).
-behaviour(dp_decoder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([protocol/0, parse/1]).

-spec parse(In::binary()) ->
                   {ok, [dp_decoder:metric()]} | undefined.
parse(<<"put ", In/binary>>) ->
    M = #{
      metric => [],
      key => [],
      tags => [],
      time => 0,
      value => 0
     },
    parse_metric(In, <<>>, M).

-spec protocol() -> dp_decoder:protocol().
protocol() ->
    dp_line_proto.

parse_metric(<<" ", R/binary>>, Part,
             M = #{key := Ks}) ->
    Ks1 = [Part | Ks],
    Ks2 = lists:reverse(Ks1),
    M1 = M#{metric := Ks2,
            key := Ks2},
    parse_time(R, <<>>, M1);

parse_metric(<<".", R/binary>>, Part,
             M = #{key := Ks}) ->
    M1 = M#{key := [Part | Ks]},
    parse_metric(R, <<>>, M1);

parse_metric(<<C, R/binary>>, Part, M) ->
    parse_metric(R, <<Part/binary, C>>, M).


parse_time(<<" ", R/binary>>, T, M) ->
    Ti = binary_to_integer(T),
    M1 = M#{time := Ti},
    parse_value(R, <<>>, M1);

parse_time(<<C, R/binary>>, Part, M) ->
    parse_time(R, <<Part/binary, C>>, M).

parse_value(<<" ", R/binary>>, V, M) ->
    Vi = dp_decoder:to_number(V),
    M1 = M#{value := Vi},
    parse_tags(R, <<>>, M1);

parse_value(<<C, R/binary>>, Part, M) ->
    parse_value(R, <<Part/binary, C>>, M).


parse_tags(<<>>, Tag, M = #{tags := Tags, key := Ks}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{key := Ks ++ dp_decoder:recombine_tags(Tags1),
            tags := Tags1},
    {ok, [M1]};

parse_tags(<<" ", R/binary>>, Tag, M = #{tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    M1 = M#{tags := [{<<"">>, K, V} | Tags]},
    parse_tags(R, <<>>, M1);

parse_tags(<<C, R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, C>>, M).

parse_tag(<<"=", V/binary>>, K) ->
    {K, V};
parse_tag(<<C, R/binary>>, K) ->
    parse_tag(R, <<K/binary, C>>).

-ifdef(TEST).
p(In) ->
    {ok, [E]} = parse(In),
    E.
example_test() ->
    In = <<"put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0">>,
    Metric = [<<"sys">>, <<"cpu">>, <<"user">>],
    Key = [<<"sys">>, <<"cpu">>, <<"user">>,<<"cpu=0">>,
           <<"host=webserver01">>],
    Tags = [{<<>>, <<"cpu">>, <<"0">>},
            {<<>>, <<"host">>, <<"webserver01">>}],
    Time = 1356998400,
    Value = 42.5,
    #{
       metric := RMetric,
       key    := RKey,
       tags   := RTags,
       time   := RTime,
       value  := RValue
     } = p(In),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Key, RKey),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Value, RValue).
-endif.

