-module(dp_infux).
-behaviour(dp_decoder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([protocol/0, parse/1]).

-spec parse(In::binary()) ->
                   {ok, [dp_decoder:metric()]} | undefined.
-define(POS_INF, 42.0e+100).
-define(NEG_INF, -42.0e+100).
-define(NAN, 0).
parse(<<>>) ->
    undefined;
parse(In) ->
    M = #{
      key => [],
      metric => [],
      tags => [],
      time => 0,
      value => 0
     },
    parse_metric(In, <<>>, M).

parse_metric(<<",", R/binary>>, Metric, M) ->
    M1 = M#{key := [Metric],
            metric := [Metric]},
    parse_tags(R, <<>>, M1);
parse_metric(<<C, R/binary>>, Metric, M) ->
    parse_metric(R, <<Metric/binary, C>>, M).


parse_tags(<<" ", R/binary>>, Tag, M = #{key := Ks, tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{key := Ks ++ dp_decoder:recombine_tags(Tags1),
            tags := Tags1},
    parse_metrics(R, <<>>, [], M1);
parse_tags(<<",", R/binary>>, Tag, M = #{key := Ks, tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{key := Ks ++ dp_decoder:recombine_tags(Tags1),
            tags := Tags1},
    parse_tags(R, <<>>, M1);
parse_tags(<<C, R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, C>>, M).

parse_tag(<<"=", V/binary>>, K) ->
    {K, V};
parse_tag(<<C, R/binary>>, K) ->
    parse_tag(R, <<K/binary, C>>).

parse_metrics(<<" ", TimeS/binary>>, Metric, Metrics,
              M = #{key := [K1 | Ks], metric := Ms}) ->
    Metrics1 = [parse_metric(Metric, <<>>) | Metrics],
    Time = dp_decoder:to_number(TimeS) div (1000 * 1000 * 1000),
    M1 = M#{time := Time },
    {ok, [M1#{value := V, metric := Ms ++ [K],
              key := [K1, K | Ks]} || {K, V} <- Metrics1]};
parse_metrics(<<",", R/binary>>, Metric, Metrics, M) ->
    parse_metrics(R, <<>>, [parse_metric(Metric, <<>>) | Metrics], M);
parse_metrics(<<C, R/binary>>, Metric, Metrics, M) ->
    parse_metrics(R, <<Metric/binary, C>>, Metrics, M).

parse_metric(<<"=", V/binary>>, K) ->
    {K, parse_value(V, <<>>)};
parse_metric(<<C, R/binary>>, K) ->
    parse_metric(R, <<K/binary, C>>).

parse_value(<<"i">>, V) ->
    binary_to_integer(V);
parse_value(<<>>, V) ->
    binary_to_float(V);
parse_value(<<C, R/binary>>, V) ->
    parse_value(R, <<V/binary, C>>).



-spec protocol() -> dp_line_proto.
protocol() ->
    dp_line_proto.

-ifdef(TEST).
p(In) ->
    {ok, E} = parse(In),
    E.

int_test() ->
    In = <<"cpu,hostname=host_0,rack=67,os=Ubuntu16.1 "
           "io_time=42i "
           "1451606400000000000">>,
    Time = 1451606400,
    Value = 42,
    Metric = [<<"cpu">>, <<"io_time">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>},
            {<<>>, <<"os">>, <<"Ubuntu16.1">>},
            {<<>>, <<"rack">>, <<"67">>}],
    [#{tags := RTags, time := RTime, value := RValue,
       metric := RMetric}] = p(In),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Value, RValue).

float_test() ->
    In = <<"cpu,hostname=host_0,rack=67,os=Ubuntu16.1 "
           "usage_user=58.1317132304976170 "
           "1451606400000000000">>,
    Time = 1451606400,
    Value = 58.1317132304976170,
    Metric = [<<"cpu">>, <<"usage_user">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>},
            {<<>>, <<"os">>, <<"Ubuntu16.1">>},
            {<<>>, <<"rack">>, <<"67">>}],
    [#{tags := RTags, time := RTime, value := RValue, metric := RMetric}]
        = p(In),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Value, RValue).


multi_test() ->
    In = <<"cpu,hostname=host_0,rack=67,os=Ubuntu16.1 "
           "usage_user=58.1317132304976170,io_time=0i "
           "1451606400000000000">>,
    Time = 1451606400,
    Value1 = 0,
    Value2 = 58.1317132304976170,
    Metric1 = [<<"cpu">>, <<"io_time">>],
    Metric2 = [<<"cpu">>, <<"usage_user">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>},
            {<<>>, <<"os">>, <<"Ubuntu16.1">>},
            {<<>>, <<"rack">>, <<"67">>}],
    [#{tags := RTags1, time := RTime1, value := RValue1, metric := RMetric1},
     #{tags := RTags2, time := RTime2, value := RValue2, metric := RMetric2}]
        = p(In),
    ?assertEqual(Tags,    RTags1),
    ?assertEqual(Time,    RTime1),
    ?assertEqual(Metric1, RMetric1),
    ?assertEqual(Value1,  RValue1),
    ?assertEqual(Tags,    RTags2),
    ?assertEqual(Time,    RTime2),
    ?assertEqual(Metric2, RMetric2),
    ?assertEqual(Value2,  RValue2).
-endif.
