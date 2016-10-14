-module(dp_influx).
-behaviour(dp_decoder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([protocol/0, parse/1]).

-define(POS_INF, 42.0e+100).
-define(NEG_INF, -42.0e+100).
-define(NAN, 0).
-define(BOOL_TRUE, 1).
-define(BOOL_FALSE, 0).

%% @doc
%% Parses the Influx line protocol. The syntax for the Influx line protocol is:
%%  measurement[,tag_key1=tag_value1...] field_key=field_value[,...] [ts]
%%
%% More information on the protocol and escaping rules can be found online:
%% https://docs.influxdata.com/influxdb/v0.13/write_protocols/write_syntax
%% @end
-spec parse(In::binary()) ->
                   {ok, [dp_decoder:metric()]} | undefined.
parse(<<>>) ->
    eat_line(<<>>);
parse(<<"#", R/binary>>) ->
    eat_line(R);
parse(In) ->
    M = #{
      key => [],
      metric => [],
      tags => [],
      time => 0,
      value => 0
     },
    parse_measurement(In, <<>>, M).

eat_line(<<"\r\n", R/binary>>) ->
    {ok, [], R};
eat_line(<<"\n", R/binary>>) ->
    {ok, [], R};
eat_line(<<_, R/binary>>) ->
    eat_line(R);
eat_line(<<>>) ->
    {ok, [], <<>>}.

%% Measurements require escaping for commas and whitespace
parse_measurement(<<$\\, $,, R/binary>>, Measure, M) ->
    parse_measurement(R, <<Measure/binary, ",">>, M);
parse_measurement(<<"\\ ", R/binary>>, Measure, M) ->
    parse_measurement(R, <<Measure/binary, " ">>, M);
parse_measurement(<<",", R/binary>>, Measure, M) ->
    M1 = M#{metric := [Measure]},
    parse_tags(R, <<>>, M1);
parse_measurement(<<" ", R/binary>>, Measure, M) ->
    M1 = M#{metric := [Measure]},
    parse_metrics(R, <<>>, [], M1);
parse_measurement(<<C, R/binary>>, Measure, M) ->
    parse_measurement(R, <<Measure/binary, C>>, M).

%% Tag keys require escaping for commas, whitespace and equals
parse_tags(<<$\\, $,, R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, ",">>, M);
parse_tags(<<"\\ ", R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, " ">>, M);
parse_tags(<<" ", R/binary>>, Tag, M = #{tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{tags := Tags1},
    parse_metrics(R, <<>>, [], M1);
parse_tags(<<",", R/binary>>, Tag, M = #{tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{tags := Tags1},
    parse_tags(R, <<>>, M1);
parse_tags(<<C, R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, C>>, M).

parse_tag(<<$\\, $=, R/binary>>, K) ->
    parse_tag(R, <<K/binary, $=>>);
parse_tag(<<"=", V/binary>>, K) ->
    {K, V};
parse_tag(<<C, R/binary>>, K) ->
    parse_tag(R, <<K/binary, C>>).

%% Field keys require escaping for commas, whitespace and equals. Fields may
%% have 'boolean', 'string', 'int' or 'float' values. Fields with 'string'
%% values are re-classified as tags
parse_metrics(<<$", R/binary>>, _Metric, Metrics, M) ->
    {_V, R0} = parse_str_value(R, <<>>),
    parse_metrics(R0, <<>>, Metrics, M);
parse_metrics(<<$\\, $,, R/binary>>, Metric, Metrics, M) ->
    parse_metrics(R, <<Metric/binary, ",">>, Metrics, M);
parse_metrics(<<"\\ ", R/binary>>, Metric, Metrics, M) ->
    parse_metrics(R, <<Metric/binary, " ">>, Metrics, M);
parse_metrics(<<" ", TimeS/binary>>, <<>>, Metrics,
              M = #{metric := Ms, tags := Tags}) ->
    parse_time(TimeS, <<>>,
               [M#{value := V, metric := Ms ++ [K],
                   key := Ms ++ [K | dp_decoder:recombine_tags(Tags)]}
                || {K, V} <- Metrics]);
parse_metrics(<<" ", TimeS/binary>>, Metric, Metrics,
              M = #{metric := Ms, tags := Tags})  ->
    Metrics1 = [parse_metric(Metric, <<>>) | Metrics],
    parse_time(TimeS, <<>>,
               [M#{value := V, metric := Ms ++ [K],
                   key := Ms ++ [K | dp_decoder:recombine_tags(Tags)]}
                || {K, V} <- Metrics1]);
parse_metrics(<<",", R/binary>>, Metric, Metrics, M) when Metric =/= <<>> ->
    parse_metrics(R, <<>>, [parse_metric(Metric, <<>>) | Metrics], M);
parse_metrics(<<C, R/binary>>, Metric, Metrics, M) ->
    parse_metrics(R, <<Metric/binary, C>>, Metrics, M).

parse_time(<<>>, TimeS, Ms) ->
    Time = dp_decoder:to_time(TimeS),
    {ok, [M#{time := Time} || M <- Ms]};
parse_time(<<"\n", R/binary>>, TimeS, Ms) ->
    Time = dp_decoder:to_time(TimeS),
    {ok, [M#{time := Time} || M <- Ms], R};
parse_time(<<"\r\n", R/binary>>, TimeS, Ms) ->
    Time = dp_decoder:to_time(TimeS),
    {ok, [M#{time := Time} || M <- Ms], R};
parse_time(<<C, R/binary>>, TimeS, Ms) ->
    parse_time(R, <<TimeS/binary, C>>, Ms).

parse_metric(<<$\\, $=, R/binary>>, K) ->
    parse_metric(R, <<K/binary, "=">>);
parse_metric(<<"=", V/binary>>, K) ->
    {K, parse_value(V, <<>>)};
parse_metric(<<C, R/binary>>, K) ->
    parse_metric(R, <<K/binary, C>>).

%% Boolean true may be one of ['t', 'T', 'true', 'True', 'TRUE']
parse_value(<<"t">>, _V) ->
    ?BOOL_TRUE;
parse_value(<<"T">>, _V) ->
    ?BOOL_TRUE;
parse_value(<<"true">>, _V) ->
    ?BOOL_TRUE;
parse_value(<<"True">>, _V) ->
    ?BOOL_TRUE;
parse_value(<<"TRUE">>, _V) ->
    ?BOOL_TRUE;

%% Boolean false may be one of ['f', 'F', 'false', 'False', 'FALSE']
parse_value(<<"f">>, _V) ->
    ?BOOL_FALSE;
parse_value(<<"F">>, _V) ->
    ?BOOL_FALSE;
parse_value(<<"false">>, _V) ->
    ?BOOL_FALSE;
parse_value(<<"False">>, _V) ->
    ?BOOL_FALSE;
parse_value(<<"FALSE">>, _V) ->
    ?BOOL_FALSE;

%% Integers are suffixed with 'i'
parse_value(<<"i">>, V) ->
    binary_to_integer(V);

%% Floats do not need a suffix
parse_value(<<>>, V) ->
    %% This tolerates integers with no 'i' suffix
    dp_decoder:to_number(V);

parse_value(<<C, R/binary>>, V) ->
    parse_value(R, <<V/binary, C>>).

%% String field values only require '"' to be escaped
parse_str_value(<<>>, V) ->
    {V, <<>>};
parse_str_value(<<$\\, $", R/binary>>, V) ->
    parse_str_value(R, <<V/binary, $">>);
parse_str_value(<<$", R/binary>>, V) ->
    {V, R};
parse_str_value(<<C, R/binary>>, V) ->
    parse_str_value(R, <<V/binary, C>>).

-spec protocol() -> dp_multiline_proto.
protocol() ->
    dp_multiline_proto.

-ifdef(TEST).
p(In) ->
    {ok, E} = parse(In),
    E.

error_test() ->
    In = <<"system,host=Schroedinger uptime=101422i,uptime_format=\"1 day,  4:10\" 1472945100">>,
    Time = 1472945100,
    Value = 101422,
    Metric = [<<"system">>, <<"uptime">>],
    Tags = [{<<>>, <<"host">>, <<"Schroedinger">>}],
    [#{tags := RTags, time := RTime, value := RValue,
       metric := RMetric}] = p(In),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Value, RValue).

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

bool_true_test() ->
    In = <<"cpu,hostname=host_0 "
           "t1=T,t2=t,t3=true,t4=True,t5=TRUE "
           "1451606400000000000">>,
    Time = 1451606400,
    Value = 1,
    Metric1 = [<<"cpu">>, <<"t5">>],
    Metric2 = [<<"cpu">>, <<"t4">>],
    Metric3 = [<<"cpu">>, <<"t3">>],
    Metric4 = [<<"cpu">>, <<"t2">>],
    Metric5 = [<<"cpu">>, <<"t1">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>}],
    [#{tags := Tags, time := Time, value := RValue1, metric := RMetric1},
     #{tags := Tags, time := Time, value := RValue2, metric := RMetric2},
     #{tags := Tags, time := Time, value := RValue3, metric := RMetric3},
     #{tags := Tags, time := Time, value := RValue4, metric := RMetric4},
     #{tags := Tags, time := Time, value := RValue5, metric := RMetric5}]
        = p(In),
    ?assertEqual(Value,   RValue1),
    ?assertEqual(Metric1, RMetric1),
    ?assertEqual(Value,   RValue2),
    ?assertEqual(Metric2, RMetric2),
    ?assertEqual(Value,   RValue3),
    ?assertEqual(Metric3, RMetric3),
    ?assertEqual(Value,   RValue4),
    ?assertEqual(Metric4, RMetric4),
    ?assertEqual(Value,   RValue5),
    ?assertEqual(Metric5, RMetric5).

bool_false_test() ->
    In = <<"cpu,hostname=host_0 "
           "t1=F,t2=f,t3=false,t4=False,t5=FALSE "
           "1451606400000000000">>,
    Time = 1451606400,
    Value = 0,
    Metric1 = [<<"cpu">>, <<"t5">>],
    Metric2 = [<<"cpu">>, <<"t4">>],
    Metric3 = [<<"cpu">>, <<"t3">>],
    Metric4 = [<<"cpu">>, <<"t2">>],
    Metric5 = [<<"cpu">>, <<"t1">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>}],
    [#{tags := Tags, time := Time, value := RValue1, metric := RMetric1},
     #{tags := Tags, time := Time, value := RValue2, metric := RMetric2},
     #{tags := Tags, time := Time, value := RValue3, metric := RMetric3},
     #{tags := Tags, time := Time, value := RValue4, metric := RMetric4},
     #{tags := Tags, time := Time, value := RValue5, metric := RMetric5}]
        = p(In),
    ?assertEqual(Value,   RValue1),
    ?assertEqual(Metric1, RMetric1),
    ?assertEqual(Value,   RValue2),
    ?assertEqual(Metric2, RMetric2),
    ?assertEqual(Value,   RValue3),
    ?assertEqual(Metric3, RMetric3),
    ?assertEqual(Value,   RValue4),
    ?assertEqual(Metric4, RMetric4),
    ?assertEqual(Value,   RValue5),
    ?assertEqual(Metric5, RMetric5).

escaped_measurement_test() ->
    In = <<"\\,c\\ pu=,hostname=host_0 "
           "usage_user=58.1317132304976170 "
           "1451606400000000000">>,
    Metric = [<<",c pu=">>, <<"usage_user">>],
    [#{tags := _RTags, time := _RTime, value := _RValue, metric := RMetric}]
        = p(In),
    ?assertEqual(Metric, RMetric).

escaped_tag_keys_test() ->
    In = <<"cpu,h\\,ost\\ n\\=ame=host_0,\\ =east "
           "usage_user=58.1317132304976170 "
           "1451606400000000000">>,
    Tags = [{<<>>, <<" ">>, <<"east">>},
            {<<>>, <<"h,ost n=ame">>, <<"host_0">>}],
    [#{tags := RTags, time := _RTime, value := _RValue, metric := _RMetric}]
        = p(In),
    ?assertEqual(Tags, RTags).

escaped_fields_test() ->
    In = <<"cpu,hostname=host_0 "
           "us\\,a\\ge\\ u\\=ser=58.1317132304976170 "
           "1451606400000000000">>,
    Metric = [<<"cpu">>, <<"us,a\\ge u=ser">>],
    [#{tags := _RTags, time := _RTime, value := _RValue, metric := RMetric}]
        = p(In),
    ?assertEqual(Metric, RMetric).

escaped_field_value_test() ->
    In = <<"cpu,hostname=host_0 "
           "usage_user=\"J\\\"o, = h\\n\",io_time=3i "
           "1451606400000000000">>,
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>}],
    [#{tags := RTags, time := _RTime, value := _RValue, metric := _RMetric}] = p(In),
    ?assertEqual(Tags, RTags).

empty_timestamp_test() ->
    In = <<"cpu,hostname=host_0 "
           "usage_user=3i ">>,
    Time = erlang:system_time(seconds),
    Value = 3,
    Metric = [<<"cpu">>, <<"usage_user">>],
    Tags = [{<<>>, <<"hostname">>, <<"host_0">>}],
    [#{tags := RTags, time := RTime, value := RValue, metric := RMetric}]
        = p(In),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Value, RValue),
    ?assertEqual(Metric, RMetric).

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

no_tagset_test() ->
    In = <<"weather temperature=82 1465839830100400200">>,
    Time = 1465839830,
    Value = 82,
    Metric = [<<"weather">>, <<"temperature">>],
    Tags = [],
    [#{tags := RTags, time := RTime, value := RValue,
       metric := RMetric}] = p(In),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Value, RValue).

-endif.
