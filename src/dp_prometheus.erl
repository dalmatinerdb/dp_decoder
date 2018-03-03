-module(dp_prometheus).
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
parse(<<" ", R/binary>>) ->
    parse(R);
parse(<<$\t, R/binary>>) ->
    parse(R);
parse(<<"#", _/binary>>) ->
    undefined;
parse(In) ->
    M = #{
      key => [],
      metric => [],
      tags => [],
      time => 0,
      hpts => 0,
      value => 0
     },
    parse_metric(In, <<>>, M).

-spec protocol() -> dp_line_proto.
protocol() ->
    dp_line_proto.

parse_metric(<<"{", R/binary>>, Metric, M) ->
    M1 = M#{key := [Metric],
            metric := [Metric]},
    parse_tags(R, <<>>, M1);
parse_metric(<<" ", R/binary>>, Metric, M) ->
    M1 = M#{key := [Metric],
            metric := [Metric]},
    parse_time(R, M1);
parse_metric(<<C, R/binary>>, Tag, M) ->
    parse_metric(R, <<Tag/binary, C>>, M).

%% Special case since tags can have a tailing ','
parse_tags(<<",} ", R/binary>>, Tag, M) ->
    parse_tags(<<"} ", R/binary>>, Tag, M);
parse_tags(<<"} ", R/binary>>, Tag,
           M = #{key := Ks, tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    Tags1 = lists:sort([{<<"">>, K, V} | Tags]),
    M1 = M#{key := Ks ++ dp_decoder:recombine_tags(Tags1),
            tags := Tags1},
    parse_time(R, M1);

parse_tags(<<",", R/binary>>, Tag, M = #{tags := Tags}) ->
    {K, V} = parse_tag(Tag, <<>>),
    M1 = M#{tags := [{<<"">>, K, V} | Tags]},
    parse_tags(R, <<>>, M1);

parse_tags(<<$", R/binary>>, Tag, M) ->
    parse_tags_quoted(R, <<Tag/binary, $">>, M);

parse_tags(<<C, R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, C>>, M).

parse_tags_quoted(<<$", R/binary>>, Tag, M) ->
    parse_tags(R, <<Tag/binary, $">>, M);
parse_tags_quoted(<<$\\, C, R/binary>>, Tag, M) ->
    parse_tags_quoted(R, <<Tag/binary, $\\, C>>, M);
parse_tags_quoted(<<C, R/binary>>, Tag, M) ->
    parse_tags_quoted(R, <<Tag/binary, C>>, M).

parse_tag(<<"=\"", V/binary>>, K) ->
    {K, unescape(V, <<>>)};
parse_tag(<<C, R/binary>>, K) ->
    parse_tag(R, <<K/binary, C>>).

unescape(<<"\"">>, V) ->
    V;
unescape(<<$\\, $", R/binary>>, V) ->
    unescape(R, <<V/binary, $">>);
unescape(<<$\\, $\\, R/binary>>, V) ->
    unescape(R, <<V/binary, $\\>>);
unescape(<<$\\, $n, R/binary>>, V) ->
    unescape(R, <<V/binary, $\n>>);
unescape(<<C, R/binary>>, V) ->
    unescape(R, <<V/binary, C>>).


parse_time(<<"+Inf", R/binary>>, M) ->
    parse_time(R, float_to_binary(?POS_INF), M);
parse_time(<<"-Inf", R/binary>>, M) ->
    parse_time(R, float_to_binary(?NEG_INF), M);
parse_time(<<"NaN", R/binary>>, M) ->
    parse_time(R, integer_to_binary(?NAN), M);
parse_time(R, M) ->
    parse_time(R, <<>>, M).

parse_time(<<>>, V, M) ->
    Vi = dp_decoder:to_number(V),
    HPTS = erlang:system_time(nanosecond),
    Ti = erlang:convert_time_unit(HPTS, nanosecond, second),
    {ok, [M#{time := Ti, hpts := HPTS, value := Vi}]};

parse_time(<<" ", T/binary>>, V, M) ->
    Vi = dp_decoder:to_number(V),
    Tr = binary_to_integer(T),
    Ti = erlang:convert_time_unit(Tr, millisecond, second),
    HPTS = erlang:convert_time_unit(Tr, millisecond, nanosecond),
    {ok, [M#{time := Ti, hpts := HPTS, value := Vi}]};

parse_time(<<C, R/binary>>, V, M) ->
    parse_time(R, <<V/binary, C>>, M).

-ifdef(TEST).
p(In) ->
    {ok, [E]} = parse(In),
    E.

nan_test() ->
    In = <<"metric_without_timestamp_and_labels NaN 1395066363000">>,
    Metric = [<<"metric_without_timestamp_and_labels">>],
    Key = [<<"metric_without_timestamp_and_labels">>],
    Tags = [],
    Time = 1395066363,
    HPTS = 1395066363000000000,
    Value = ?NAN,
    #{
       metric := RMetric,
       key    := RKey,
       tags   := RTags,
       time   := RTime,
       hpts   := RHPTS,
       value  := RValue
     } = p(In),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Key, RKey),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(HPTS, RHPTS),
    ?assertEqual(Value, RValue).

bad_float_test() ->
    In = <<"metric_without_timestamp_and_labels 12e+06 1395066363000">>,
    Metric = [<<"metric_without_timestamp_and_labels">>],
    Key = [<<"metric_without_timestamp_and_labels">>],
    Tags = [],
    Time = 1395066363,
    Value = 12.0e+06,
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

pos_inf_test() ->
    In = <<"metric_without_timestamp_and_labels +Inf 1395066363000">>,
    Metric = [<<"metric_without_timestamp_and_labels">>],
    Key = [<<"metric_without_timestamp_and_labels">>],
    Tags = [],
    Time = 1395066363,
    Value = ?POS_INF,
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

neg_inf_test() ->
    In = <<"metric_without_timestamp_and_labels -Inf 1395066363000">>,
    Metric = [<<"metric_without_timestamp_and_labels">>],
    Key = [<<"metric_without_timestamp_and_labels">>],
    Tags = [],
    Time = 1395066363,
    Value = ?NEG_INF,
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
example_test() ->
    In = <<"http_requests_total{method=\"post\",code=\"200\"} 1027 ",
           "1395066363000">>,
    Metric = [<<"http_requests_total">>],
    Key = [<<"http_requests_total">>,<<"code=200">>, <<"method=post">>],
    Tags = [{<<>>, <<"code">>, <<"200">>},
            {<<>>, <<"method">>, <<"post">>}],
    Time = 1395066363,
    HPTS = 1395066363000000000,
    Value = 1027,
    #{
       metric := RMetric,
       key    := RKey,
       tags   := RTags,
       time   := RTime,
       hpts   := RHPTS,
       value  := RValue
     } = p(In),
    ?assertEqual(Metric, RMetric),
    ?assertEqual(Key, RKey),
    ?assertEqual(Tags, RTags),
    ?assertEqual(Time, RTime),
    ?assertEqual(HPTS, RHPTS),
    ?assertEqual(Value, RValue).
tailing_colon_test() ->
    In = <<"http_requests_total{method=\"post\",code=\"200\",} 1027 ",
           "1395066363000">>,
    Metric = [<<"http_requests_total">>],
    Key = [<<"http_requests_total">>,<<"code=200">>, <<"method=post">>],
    Tags = [{<<>>, <<"code">>, <<"200">>},
            {<<>>, <<"method">>, <<"post">>}],
    Time = 1395066363,
    Value = 1027,
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

minimal_test() ->
    In = <<"metric_without_timestamp_and_labels 12.47">>,
    Metric = [<<"metric_without_timestamp_and_labels">>],
    Key = [<<"metric_without_timestamp_and_labels">>],
    Tags = [],
    Time = erlang:system_time(seconds),
    Value = 12.47,
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
    %% since this will use a timestamp generated by us
    %% we can expect that parsing takes less then a second
    %% so worst case we start right before a second flip
    %% so the delta would be 1
    ?assert(RTime - Time =< 1),
    ?assertEqual(Value, RValue).
escape_test() ->
    In = <<"msdos_file_access_time_seconds{",
           "path=\"C:\\DIR\\FILE.TXT\",",
           "error=\"Cannot find file:\n\\\"FILE.TXT\\\"\"} ",
           "1.458255915e9">>,
    Metric = [<<"msdos_file_access_time_seconds">>],
    Key = [<<"msdos_file_access_time_seconds">>,
           <<"error=Cannot find file:\n\"FILE.TXT\"">>,
           <<"path=C:\\DIR\\FILE.TXT">>],
    Tags = [{<<>>, <<"error">>, <<"Cannot find file:\n\"FILE.TXT\"">>},
            {<<>>, <<"path">>, <<"C:\\DIR\\FILE.TXT">>}],
    Time = erlang:system_time(seconds),
    Value = 1458255915.0,
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
    %% since this will use a timestamp generated by us
    %% we can expect that parsing takes less then a second
    %% so worst case we start right before a second flip
    %% so the delta would be 1
    ?assert(RTime - Time =< 1),
    ?assertEqual(Value, RValue).

comma_in_tag_value_test() ->
    In = <<"node_filesystem_files{",
           "device=\"cpu,cpuacct\",",
           "fstype=\"cgroup\",",
           "mountpoint=\"/run/lxcfs/controllers/cpu,cpuacct\"} ",
           "0">>,
    Tags = [{<<>>, <<"device">>, <<"cpu,cpuacct">>},
            {<<>>, <<"fstype">>, <<"cgroup">>},
            {<<>>, <<"mountpoint">>, <<"/run/lxcfs/controllers/cpu,cpuacct">>}],
    #{tags := RTags} = p(In),
    ?assertEqual(Tags, RTags).
-endif.
