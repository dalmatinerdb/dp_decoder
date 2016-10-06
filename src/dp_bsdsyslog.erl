-module(dp_bsdsyslog).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(dp_decoder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([protocol/0, parse/1]).

-spec protocol() -> dp_line_proto.
protocol() ->
    dp_line_proto.


-spec parse(In::binary()) ->
                   {ok, [dp_decoder:event()]} | undefined.
parse(<<"<", N, R/binary>>) ->
    M = #{
      type => event,
      data => #{}
     },
    parse_priority(R, <<N>>, M);
parse(_M) ->
    undefined.

parse_priority(<<">", R/binary>>, PrioS, M = #{data := D}) ->
    Prio = binary_to_integer(PrioS),
    FacilityI = Prio div 8,
    SeverityI = Prio rem 8,
    D1 = D#{
           <<"priority">> => Prio,
           <<"facility_int">> => FacilityI,
           <<"severity_int">> => SeverityI,
           <<"facility">> => int_to_facility(FacilityI),
           <<"severity">> => int_to_severity(SeverityI)
          },
    M1 = M#{data => D1},
    parse_month(R, M1);
parse_priority(<<C, R/binary>>, PrioS, M) when C >= $0, C =< $9 ->
    parse_priority(R, <<PrioS/binary, C>>, M).


parse_month(<<"Jan ", R/binary>>, M) ->
    parse_day(R, 1, M);
parse_month(<<"Feb ", R/binary>>, M) ->
    parse_day(R, 2, M);
parse_month(<<"Mar ", R/binary>>, M) ->
    parse_day(R, 3, M);
parse_month(<<"Apr ", R/binary>>, M) ->
    parse_day(R, 4, M);
parse_month(<<"May ", R/binary>>, M) ->
    parse_day(R, 5, M);
parse_month(<<"Jun ", R/binary>>, M) ->
    parse_day(R, 6, M);
parse_month(<<"Jul ", R/binary>>, M) ->
    parse_day(R, 7, M);
parse_month(<<"Aug ", R/binary>>, M) ->
    parse_day(R, 8, M);
parse_month(<<"Sep ", R/binary>>, M) ->
    parse_day(R, 9, M);
parse_month(<<"Oct ", R/binary>>, M) ->
    parse_day(R, 10, M);
parse_month(<<"Nov ", R/binary>>, M) ->
    parse_day(R, 11, M);
parse_month(<<"Dec ", R/binary>>, M) ->
    parse_day(R, 12, M).

parse_day(<<" ", Day:1/binary, " ", R/binary>>, Mon, M) ->
    parse_time(R, Mon, binary_to_integer(Day), M);

parse_day(<<Day:2/binary, " ", R/binary>>, Mon, M) ->
    parse_time(R, Mon, binary_to_integer(Day), M).

parse_time(<<HH:2/binary, ":", MM:2/binary, ":", SS:2/binary, " ", R/binary>>,
           Month, Day, M = #{data := D}) ->
    {{Year, _, _}, _} = erlang:localtime(),
    Local = {{Year, Month, Day},
             {binary_to_integer(HH),
              binary_to_integer(MM),
              binary_to_integer(SS)}},
    Universal = erlang:localtime_to_universaltime(Local),
    Seconds = erlang:universaltime_to_posixtime(Universal),
    Nano = erlang:convert_time_unit(Seconds, seconds, nano_seconds),
    D1 = D#{<<"timestamp">> => Nano},
    M1 = M#{data => D1, time => Nano},
    parse_hostname(R, <<>>, M1).

%% If the hostname ends with a ':' it seems to be considered a tag
%% FFS who came up with this ...
parse_hostname(<<": ", R/binary>>, H, M = #{data := D}) ->
    D1 = D#{<<"hostname">> => <<"localhost">>,
            <<"tag">> => H,
            <<"body">> => nonl(R)},
    {ok, [M#{data => D1}]};
parse_hostname(<<" ", R/binary>>, H, M = #{data := D}) ->
    D1 = D#{<<"hostname">> => H},
    M1 = M#{data => D1},
    parse_tag(R, <<>>, M1);
parse_hostname(<<C, R/binary>>, H, M) ->
    parse_hostname(R, <<H/binary, C>>, M).

parse_tag(<<": ", R/binary>>, T, M = #{data := D}) ->
    D1 = D#{<<"tag">> => T,
            <<"body">> => nonl(R)},
    {ok, [M#{data => D1}]};
%% parse_tag(<<" ", R/binary>>, T, M = #{data := D}) ->
%%     D1 = D#{<<"tag">> => T,
%%             <<"body">> => R},
%%     {ok, [M#{data => D1}]};
parse_tag(<<C, R/binary>>, T, M) ->
    parse_tag(R, <<T/binary, C>>, M).


nonl(B) ->
    Size = byte_size(B) - 1,
    case B of
        <<S:Size/binary, $\n>> ->
            S;
        _ ->
            B
    end.

int_to_facility(0) -> <<"kern">>;
int_to_facility(1) -> <<"user">>;
int_to_facility(2) -> <<"mail">>;
int_to_facility(3) -> <<"system">>;
int_to_facility(4) -> <<"auth">>;
int_to_facility(5) -> <<"internal">>;
int_to_facility(6) -> <<"lpr">>;
int_to_facility(7) -> <<"nns">>;
int_to_facility(8) -> <<"uucp">>;
int_to_facility(9) -> <<"clock">>;
int_to_facility(10) -> <<"authpriv">>;
int_to_facility(11) -> <<"ftp">>;
int_to_facility(12) -> <<"ntp">>;
int_to_facility(13) -> <<"audit">>;
int_to_facility(14) -> <<"alert">>;
int_to_facility(15) -> <<"clock2">>;
int_to_facility(16) -> <<"local0">>;
int_to_facility(17) -> <<"local1">>;
int_to_facility(18) -> <<"local2">>;
int_to_facility(19) -> <<"local3">>;
int_to_facility(20) -> <<"local4">>;
int_to_facility(21) -> <<"local5">>;
int_to_facility(22) -> <<"local6">>;
int_to_facility(23) -> <<"local7">>;
int_to_facility(_) -> <<"">>.


int_to_severity(0) -> <<"emerg">>;
int_to_severity(1) -> <<"alert">>;
int_to_severity(2) -> <<"crit">>;
int_to_severity(3) -> <<"err">>;
int_to_severity(4) -> <<"warn">>;
int_to_severity(5) -> <<"notice">>;
int_to_severity(6) -> <<"info">>;
int_to_severity(7) -> <<"debug">>;
int_to_severity(_) -> <<"">>.

-ifdef(TEST).

to_nano(Month, Day, Hour, Minute, Second) ->
    {{Year, _, _}, _} = erlang:localtime(),
    Local = {{Year, Month, Day}, {Hour, Minute, Second}},
    Universal = erlang:localtime_to_universaltime(Local),
    Seconds = erlang:universaltime_to_posixtime(Universal),
    erlang:convert_time_unit(Seconds, seconds, nano_seconds).

p(E) ->
    case parse(list_to_binary(E)) of
        {ok, [#{data := D}]} ->
            D;
        R ->
            R
    end.
parse_test() ->
    Expected1 = #{
      <<"priority">> => 30,
      <<"facility_int">> => 3,
      <<"facility">> => <<"system">>,
      <<"severity_int">> => 6,
      <<"severity">> => <<"info">>,
      <<"timestamp">> => to_nano(12, 13, 19, 40, 50),
      %% host has been left off, so assume localhost
      <<"hostname">> => <<"localhost">>,
      <<"tag">> => <<"thttpd[1340]">>,
      <<"body">>
          => <<"192.168.1.138 - admin \"GET /cgi-bin/Qdownload/html/"
               "1260751250.rcsv HTTP/1.1\" 200 138 \"http://illmatic:8080/"
               "cgi-bin/Qdownload/html/rlist.html\" \"Mozilla/5.0 (Macintosh;"
               " U; Intel Mac OS X 10.6; en-US; rv:1.9.1.5) Gecko/2009110\"">>
     },
    Parsed1 = p("<30>Dec 13 19:40:50 thttpd[1340]: 192.168.1.138 - "
                "admin \"GET /cgi-bin/Qdownload/html/1260751250.rcsv "
                "HTTP/1.1\" 200 138 "
                "\"http://illmatic:8080/cgi-bin/Qdownload/html/"
                "rlist.html\" \"Mozilla/5.0 (Macintosh; U; Intel Mac "
                "OS X 10.6; en-US; rv:1.9.1.5) Gecko/2009110\""),

    Expected2 = #{
      <<"priority">> => 30,
      <<"facility_int">> => 3,
      <<"facility">> => <<"system">>,
      <<"severity_int">> => 6,
      <<"severity">> => <<"info">>,
      <<"timestamp">> => to_nano(12, 13, 19, 41, 03),
      <<"hostname">> => <<"localhost">>, % host has been left off, so assume localhost
      <<"tag">> => <<"thttpd[1340]">>,
      <<"body">> => <<"spawned CGI process 24156 for file "
                "'cgi-bin/Qdownload/refresh.cgi'">>
     },
    Parsed2 = p("<30>Dec 13 19:41:03 thttpd[1340]: spawned CGI "
                "process 24156 for file "
                "'cgi-bin/Qdownload/refresh.cgi'"),

    Expected3 = #{
      <<"priority">> => 147,
      <<"facility_int">> => 18,
      <<"facility">> => <<"local2">>,
      <<"severity_int">> => 3,
      <<"severity">> => <<"err">>,
      <<"timestamp">> => to_nano(11, 18, 19, 17, 55),
      <<"hostname">> => <<"myhost">>,
      <<"tag">> => <<"mytag[909]">>,
      <<"body">> => <<"yo what's really real">>
     },
    Parsed3 = p("<147>Nov 18 19:17:55 myhost mytag[909]: yo what's "
                "really real"),

    Expected4 = #{
      <<"priority">> => 147,
      <<"facility_int">> => 18,
      <<"facility">> => <<"local2">>,
      <<"severity_int">> => 3,
      <<"severity">> => <<"err">>,
      <<"timestamp">> => to_nano(11, 18, 19, 17, 55),
      <<"hostname">> => <<"myhost">>,
      <<"tag">> => <<"mytag[909]">>,
      <<"body">> => <<"yo">>
     },
    Parsed4 = p("<147>Nov 18 19:17:55 myhost mytag[909]: yo"),

    Expected5 = #{
      <<"priority">> => 12,
      <<"facility_int">> => 1,
      <<"facility">> => <<"user">>,
      <<"severity_int">> => 4,
      <<"severity">> => <<"warn">>,
      <<"timestamp">> => to_nano(9, 19, 3, 55, 25),
      <<"hostname">> => <<"Schroedinger">>,
      <<"tag">> => <<"com.apple.xpc.launchd[1] (com.adobe.ARMDCHelper."
               "cc24aef4a1b90ed56a725c38014c95072f92651fb65e1bf9c"
               "8e43c37a23d420d[60845])">>,
      <<"body">> => <<"Service exited with abnormal code: 111">>
     },
    Parsed5 = p("<12>Sep 19 03:55:25 Schroedinger com.apple.xpc."
                "launchd[1] (com.adobe.ARMDCHelper.cc24aef4a1b90ed56"
                "a725c38014c95072f92651fb65e1bf9c8e43c37a23d420d"
                "[60845]): Service exited with abnormal code: 111\n"),

    Expected6 = #{
      <<"priority">> => 5,
      <<"facility_int">> => 0,
      <<"facility">> => <<"kern">>,
      <<"severity_int">> => 5,
      <<"severity">> => <<"notice">>,
      <<"timestamp">> => to_nano(9, 19, 4, 2, 59),
      <<"hostname">> => <<"Schroedinger">>,
      <<"tag">> => <<"sandboxd[155] ([531])">>,
      <<"body">> => <<"Paste(531) deny file-read-data "
                "/Applications/Emacs.app/Contents/PkgInfo">>
     },
    Parsed6 = p("<5>Sep 19 04:02:59 Schroedinger sandboxd[155] "
                "([531]): Paste(531) deny file-read-data "
                "/Applications/Emacs.app/Contents/PkgInfo\n"),
    Expected7 = #{<<"body">> => <<>>,
                  <<"facility">> => <<"user">>,
                  <<"facility_int">> => 1,
                  <<"hostname">> => <<"Schroedinger">>,
                  <<"priority">> => 13,
                  <<"severity">> => <<"notice">>,
                  <<"severity_int">> => 5,
                  <<"tag">> => <<"Twitter[10811]">>,
                  <<"timestamp">> => 1474298748000000000},
    Parsed7 = p("<13>Sep 19 17:25:48 Schroedinger Twitter[10811]: \n"),

    Expected8 = #{},
    Parsed8 = p("<30>Oct  3 08:58:08 gcenagiosn0 dataloop-agent[28746]:  "
                "* Stopping dataloop-agent daemon:"),
    ?assertEqual(Expected1, Parsed1),
    ?assertEqual(Expected2, Parsed2),
    ?assertEqual(Expected3, Parsed3),
    ?assertEqual(Expected4, Parsed4),
    ?assertEqual(Expected5, Parsed5),
    ?assertEqual(Expected6, Parsed6),
    ?assertEqual(Expected7, Parsed7),
    ?assertEqual(Expected8, Parsed8),
    ?assertEqual(undefined, parse("asdf")).

-endif.
