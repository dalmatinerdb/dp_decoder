-module(dp_decoder).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([recombine_tags/1, to_number/1, protocol/1, parse/2]).
-export_type([metric/0, protocol/0]).

-type metric() :: #{
              metric => [binary()],
              key => [binary()],
              tags => [{binary(), binary(), binary()}],
              time => pos_integer(),
              value => integer()
             }.

-type protocol() :: dp_line_proto.

-callback parse(In::binary()) ->
    {ok, [dp_decoder:metric()]} | {error, term()} | undefined.

-callback protocol() ->
    dp_decoder:protocol().

recombine_tags(Tags) ->
    [<<K/binary, "=", V/binary>> || {_,K,V} <- Tags].

to_number(X) ->
    try
        binary_to_float(fix_num(X, <<>>))
    catch
        error:badarg ->
            binary_to_integer(X)
    end.

-spec parse(Decoder::module(), In::binary()) ->
                   {ok, [dp_decoder:metric()]} | {error, term()} | undefined.
parse(Decoder, In) ->
    Decoder:parse(In).

-spec protocol(Decoder :: module()) ->
    dp_decoder:protocol().
protocol(Decoder) ->
    Decoder:protocol().

fix_num(<<>>, Acc) ->
    Acc;
fix_num(<<"e", R/binary>>, Acc) ->
    <<Acc/binary, ".0e", R/binary>>;
fix_num(<<".", R/binary>>, Acc) ->
    <<Acc/binary, ".", R/binary>>;
fix_num(<<C, R/binary>>, Acc) ->
    fix_num(R, <<Acc/binary, C>>).


-ifdef(TEST).
to_number_test_() ->
    [?_assertEqual(to_number(<<"21.12">>), 21.12),
     ?_assertEqual(to_number(<<"21">>), 21),
     ?_assertEqual(to_number(<<"1.2e-2">>), 0.012),
     ?_assertEqual(to_number(<<"12e-2">>), 0.12)].
-endif.
