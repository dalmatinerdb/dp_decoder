	-module(dp_decoder).

-export([recombine_tags/1, to_number/1]).
-export_type([metric/0]).

-type metric() :: #{
              metric => [binary()],
              key => [binary()],
              tags => [{binary(), binary(), binary()}],
              time => pos_integer(),
              value => integer()
             }.

-type protocol() :: dp_line_proto.

-callback parse(In::binary()) ->
    dp_decoder:metric().

-callback protocol() ->
    dp_decoder:protocol().

recombine_tags(Tags) ->
    [<<K/binary, "=", V/binary>> || {_,K,V} <- Tags].

to_number(X) ->
    try
        binary_to_float(X)
    catch
        _:_ ->
            binary_to_integer(X)
    end.
