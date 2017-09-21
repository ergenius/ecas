%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%% Parse cql protocol frames comming from stream.
%%% The parser is able to handle parsing fragmented frames until the full frame is received.
%%% The parser is also able to handle parsing more (remaining) frames later.
%%% @end
%%% Created : 30. May 2017 10:43 AM
%%%-------------------------------------------------------------------
-module(ecas_lib_frame_parser).
-author("madalin").

-include_lib("ecas/include/ecas_protocol.hrl").

%% API
-export([initial_options/0]).
-export([new/0]).

-export([parse/2]).
-export([parse_more/5]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% initial_options
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns parser initial options
initial_options() -> #ecas_protocol_state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% new
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new parser
new() -> fun(Bin, Options) -> parse(Bin, Options) end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% parse
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Parse new data combing from stream
-spec(parse(binary(), ecas_protocol_options()) -> {ok, ecas_frame(), Rest} | {more, fun()} | {error, any()}).

%%=============================================
%% parse new
%%=============================================

%% Parse full frame with empty body
parse(<<Header:?ECAS_PROTO_TYPESPEC_HEADER, 0:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>, Options) ->

    DecodedResult = ecas_lib_frame_decoder:decode(Header, <<>>, Options),
    case DecodedResult of
        {ok, Decoded} -> {ok, Decoded, Rest};
        _ -> {error, DecodedResult}
    end;

%% Parse full frame with body
parse(<<Header:?ECAS_PROTO_TYPESPEC_HEADER, Size:?ECAS_PROTO_TYPESPEC_INT, Content:Size/binary, Rest/binary>>, Options) ->

    DecodedResult = ecas_lib_frame_decoder:decode(Header, Content, Options),
    case DecodedResult of
        {ok, Decoded} -> {ok, Decoded, Rest};
        _ -> {error, DecodedResult}
    end;

%% Ignore empty data
parse(<<>>, _Options) -> {more, fun(Bin, NewOptions) -> parse(Bin, NewOptions) end};

%% Parse partial frame that have full size.
parse(<<Header:?ECAS_PROTO_TYPESPEC_HEADER, Size:?ECAS_PROTO_TYPESPEC_INT, LastBin/binary>>, _Options) ->

    RemainingSize = Size - erlang:size(LastBin),
    {more, fun(Bin, NewOptions) -> parse_more(RemainingSize, Bin, LastBin, Header, NewOptions) end};

%% Parse partial packet that doesn't include the size component
parse(Partial, _Options) -> {more, fun(Bin, NewOptions) -> parse(<<Partial/binary, Bin/binary>>, NewOptions) end}.

%%=============================================
%% parse more
%%=============================================

%% Parse complete remaining body from current frame
parse_more(RemainingSize, <<Content:RemainingSize/binary, Rest/binary>>, LastBin, Header, Options) ->

    DecodedResult = ecas_lib_frame_decoder:decode(Header, <<LastBin/binary, Content/binary>>, Options),
    case DecodedResult of
        {ok, Decoded} -> {ok, Decoded, Rest};
        _ -> {error, DecodedResult}
    end;

%% Ignore empty data
parse_more(RemainingSize, <<>>, LastBin, Header, _Options) ->

    {more, fun(Bin, NewOptions) -> parse_more(RemainingSize, Bin, LastBin, Header, NewOptions) end};

%% Parse incomplete remaining content from current frame
parse_more(RemainingSize, More, LastBin, Header, _Options) ->

    NewRemainingSize = RemainingSize - erlang:size(More),
    {more, fun(Bin, NewOptions) -> parse_more(NewRemainingSize, Bin, <<LastBin/binary, More/binary>>, Header, NewOptions) end}.

