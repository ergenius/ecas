%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%% Implements encoding/decoding routines for al cql protocol datatypes.
%%% Makes caller less aware of common data types.
%%% @end
%%% Created : 31. May 2017 12:59 PM
%%%-------------------------------------------------------------------
-module(ecas_lib_frame_types).
-author("madalin").

-include_lib("ecas/include/ecas_protocol.hrl").

-export([encode_int/1]).
-export([decode_int/1]).

-export([encode_long/1]).
-export([decode_long/1]).

-export([encode_byte/1]).
-export([decode_byte/1]).

-export([encode_short/1]).
-export([decode_short/1]).

-export([encode_string/1]).
-export([decode_string/1]).

-export([encode_long_string/1]).
-export([decode_long_string/1]).

-export([encode_uuid/1]).
-export([decode_uuid/1]).

-export([encode_string_list/1]).
-export([decode_string_list/1]).

-export([encode_bytes/1]).
-export([decode_bytes/1]).

-export([encode_value/1]).
-export([decode_value/1]).

-export([encode_short_bytes/1]).
-export([decode_short_bytes/1]).

-export([encode_unsigned_vint/1]).
-export([decode_unsigned_vint/1]).

-export([encode_vint/1]).
-export([decode_vint/1]).

%%-export([encode_option/1]).
%%-export([decode_option/1]).

%%-export([encode_option_list/1]).
%%-export([decode_option_list/1]).

-export([encode_inet/1]).
-export([decode_inet/1]).

-export([encode_inetaddr/1]).
-export([decode_inetaddr/1]).

-export([encode_consistency/1]).
-export([decode_consistency/1]).

-export([encode_string_map/1]).
-export([decode_string_map/1]).

-export([encode_string_multimap/1]).
-export([decode_string_multimap/1]).

-export([encode_bytes_map/1]).
-export([decode_bytes_map/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% int
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode 4 bytes integer
encode_int(Value) when erlang:is_integer(Value) -> <<Value:?ECAS_PROTO_TYPESPEC_INT>>.

%% @doc Decode 4 bytes integer
decode_int(<<Value:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {Value, Rest};
decode_int(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% long
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode 8 bytes integer
encode_long(Value) when erlang:is_integer(Value) -> <<Value:?ECAS_PROTO_TYPESPEC_LONG>>.

%% @doc Decode 8 bytes integer
decode_long(<<Value:?ECAS_PROTO_TYPESPEC_LONG, Rest/binary>>) -> {Value, Rest};
decode_long(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% byte
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode 1 byte unsigned integer
encode_byte(Value) when erlang:is_integer(Value) -> <<Value:1/big-unsigned-integer>>.

%% @doc Decode 1 byte unsigned integer
decode_byte(<<Value:1/big-unsigned-integer, Rest/binary>>) -> {Value, Rest};
decode_byte(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% short
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode 2 bytes unsigned integer
encode_short(Value) when erlang:is_integer(Value) -> <<Value:?ECAS_PROTO_TYPESPEC_SHORT>>.

%% @doc Decode 2 bytes unsigned integer
decode_short(<<Value:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> {Value, Rest};
decode_short(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% string
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode string.
%% String is a [short] n, followed by n bytes representing an UTF-8 string.
encode_string(Value) when erlang:is_list(Value) -> encode_string(erlang:list_to_binary(Value));
encode_string(Value) when erlang:is_binary(Value) -> encode_string(Value, erlang:size(Value)).
encode_string(_Value, 0) -> <<0:?ECAS_PROTO_TYPESPEC_SHORT>>;
encode_string(Value, Size) when erlang:is_binary(Value), Size =< ?ECAS_PROTO_MAX_TYPE_SHORT -> <<Size:?ECAS_PROTO_TYPESPEC_SHORT, Value/binary>>.

%% @doc Decode a string.
%% String is a [short] n, followed by n bytes representing an UTF-8 string.
decode_string(<<0:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> {<<>>, Rest};
decode_string(<<Size:?ECAS_PROTO_TYPESPEC_SHORT, Value:Size/binary, Rest/binary>>) -> {Value, Rest};
decode_string(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% long_string
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode long string.
%% String is a [short] n, followed by n bytes representing an UTF-8 string.
encode_long_string(Value) when erlang:is_list(Value) -> encode_long_string(erlang:list_to_binary(Value));
encode_long_string(Value) when erlang:is_binary(Value) -> encode_long_string(Value, erlang:size(Value)).
encode_long_string(_Value, 0) -> <<0:?ECAS_PROTO_TYPESPEC_INT>>;
encode_long_string(Value, Size) when erlang:is_binary(Value), Size =< ?ECAS_PROTO_MAX_TYPE_INT -> <<Size:?ECAS_PROTO_TYPESPEC_INT, Value/binary>>.

%% @doc Decode a long string.
%% String is a [short] n, followed by n bytes representing an UTF-8 string.
decode_long_string(<<0:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {<<>>, Rest};
decode_long_string(<<Size:?ECAS_PROTO_TYPESPEC_INT, Value:Size/binary, Rest/binary>>) -> {Value, Rest};
decode_long_string(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% uuid
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode uuid
encode_uuid(Value) when erlang:is_integer(Value) -> <<Value:?ECAS_PROTO_TYPESPEC_UUID>>.

%% @doc Decode uuid
decode_uuid(<<Value:?ECAS_PROTO_TYPESPEC_UUID, Rest/binary>>) -> {Value, Rest};
decode_uuid(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% string list
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode string list
encode_string_list(Value) -> encode_string_list(Value, 0, []).
encode_string_list([], Length, Acc) ->
    Binary = erlang:iolist_to_binary(lists:reverse(Acc)),
    <<Length:?ECAS_PROTO_TYPESPEC_SHORT, Binary/binary>>;
encode_string_list([H|T], Length, Acc) ->
    Encoded = encode_string(H),
    encode_string_list(T, Length+1, [Encoded|Acc]).

%% @doc Decode string list
decode_string_list(<<Length:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> decode_string_list(Rest, Length, []).
decode_string_list(Binary, 0, Acc) when is_binary(Binary) -> {lists:reverse(Acc), Binary};
decode_string_list(Binary, Length, Acc) when is_binary(Binary) ->
    case decode_string(Binary) of
        {String, Rest} -> decode_string_list(Rest, Length-1, [String|Acc]);
        Error -> Error
    end;
decode_string_list(_Any, _Length, _Acc) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% bytes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode bytes.
encode_bytes(Value) when erlang:is_list(Value) -> encode_bytes(erlang:list_to_binary(Value));
encode_bytes(Value) when erlang:is_binary(Value) -> encode_bytes(Value, erlang:size(Value)).
encode_bytes(_Value, 0) -> <<0:?ECAS_PROTO_TYPESPEC_INT>>;
encode_bytes(Value, Size) when Size =< ?ECAS_PROTO_MAX_TYPE_INT -> <<Size:?ECAS_PROTO_TYPESPEC_INT, Value/binary>>.

%% @doc Decode bytes.
decode_bytes(<<0:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {<<>>, Rest};
decode_bytes(<<Size:?ECAS_PROTO_TYPESPEC_INT, Value:Size/binary, Rest/binary>>) -> {Value, Rest};
decode_bytes(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% value
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode value.
encode_value(null) -> <<-1:?ECAS_PROTO_TYPESPEC_INT>>;
encode_value('not set') -> <<-2:?ECAS_PROTO_TYPESPEC_INT>>;
encode_value(Value) when erlang:is_binary(Value) -> encode_value(Value, erlang:size(Value));
encode_value(Value) when erlang:is_atom(Value) -> encode_value(erlang:atom_to_binary(Value, utf8));
encode_value(Value) when erlang:is_list(Value) -> encode_value(erlang:list_to_binary(Value)).
encode_value(_Value, 0) -> <<0:?ECAS_PROTO_TYPESPEC_INT>>;
encode_value(Value, Size) when Size =< ?ECAS_PROTO_MAX_TYPE_INT -> <<Size:?ECAS_PROTO_TYPESPEC_INT, Value/binary>>.

%% @doc Decode value.
decode_value(<<-1:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {null, Rest};
decode_value(<<-2:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {'not set', Rest};
decode_value(<<0:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {<<>>, Rest};
decode_value(<<Size:?ECAS_PROTO_TYPESPEC_INT, Value:Size/binary, Rest/binary>>) -> {Value, Rest};
decode_value(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% short bytes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode short bytes.
encode_short_bytes(Value) when erlang:is_binary(Value) -> encode_short_bytes(Value, erlang:size(Value));
encode_short_bytes(Value) when erlang:is_list(Value) -> encode_short_bytes(erlang:list_to_binary(Value)).
encode_short_bytes(_Value, 0) -> <<0:?ECAS_PROTO_TYPESPEC_SHORT>>;
encode_short_bytes(Value, Size) when Size =< ?ECAS_PROTO_TYPESPEC_SHORT -> <<Size:?ECAS_PROTO_TYPESPEC_SHORT, Value/binary>>.

%% @doc Decode short bytes.
decode_short_bytes(<<0:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> {<<>>, Rest};
decode_short_bytes(<<Size:?ECAS_PROTO_TYPESPEC_SHORT, Value:Size/binary, Rest/binary>>) -> {Value, Rest};
decode_short_bytes(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% unsigned vint
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode unsigned vint.
-spec encode_unsigned_vint(non_neg_integer()) -> binary().
encode_unsigned_vint(Value) when erlang:is_integer(Value), Value >= 0, Value =< 127 -> <<Value>>;
encode_unsigned_vint(Value) when erlang:is_integer(Value), Value > 127 -> <<1:1, (Value band 127):7, (encode_unsigned_vint(Value bsr 7))/binary>>.

%% @doc Decode unsigned vint.
%% TODO: Chose between ugly and nice version

%% Ugly version
-spec decode_unsigned_vint(binary()) -> {non_neg_integer(), binary()}.
decode_unsigned_vint(<<255:8/big-unsigned-integer-unit:1, Value:64/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<127:7/big-unsigned-integer-unit:1, Value:57/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<63:6/big-unsigned-integer-unit:1, Value:50/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<31:5/big-unsigned-integer-unit:1, Value:43/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<15:4/big-unsigned-integer-unit:1, Value:36/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<7:3/big-unsigned-integer-unit:1, Value:29/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<3:2/big-unsigned-integer-unit:1, Value:22/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<1:1/big-unsigned-integer-unit:1, Value:15/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(<<Value:8/big-unsigned-integer-unit:1, Rest/binary>>) -> {Value, Rest};
decode_unsigned_vint(_) -> error.

%% Nice version
%% -spec decode_unsigned_vint(binary()) -> {non_neg_integer(), binary()}.
%% decode_unsigned_vint(Binary) -> decode_unsigned_vint(Binary, 0, 0).
%% decode_unsigned_vint(<<1:1, Number:7, Rest/binary>>, Position, Acc) -> decode_unsigned_vint(Rest, Position + 7, (Number bsl Position) + Acc);
%% decode_unsigned_vint(<<0:1, Number:7, Rest/binary>>, Position, Acc) -> {(Number bsl Position) + Acc, Rest}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% vint
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode vint.
encode_vint(Value) when erlang:is_integer(Value), Value >= 0 -> encode_unsigned_vint(Value * 2);
encode_vint(Value) when erlang:is_integer(Value), Value < 0  -> - encode_unsigned_vint((Value * 2) - 1).

%% @doc Decode vint.
decode_vint(Binary) -> decode_vint_zigzag(decode_unsigned_vint(Binary)).
decode_vint_zigzag({Value, Rest}) when Value >= 0, Value rem 2 == 0 -> {Value div 2, Rest};
decode_vint_zigzag({Value, Rest}) when Value >= 0, Value rem 2 == 1 -> - {(Value div 2) - 1, Rest};
decode_vint_zigzag(_) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% inet
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode inet.
encode_inet({Address, Port}) when erlang:is_binary(Address) -> encode_inet(Address, Port);
encode_inet({Address, Port}) when erlang:is_list(Address) -> encode_inet(erlang:list_to_binary(Address), Port).
encode_inet(Address, Port) ->
    Length = erlang:byte_size(Address),
    <<Length:?ECAS_PROTO_TYPESPEC_CHAR, Address/binary, Port:?ECAS_PROTO_TYPESPEC_INT>>.

%% @doc Decode inet.
decode_inet(<<Length:?ECAS_PROTO_TYPESPEC_CHAR, Address:Length/binary, Port:?ECAS_PROTO_TYPESPEC_INT, Rest/binary>>) -> {{Address, Port}, Rest};
decode_inet(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% inetaddr
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode inetaddr.
encode_inetaddr(Address) when erlang:is_binary(Address) ->
    Length = erlang:byte_size(Address),
    <<Length:?ECAS_PROTO_TYPESPEC_CHAR, Address/binary>>;
encode_inetaddr(Address) when erlang:is_list(Address) -> encode_inetaddr(erlang:list_to_binary(Address)).

%% @doc Decode inetaddr.
decode_inetaddr(<<Length:?ECAS_PROTO_TYPESPEC_CHAR, Address:Length/binary, Rest/binary>>) -> {Address, Rest};
decode_inetaddr(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% consistency
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode consistency.
encode_consistency(Value) when erlang:is_integer(Value), Value >= 16#0000, Value =< 16#000A -> <<Value:?ECAS_PROTO_TYPESPEC_SHORT>>;
encode_consistency(any) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_ANY);
encode_consistency(one) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_ONE);
encode_consistency(two) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_TWO);
encode_consistency(three) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_THREE);
encode_consistency(quorum) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_QUORUM);
encode_consistency(all) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_ALL);
encode_consistency(local_quorum) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_LOCAL_QUORUM);
encode_consistency(each_quorum) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_EACH_QUORUM);
encode_consistency(serial) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_SERIAL);
encode_consistency(local_serial) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_LOCAL_SERIAL);
encode_consistency(local_one) -> encode_consistency(?ECAS_PROTO_CONSISTENCY_LOCAL_ONE).

%% @doc Decode consistency.
decode_consistency(<<Value:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) when Value >= 16#0000, Value =< 16#000A -> {Value, Rest};
decode_consistency(_Any) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% string map
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode string map
encode_string_map(Value) when erlang:is_list(Value) -> encode_string_map(Value, 0, []).
encode_string_map([], Length, Acum) ->
    Binary = erlang:iolist_to_binary(lists:reverse(Acum)),
    <<Length:?ECAS_PROTO_TYPESPEC_SHORT, Binary/binary>>;
encode_string_map([{Key, Value}|T], Length, Acum) ->
    EncodedKey   = encode_string(Key),
    EncodedValue = encode_string(Value),
    EncodedMember= <<EncodedKey/binary, EncodedValue/binary>>,
    encode_string_map(T, Length+1, [EncodedMember|Acum]).

%% @doc Decode string map
decode_string_map(<<Length:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> decode_string_map(Rest, Length, []).
decode_string_map(Binary, 0, Acum) -> {lists:reverse(Acum), Binary};
decode_string_map(Binary, Length, Acum) ->
    case decode_string(Binary) of
        {Key, Rest1} ->
            case decode_string(Rest1) of
                {Value, Rest2} -> decode_string_map(Rest2, Length-1, [{Key, Value}|Acum]);
                _ -> error
            end;
        _ -> error
    end;
decode_string_map(_Any, _Length, _Acc) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% string multimap
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode string multimap
encode_string_multimap(Value) when erlang:is_list(Value) -> encode_string_multimap(Value, 0, []).
encode_string_multimap([], Length, Acum) ->
    Binary = erlang:iolist_to_binary(lists:reverse(Acum)),
    <<Length:?ECAS_PROTO_TYPESPEC_SHORT, Binary/binary>>;
encode_string_multimap([{Key, Value}|T], Length, Acum) ->
    EncodedKey   = encode_string(Key),
    EncodedValue = encode_string(Value),
    EncodedMember= <<EncodedKey/binary, EncodedValue/binary>>,
    encode_string_multimap(T, Length+1, [EncodedMember|Acum]).

%% @doc Decode string multimap
decode_string_multimap(<<Length:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> decode_string_multimap(Rest, Length, []).
decode_string_multimap(Binary, 0, Acum) -> {lists:reverse(Acum), Binary};
decode_string_multimap(Binary, Length, Acum) ->
    case decode_string(Binary) of
        {Key, Rest1} ->
            case decode_string_list(Rest1) of
                {Value, Rest2} -> decode_string_multimap(Rest2, Length-1, [{Key, Value}|Acum]);
                _ -> error
            end;
        _ -> error
    end;
decode_string_multimap(_Any, _Length, _Acc) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% bytes map
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode bytes map
encode_bytes_map(Value) when erlang:is_list(Value) -> encode_bytes_map(Value, 0, []).
encode_bytes_map([], Length, Acum) ->
    Binary = erlang:iolist_to_binary(lists:reverse(Acum)),
    <<Length:?ECAS_PROTO_TYPESPEC_SHORT, Binary/binary>>;
encode_bytes_map([{Key, Value}|T], Length, Acum) ->
    EncodedKey   = encode_string(Key),
    EncodedValue = decode_bytes(Value),
    EncodedMember= <<EncodedKey/binary, EncodedValue/binary>>,
    encode_bytes_map(T, Length+1, [EncodedMember|Acum]).

%% @doc Decode bytes map
decode_bytes_map(<<Length:?ECAS_PROTO_TYPESPEC_SHORT, Rest/binary>>) -> decode_bytes_map(Rest, Length, []).
decode_bytes_map(Binary, 0, Acum) -> {lists:reverse(Acum), Binary};
decode_bytes_map(Binary, Length, Acum) ->
    case decode_string(Binary) of
        {Key, Rest1} ->
            case decode_bytes(Rest1) of
                {Value, Rest2} -> decode_bytes_map(Rest2, Length-1, [{Key, Value}|Acum]);
                _ -> error
            end;
        _ -> error
    end;
decode_bytes_map(_Any, _Length, _Acc) -> error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% utils
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

