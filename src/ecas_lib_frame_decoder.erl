%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. May 2017 10:59 AM
%%%-------------------------------------------------------------------
-module(ecas_lib_frame_decoder).
-author("madalin").

-include_lib("ecas/include/ecas_protocol.hrl").

%% API
-export([decode/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% decode
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Decode the specified frame header first
decode(<<Direction:1, Version:7/big-unsigned-integer-unit:1,
         _UnusedFlags:3, UseBetaFlag:1, WarningFlag:1, CustomPayloadFlag:1, TracingFlag:1, CompressionFlag:1,
         StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
         Opcode:?ECAS_PROTO_TYPESPEC_OPCODE>>,
       Body,
       ProtocolState) ->

    FrameHeader = #ecas_frame_header{
        direction           = Direction,
        version             = Version,
        flag_use_beta       = UseBetaFlag,
        flag_warning        = WarningFlag,
        flag_custom_payload = CustomPayloadFlag,
        flag_tracing        = TracingFlag,
        flag_compression    = CompressionFlag,
        stream_id           = StreamId,
        opcode              = Opcode
    },

    %% Decode body
    decode_body(FrameHeader, Body, ProtocolState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% decode body
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Decode the frame body acording to the frame header

%% Note to client implementors: client libraries should always assume that the
%% body of a given frame may contain more data than what is described in this
%% document. It will however always be safe to ignore the remainder of the frame
%% body in such cases. The reason is that this may enable extending the protocol
%% with optional features without needing to change the protocol version.

%% OPCODE_ERROR
%%
%% == V3 ==
%% Indicates an error processing a request. The body of the message will be an
%% error code ([int]) followed by a [string] error message. Then, depending on
%% the exception, more content may follow. The error codes are defined in
%% Section 9, along with their additional content if any.
%%
%% == V4 ==
%% Indicates an error processing a request. The body of the message will be an
%% error code ([int]) followed by a [string] error message. Then, depending on
%% the exception, more content may follow. The error codes are defined in
%% Section 9, along with their additional content if any.
%%
%% == V5 ==
%% Indicates an error processing a request. The body of the message will be an
%% error code ([int]) followed by a [string] error message. Then, depending on
%% the exception, more content may follow. The error codes are defined in
%% Section 9, along with their additional content if any.
%%
%% Changes between V3, V4 and V5 only involve section 9.
decode_body(#ecas_frame_header{
        opcode = ?ECAS_PROTO_HEADER_OPCODE_ERROR
    },
            <<ErrorCode:?ECAS_PROTO_TYPESPEC_INT, ErrorMessageSize:?ECAS_PROTO_TYPESPEC_SHORT, ErrorMessage:ErrorMessageSize/binary, Rest/binary>>,
            _ProtocolState) ->

    case decode_body_error(ErrorCode, Rest) of
        {ok, More, Rest} ->
            {ok, #ecas_frame_body_error{
                code    = ErrorCode,
                message = ErrorMessage,
                more    = More,
                rest    = Rest
            }};
        _ -> {error, invalid_frame_error}
    end;

%% OPCODE_STARTUP
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_STARTUP}, Body, _ProtocolState) -> ok;

%% OPCODE_READY
%% Indicates that the server is ready to process queries. This message will be
%% sent by the server either after a STARTUP message if no authentication is
%% required (if authentication is required, the server indicates readiness by
%% sending a AUTH_RESPONSE message).
%%
%% The body of a READY message is empty.
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_READY}, _Body, _ProtocolState) -> {ok, empty};

%% OPCODE_AUTHENTICATE
%%
%% == V5 ==
%%
%% Indicates that the server requires authentication, and which authentication
%% mechanism to use.
%%
%% The authentication is SASL based and thus consists of a number of server
%% challenges (AUTH_CHALLENGE, Section 4.2.7) followed by client responses
%% (AUTH_RESPONSE, Section 4.1.2). The initial exchange is however boostrapped
%% by an initial client response. The details of that exchange (including how
%% many challenge-response pairs are required) are specific to the authenticator
%% in use. The exchange ends when the server sends an AUTH_SUCCESS message or
%% an ERROR message.
%%
%% This message will be sent following a STARTUP message if authentication is
%% required and must be answered by a AUTH_RESPONSE message from the client.
%%
%% The body consists of a single [string] indicating the full class name of the
%% IAuthenticator in use.
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_AUTHENTICATE}, Body, _ProtocolState) ->

    case ecas_lib_frame_types:decode_string(Body) of
        {Value, _} -> {ok, #ecas_frame_body_authenticate{authenticator_class_name = Value}};
        _ -> {error, invalid_authenticate_frame}
    end;

%% OPCODE_OPTIONS
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_OPTIONS}, Body, _ProtocolState) -> ok;

%% OPCODE_SUPPORTED
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_SUPPORTED}, Body, _ProtocolState) -> ok;

%% OPCODE_QUERY
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_QUERY}, Body, _ProtocolState) -> ok;

%% OPCODE_RESULT
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_RESULT}, Body, _ProtocolState) -> ok;

%% OPCODE_PREPARE
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_PREPARE}, Body, _ProtocolState) -> ok;

%% OPCODE_EXECUTE
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_EXECUTE}, Body, _ProtocolState) -> ok;

%% OPCODE_REGISTER
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_REGISTER}, Body, _ProtocolState) -> ok;

%% OPCODE_EVENT
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_EVENT}, Body, _ProtocolState) -> ok;

%% OPCODE_BATCH
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_BATCH}, Body, _ProtocolState) -> ok;

%% OPCODE_AUTH_CHALLENGE
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_AUTH_CHALLENGE}, Body, _ProtocolState) -> ok;

%% OPCODE_AUTH_RESPONSE
%% This is a client request and will never be send by server
%% decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE}, Body, _ProtocolState) -> ok;

%% OPCODE_AUTH_SUCCESS
decode_body(#ecas_frame_header{opcode = ?ECAS_PROTO_HEADER_OPCODE_AUTH_SUCCESS}, Body, _ProtocolState) -> ok;

%% UNKNOWN
decode_body(_FrameHeader, _Body, _ProtocolState) -> {error, unknown_frame_header_opcode}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% decode body error
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Server error: something unexpected happened. This indicates a server-side bug.
%% ?ECAS_PROTO_ERROR_CODE_SERVER_ERROR

%% Protocol error: some client message triggered a protocol violation
%% (for instance a QUERY message is sent before a STARTUP one has been sent)
%% ?ECAS_PROTO_ERROR_CODE_PROTOCOL_ERROR

%% Authentication error: authentication was required and failed.
%% The possible reason for failing depends on the authenticator in use,
%% which may or may not include more detail in the accompanying error message.
%% ?ECAS_PROTO_ERROR_CODE_AUTHENTICATION_ERROR

%% Unavailable exception.
%% The rest of the ERROR message body will be <cl><required><alive> where:
%% <cl> is the [consistency] level of the query that triggered the exception.
%% <required> is an [int] representing the number of nodes that should be alive to respect <cl>
%% <alive> is an [int] representing the number of replicas that were known to be alive when the request had been
%% processed (since an unavailable exception has been triggered, there will be <alive> < <required>)
decode_body_error(?ECAS_PROTO_ERROR_CODE_UNAVAILABLE_EXCEPTION, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                                  {cl, consistency},
                                                  {required, int},
                                                  {alive, int}], Rest);

%% Overloaded: the request cannot be processed because the coordinator node is overloaded
%% ?ECAS_PROTO_ERROR_CODE_OVERLOADED

%% Is_bootstrapping: the request was a read request but the coordinator node is bootstrapping
%% ?ECAS_PROTO_ERROR_CODE_OVERLOADED

%% Truncate_error: error during a truncation error.
%% ?ECAS_PROTO_ERROR_CODE_TRUNCATE_ERROR

%% Write_timeout: Timeout exception during a write request.
%% The rest of the ERROR message body will be <cl><received><blockfor><writeType> where:
%% <cl> is the [consistency] level of the query having triggered the exception.
%% <received> is an [int] representing the number of nodes having acknowledged the request.
%% <blockfor> is an [int] representing the number of replicas whose acknowledgement is required to achieve <cl>.
%% <writeType> is a [string] that describe the type of the write that timed out. The value of that string can be one
%% of:
%% - "SIMPLE": the write was a non-batched non-counter write.
%% - "BATCH": the write was a (logged) batch write. If this type is received, it means the batch log has been successfully written (otherwise a "BATCH_LOG" type would have been sent instead).
%% - "UNLOGGED_BATCH": the write was an unlogged batch. No batch log write has been attempted.
%% - "COUNTER": the write was a counter write (batched or not).
%% - "BATCH_LOG": the timeout occurred during the write to the batch log when a (logged) batch write was requested.
decode_body_error(?ECAS_PROTO_ERROR_CODE_WRITE_TIMEOUT, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                                  {cl, consistency},
                                                  {received, int},
                                                  {blockfor, int},
                                                  {writeType, string}
                                              ], Rest);

%% Read_timeout: Timeout exception during a read request.
%% The rest of the ERROR message body will be <cl><received><blockfor><data_present> where:
%% <cl> is the [consistency] level of the query having triggered the exception.
%% <received> is an [int] representing the number of nodes having answered the request.
%% <blockfor> is an [int] representing the number of replicas whose response is required to achieve <cl>.
%% Please note that it is possible to have <received> >= <blockfor> if <data_present> is false. Also in the (unlikely)
%% case where <cl> is achieved but the coordinator node times out while waiting for read-repair acknowledgement.
%% <data_present> is a single byte. If its value is 0, it means the replica that was asked for data has not
%% responded. Otherwise, the value is != 0.
decode_body_error(?ECAS_PROTO_ERROR_CODE_READ_TIMEOUT, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                                  {cl, consistency},
                                                  {received, int},
                                                  {blockfor, int},
                                                  {data_present, byte}
                                              ], Rest);

%% Read_failure: A non-timeout exception during a read request.
%% The rest of the ERROR message body will be <cl><received><blockfor><reasonmap><data_present> where:
%% <cl> is the [consistency] level of the query having triggered the exception.
%% <received> is an [int] representing the number of nodes having answered the request.
%% <blockfor> is an [int] representing the number of replicas whose acknowledgement is required to achieve <cl>.
%% <reasonmap> is a map of endpoint to failure reason codes. This maps the endpoints of the replica nodes that failed when
%% executing the request to a code representing the reason for the failure. The map is encoded starting with an [int] n
%% followed by n pairs of <endpoint><failurecode> where <endpoint> is an [inetaddr] and <failurecode> is a [short].
%% <data_present> is a single byte. If its value is 0, it means the replica that was asked for data had not
%% responded. Otherwise, the value is != 0.
decode_body_error(?ECAS_PROTO_ERROR_CODE_READ_FAILURE, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                                  {cl, consistency},
                                                  {received, int},
                                                  {blockfor, int},
                                                  {data_present, byte}
                                              ], Rest);

%% Function_failure: A (user defined) function failed during execution.
%% The rest of the ERROR message body will be <keyspace><function><arg_types> where:
%% <keyspace> is the keyspace [string] of the failed function
%% <function> is the name [string] of the failed function
%% <arg_types> [string list] one string for each argument type (as CQL type) of the failed function
decode_body_error(?ECAS_PROTO_ERROR_CODE_FUNCTION_FAILURE, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                                  {keyspace, string},
                                                  {function, string},
                                                  {arg_types, string_list}
                                              ], Rest);

%% Write_failure: A non-timeout exception during a write request. The rest of the ERROR message body will be
%% <cl><received><blockfor><reasonmap><write_type>
%% where:
%% <cl> is the [consistency] level of the query having triggered the exception.
%% <received> is an [int] representing the number of nodes having answered the request.
%% <blockfor> is an [int] representing the number of replicas whose acknowledgement is required to achieve <cl>.
%% <reasonmap> is a map of endpoint to failure reason codes. This maps
%% the endpoints of the replica nodes that failed when
%% executing the request to a code representing the reason
%% for the failure. The map is encoded starting with an [int] n
%% followed by n pairs of <endpoint><failurecode> where
%% <endpoint> is an [inetaddr] and <failurecode> is a [short].
%% <writeType> is a [string] that describes the type of the write
%% that failed. The value of that string can be one
%% of:
%% - "SIMPLE": the write was a non-batched non-counter write.
%% - "BATCH": the write was a (logged) batch write. If this type is received,
%% it means the batch log has been successfully written (otherwise a "BATCH_LOG" type would have been sent instead).
%% - "UNLOGGED_BATCH": the write was an unlogged batch. No batch log write has been attempted.
%% - "COUNTER": the write was a counter write (batched or not).
%% - "BATCH_LOG": the failure occured during the write to the batch log when a (logged) batch write was requested.
decode_body_error(?ECAS_PROTO_ERROR_CODE_WRITE_FAILURE, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                          {cl, consistency},
                                          {received, int},
                                          {blockfor, int},
                                          {data_present, byte}
                                      ], Rest);

%% Syntax_error: The submitted query has a syntax error.
%% ?ECAS_PROTO_ERROR_CODE_SYNTAX_ERROR

%% Unauthorized: The logged user doesn't have the right to perform the query.
%% ?ECAS_PROTO_ERROR_CODE_UNAUTHORIZED

%% Invalid: The query is syntactically correct but invalid.
%% ?ECAS_PROTO_ERROR_CODE_INVALID

%% Config_error: The query is invalid because of some configuration issue
%% ?ECAS_PROTO_ERROR_CODE_CONFIG_ERROR

%% Already_exists: The query attempted to create a keyspace or a table that was already existing.
%% The rest of the ERROR message body will be <ks><table> where:
%%                 <ks> is a [string] representing either the keyspace that
%%                      already exists, or the keyspace in which the table that
%%                      already exists is.
%%                 <table> is a [string] representing the name of the table that
%%                         already exists. If the query was attempting to create a
%%                         keyspace, <table> will be present but will be the empty
%%                         string.
decode_body_error(?ECAS_PROTO_ERROR_CODE_ALREADY_EXISTS, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                          {ks, string},
                                          {table, string}
                                      ], Rest);

%% Unprepared: Can be thrown while a prepared statement tries to be executed if the provided prepared statement ID is not known by this host.
%% The rest of the ERROR message body will be [short bytes] representing the unknown ID.
decode_body_error(?ECAS_PROTO_ERROR_CODE_UNPREPARED, Rest) ->

    ecas_lib_frame_types:decode_specs([
                                          {unknown_id, short_bytes}
                                      ], Rest);

%% Give driver a chance to survive unexpected and handle any other error code
decode_body_error(_, Rest) -> {ok, [], Rest}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% decode body ready
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

