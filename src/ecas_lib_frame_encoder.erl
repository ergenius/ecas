%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jun 2017 2:23 PM
%%%-------------------------------------------------------------------
-module(ecas_lib_frame_encoder).
-author("madalin").

-include_lib("ecas/include/ecas_protocol.hrl").

%% API
-export([encode/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% encode
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Encode the specified frame according to the specified protocol state

%% OPTIONS
%%
%% Asks the server to return which STARTUP options are supported. The body of an
%% OPTIONS message should be empty and the server will respond with a SUPPORTED
%% message.
encode(options, #ecas_protocol_state{
    stream_id = StreamId
}) ->

    %% Request options with the higher protocol version we know (v5)
    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_OPTIONS:?ECAS_PROTO_TYPESPEC_OPCODE,
           0:?ECAS_PROTO_TYPESPEC_INT>>};

%% STARTUP
%%
%% Initialize the connection. The server will respond by either a READY message
%% (in which case the connection is ready for queries) or an AUTHENTICATE message
%% (in which case credentials will need to be provided using AUTH_RESPONSE).
%%
%% This must be the first message of the connection, except for OPTIONS that can
%% be sent before to find out the options supported by the server. Once the
%% connection has been initialized, a client should not send any more STARTUP
%% messages.
%%
%% The body is a [string map] of options. Possible options are:
%% - "CQL_VERSION": the version of CQL to use. This option is mandatory and
%% currently the only version supported is "3.0.0". Note that this is
%% different from the protocol version.
%% - "COMPRESSION": the compression algorithm to use for frames (See section 5).
%% This is optional; if not specified no compression will be used.
encode(startup, #ecas_protocol_state{
    cql_protocol_version    = ProtocolVersion,
    stream_id               = StreamId
}) ->

    Body        = ecas_lib_frame_types:encode_string_map([{<<"CQL_VERSION">>, ProtocolVersion}]),
    BodyLength  = erlang:byte_size(Body),
    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_OPTIONS:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% AUTH_RESPONSE
%%
%% Answers a server authentication challenge.
%%
%% Authentication in the protocol is SASL based. The server sends authentication
%% challenges (a bytes token) to which the client answers with this message. Those
%% exchanges continue until the server accepts the authentication by sending a
%% AUTH_SUCCESS message after a client AUTH_RESPONSE. Note that the exchange
%% begins with the client sending an initial AUTH_RESPONSE in response to a
%% server AUTHENTICATE request.
%%
%% The body of this message is a single [bytes] token. The details of what this
%% token contains (and when it can be null/empty, if ever) depends on the actual
%% authenticator used.
%%
%% The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
%% an AUTH_SUCCESS message or an ERROR message.
encode({auth_response, Token}, #ecas_protocol_state{
    stream_id               = StreamId
}) ->

    Body        = ecas_lib_frame_types:encode_bytes(Token),
    BodyLength  = erlang:byte_size(Body),
    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% QUERY
%%
%% Performs a CQL query. The body of the message must be:
%% <query><query_parameters>
%% where <query> is a [long string] representing the query and
%% <query_parameters> must be
%% <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
%% where:
%% - <consistency> is the [consistency] level for the operation.
%% - <flags> is a [byte] whose bits define the options for this query and
%% in particular influence what the remainder of the message contains.
%%
%% A flag is set if the bit corresponding to its `mask` is set. Supported
%% flags are, given their mask:
%%
%% 0x01: Values. If set, a [short] <n> followed by <n> [value]
%% values are provided. Those values are used for bound variables in
%% the query. Optionally, if the 0x40 flag is present, each value
%% will be preceded by a [string] name, representing the name of
%% the marker the value must be bound to.
%%
%% 0x02: Skip_metadata. If set, the Result Set returned as a response
%% to the query (if any) will have the NO_METADATA flag (see
%% Section 4.2.5.2).
%%
%% 0x04: Page_size. If set, <result_page_size> is an [int]
%% controlling the desired page size of the result (in CQL3 rows).
%% See the section on paging (Section 8) for more details.
%%
%% 0x08: With_paging_state. If set, <paging_state> should be present.
%% <paging_state> is a [bytes] value that should have been returned
%% in a result set (Section 4.2.5.2). The query will be
%% executed but starting from a given paging state. This is also to
%% continue paging on a different node than the one where it
%% started (See Section 8 for more details).
%%
%% 0x10: With serial consistency. If set, <serial_consistency> should be
%% present. <serial_consistency> is the [consistency] level for the
%% serial phase of conditional updates. That consitency can only be
%% either SERIAL or LOCAL_SERIAL and if not present, it defaults to
%% SERIAL. This option will be ignored for anything else other than a
%% conditional update/insert.
%%
%% 0x20: With default timestamp. If set, <timestamp> should be present.
%% <timestamp> is a [long] representing the default timestamp for the query
%% in microseconds (negative values are forbidden). This will
%% replace the server side assigned timestamp as default timestamp.
%% Note that a timestamp in the query itself will still override
%% this timestamp. This is entirely optional.
%%
%% 0x40: With names for values. This only makes sense if the 0x01 flag is set and
%% is ignored otherwise. If present, the values from the 0x01 flag will
%% be preceded by a name (see above). Note that this is only useful for
%% QUERY requests where named bind markers are used; for EXECUTE statements,
%% since the names for the expected values was returned during preparation,
%% a client can always provide values in the right order without any names
%% and using this flag, while supported, is almost surely inefficient.
%%
%% Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
%% TRUNCATE, ...).
%%
%% The server will respond to a QUERY message with a RESULT message, the content
%% of which depends on the query.
encode({query, #ecas_query{query = Query, parameters = Parameters}}, #ecas_protocol_state{
    stream_id               = StreamId
}) ->

    BodyQuery           = ecas_lib_frame_types:encode_long_string(Query),
    BodyQueryParameters = encode_query_parameters(Parameters),
    Body                = <<BodyQuery/binary, BodyQueryParameters/binary>>,
    BodyLength          = erlang:byte_size(Body),

    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% PREPARE
%%
%% Prepare a query for later execution (through EXECUTE). The body consists of
%% the CQL query to prepare as a [long string].
%%
%% The server will respond with a RESULT message with a `prepared` kind (0x0004,
%% see Section 4.2.5).
encode({prepare, #ecas_query{query = Query}}, #ecas_protocol_state{
    stream_id               = StreamId
}) ->

    Body        = ecas_lib_frame_types:encode_long_string(Query),
    BodyLength  = erlang:byte_size(Body),
    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% EXECUTE
%%
%% Executes a prepared query. The body of the message must be:
%% <id><query_parameters> where <id> is the prepared query ID. It's the [short bytes] returned as a
%% response to a PREPARE message. As for <query_parameters>, it has the exact same definition as in QUERY (see Section 4.1.4).
%%
%% The response from the server will be a RESULT message.
encode({execute, #ecas_query{prepared_id = PreparedId, parameters = Parameters}}, #ecas_protocol_state{stream_id = StreamId}) ->

    BodyPreparedId      = ecas_lib_frame_types:encode_short_bytes(PreparedId),
    BodyQueryParameters = encode_query_parameters(Parameters),
    Body                = <<BodyPreparedId/binary, BodyQueryParameters/binary>>,
    BodyLength          = erlang:byte_size(Body),

    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% BATCH
%%
%% Allows executing a list of queries (prepared or not) as a batch (note that
%% only DML statements are accepted in a batch). The body of the message must
%% be:
%% <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
%% where:
%%
%% - <type> is a [byte] indicating the type of batch to use:
%% - If <type> == 0, the batch will be "logged". This is equivalent to a
%% normal CQL3 batch statement.
%% - If <type> == 1, the batch will be "unlogged".
%% - If <type> == 2, the batch will be a "counter" batch (and non-counter
%% statements will be rejected).
%%
%% - <flags> is a [byte] whose bits define the options for this query and
%% in particular influence what the remainder of the message contains. It is similar
%% to the <flags> from QUERY and EXECUTE methods, except that the 4 rightmost
%% bits must always be 0 as their corresponding options do not make sense for
%% Batch. A flag is set if the bit corresponding to its `mask` is set. Supported
%% flags are, given their mask:
%%
%% 0x10: With serial consistency. If set, <serial_consistency> should be
%% present. <serial_consistency> is the [consistency] level for the
%% serial phase of conditional updates. That consistency can only be
%% either SERIAL or LOCAL_SERIAL and if not present, it defaults to
%% SERIAL. This option will be ignored for anything else other than a
%% conditional update/insert.
%%
%% 0x20: With default timestamp. If set, <timestamp> should be present.
%% <timestamp> is a [long] representing the default timestamp for the query
%% in microseconds. This will replace the server side assigned
%% timestamp as default timestamp. Note that a timestamp in the query itself
%% will still override this timestamp. This is entirely optional.
%%
%% 0x40: With names for values. If set, then all values for all <query_i> must be
%% preceded by a [string] <name_i> that have the same meaning as in QUERY
%% requests [IMPORTANT NOTE: this feature does not work and should not be
%% used. It is specified in a way that makes it impossible for the server
%% to implement. This will be fixed in a future version of the native
%% protocol. See https://issues.apache.org/jira/browse/CASSANDRA-10246 for
%% more details].
%%
%% - <n> is a [short] indicating the number of following queries.
%% - <query_1>...<query_n> are the queries to execute. A <query_i> must be of the
%% form:
%% <kind><string_or_id><n>[<name_1>]<value_1>...[<name_n>]<value_n>
%% where:
%% - <kind> is a [byte] indicating whether the following query is a prepared
%% one or not. <kind> value must be either 0 or 1.
%% - <string_or_id> depends on the value of <kind>. If <kind> == 0, it should be
%% a [long string] query string (as in QUERY, the query string might contain
%% bind markers). Otherwise (that is, if <kind> == 1), it should be a
%% [short bytes] representing a prepared query ID.
%% - <n> is a [short] indicating the number (possibly 0) of following values.
%% - <name_i> is the optional name of the following <value_i>. It must be present
%% if and only if the 0x40 flag is provided for the batch.
%% - <value_i> is the [value] to use for bound variable i (of bound variable <name_i>
%% if the 0x40 flag is used).
%% - <consistency> is the [consistency] level for the operation.
%% - <serial_consistency> is only present if the 0x10 flag is set. In that case,
%% <serial_consistency> is the [consistency] level for the serial phase of
%% conditional updates. That consitency can only be either SERIAL or
%% LOCAL_SERIAL and if not present will defaults to SERIAL. This option will
%% be ignored for anything else other than a conditional update/insert.
%%
%% The server will respond with a RESULT message.
encode({batch, #ecas_query_batch{type = BatchType}}, #ecas_protocol_state{stream_id = StreamId}) ->

    BodyPreparedId      = ecas_lib_frame_types:encode_short_bytes(PreparedId),
    BodyQueryParameters = encode_query_parameters(Parameters),
    Body                = <<BodyPreparedId/binary, BodyQueryParameters/binary>>,
    BodyLength          = erlang:byte_size(Body),

    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>};

%% REGISTER
%%
%% Register this connection to receive some types of events. The body of the
%% message is a [string list] representing the event types to register for. See
%% section 4.2.6 for the list of valid event types.
%%
%% The response to a REGISTER message will be a READY message.
%%
%% Please note that if a client driver maintains multiple connections to a
%% Cassandra node and/or connections to multiple nodes, it is advised to
%% dedicate a handful of connections to receive events, but to *not* register
%% for events on all connections, as this would only result in receiving
%% multiple times the same event messages, wasting bandwidth.
encode({register, EventsTypeList}, #ecas_protocol_state{stream_id = StreamId}) ->

    Body        = ecas_lib_frame_types:encode_string_list(EventsTypeList),
    BodyLength  = erlang:byte_size(Body),

    {ok, <<0:1, 4:7/big-unsigned-integer-unit:1, 0:3, 1:1, 0:1, 0:1, 0:1, 0:1,
           StreamId:?ECAS_PROTO_TYPESPEC_STREAM,
           ?ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE:?ECAS_PROTO_TYPESPEC_OPCODE,
           BodyLength:?ECAS_PROTO_TYPESPEC_INT,
           Body/binary>>}.