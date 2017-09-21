%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. May 2017 3:04 PM
%%%-------------------------------------------------------------------
-author("madalin").

-include("ecas.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ETS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% ETS storing prepared statements
-define(ECAS_ETS_PREPARED_STATEMENTS,  ecas_ets_prepared_statements).

%% List containing all ETS that must be created when Q starts
%% ETS are optimized based on their usage. Benchmarks where done to pick up the best settings.
%% If you add any new Q ETS please add it to this table and the ETS will be automatically created
%% when Q starts.
-define(ECAS_ETS_LIST, [
    {?ECAS_ETS_PREPARED_STATEMENTS,            [set, named_table, public, {read_concurrency, true}]}
]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% PROTOCOL RELATED
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Supported protocol versions
-define(ECAS_PROTO_VERSION_3,           3).
-define(ECAS_PROTO_VERSION_4,           4).
-define(ECAS_PROTO_VERSION_5,           5).

%% Protocol binary type specifications
-define(ECAS_PROTO_TYPESPEC_CHAR,       8/big-unsigned-integer).
-define(ECAS_PROTO_TYPESPEC_SHORT,      16/big-unsigned-integer).
-define(ECAS_PROTO_TYPESPEC_INT,        32/big-signed-integer).
-define(ECAS_PROTO_TYPESPEC_LONG,       64/big-signed-integer).
-define(ECAS_PROTO_TYPESPEC_UUID,       16/big-signed-integer).

-define(ECAS_PROTO_MAX_TYPE_SHORT,      65535).
-define(ECAS_PROTO_MAX_TYPE_INT,        65535).

-define(ECAS_PROTO_TYPESPEC_HEADER,     5/binary).
-define(ECAS_PROTO_TYPESPEC_VERSION,    ?ECAS_PROTO_TYPESPEC_CHAR).
-define(ECAS_PROTO_TYPESPEC_FLAGS,      ?ECAS_PROTO_TYPESPEC_CHAR).
-define(ECAS_PROTO_TYPESPEC_STREAM,     ?ECAS_PROTO_TYPESPEC_SHORT).
-define(ECAS_PROTO_TYPESPEC_OPCODE,     ?ECAS_PROTO_TYPESPEC_CHAR).

%% 2.4. opcode
%% An integer byte that distinguishes the actual message
%% Note that there is no 0x04 message in version 3, 4 or 5 of the protocol
%% (0x04 is reserved).
-define(ECAS_PROTO_HEADER_OPCODE_ERROR,             16#00).
-define(ECAS_PROTO_HEADER_OPCODE_STARTUP,           16#01).
-define(ECAS_PROTO_HEADER_OPCODE_READY,             16#02).
-define(ECAS_PROTO_HEADER_OPCODE_AUTHENTICATE,      16#03).
-define(ECAS_PROTO_HEADER_OPCODE_OPTIONS,           16#05).
-define(ECAS_PROTO_HEADER_OPCODE_SUPPORTED,         16#06).
-define(ECAS_PROTO_HEADER_OPCODE_QUERY,             16#07).
-define(ECAS_PROTO_HEADER_OPCODE_RESULT,            16#08).
-define(ECAS_PROTO_HEADER_OPCODE_PREPARE,           16#09).
-define(ECAS_PROTO_HEADER_OPCODE_EXECUTE,           16#0A).
-define(ECAS_PROTO_HEADER_OPCODE_REGISTER,          16#0B).
-define(ECAS_PROTO_HEADER_OPCODE_EVENT,             16#0C).
-define(ECAS_PROTO_HEADER_OPCODE_BATCH,             16#0D).
-define(ECAS_PROTO_HEADER_OPCODE_AUTH_CHALLENGE,    16#0E).
-define(ECAS_PROTO_HEADER_OPCODE_AUTH_RESPONSE,     16#0F).
-define(ECAS_PROTO_HEADER_OPCODE_AUTH_SUCCESS,      16#10).

%% 3. Notations
-define(ECAS_PROTO_CONSISTENCY_ANY,                 16#0000).
-define(ECAS_PROTO_CONSISTENCY_ONE,                 16#0001).
-define(ECAS_PROTO_CONSISTENCY_TWO,                 16#0002).
-define(ECAS_PROTO_CONSISTENCY_THREE,               16#0003).
-define(ECAS_PROTO_CONSISTENCY_QUORUM,              16#0004).
-define(ECAS_PROTO_CONSISTENCY_ALL,                 16#0005).
-define(ECAS_PROTO_CONSISTENCY_LOCAL_QUORUM,        16#0006).
-define(ECAS_PROTO_CONSISTENCY_EACH_QUORUM,         16#0007).
-define(ECAS_PROTO_CONSISTENCY_SERIAL,              16#0008).
-define(ECAS_PROTO_CONSISTENCY_LOCAL_SERIAL,        16#0009).
-define(ECAS_PROTO_CONSISTENCY_LOCAL_ONE,           16#000A).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 9. Error codes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Server error: something unexpected happened. This indicates a server-side bug.
-define(ECAS_PROTO_ERROR_CODE_SERVER_ERROR,             16#0000).

%% Protocol error: some client message triggered a protocol violation
%% (for instance a QUERY message is sent before a STARTUP one has been sent)
-define(ECAS_PROTO_ERROR_CODE_PROTOCOL_ERROR,           16#000A).

%% Authentication error: authentication was required and failed.
%% The possible reason for failing depends on the authenticator in use,
%% which may or may not include more detail in the accompanying error message.
-define(ECAS_PROTO_ERROR_CODE_AUTHENTICATION_ERROR,     16#0100).

%% Unavailable exception.
%% The rest of the ERROR message body will be <cl><required><alive> where:
%% <cl> is the [consistency] level of the query that triggered the exception.
%% <required> is an [int] representing the number of nodes that should be alive to respect <cl>
%% <alive> is an [int] representing the number of replicas that were known to be alive when the request had been
%% processed (since an unavailable exception has been triggered, there will be <alive> < <required>)
-define(ECAS_PROTO_ERROR_CODE_UNAVAILABLE_EXCEPTION,    16#1000).

%% Overloaded: the request cannot be processed because the coordinator node is overloaded
-define(ECAS_PROTO_ERROR_CODE_OVERLOADED,               16#1001).

%% Is_bootstrapping: the request was a read request but the coordinator node is bootstrapping
-define(ECAS_PROTO_ERROR_CODE_OVERLOADED,               16#1002).

%% Truncate_error: error during a truncation error.
-define(ECAS_PROTO_ERROR_CODE_TRUNCATE_ERROR,           16#1003).

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
-define(ECAS_PROTO_ERROR_CODE_WRITE_TIMEOUT,            16#1100).

%% Read_timeout: Timeout exception during a read request.
%% The rest of the ERROR message body will be <cl><received><blockfor><data_present> where:
%% <cl> is the [consistency] level of the query having triggered the exception.
%% <received> is an [int] representing the number of nodes having answered the request.
%% <blockfor> is an [int] representing the number of replicas whose response is required to achieve <cl>.
%% Please note that it is possible to have <received> >= <blockfor> if <data_present> is false. Also in the (unlikely)
%% case where <cl> is achieved but the coordinator node times out while waiting for read-repair acknowledgement.
%% <data_present> is a single byte. If its value is 0, it means the replica that was asked for data has not
%% responded. Otherwise, the value is != 0.
-define(ECAS_PROTO_ERROR_CODE_READ_TIMEOUT,             16#1200).

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
-define(ECAS_PROTO_ERROR_CODE_READ_FAILURE,             16#1300).

%% Function_failure: A (user defined) function failed during execution.
%% The rest of the ERROR message body will be <keyspace><function><arg_types> where:
%% <keyspace> is the keyspace [string] of the failed function
%% <function> is the name [string] of the failed function
%% <arg_types> [string list] one string for each argument type (as CQL type) of the failed function
-define(ECAS_PROTO_ERROR_CODE_FUNCTION_FAILURE,         16#1400).

%% Write_failure: A non-timeout exception during a write request. The rest of the ERROR message body will be
%% <cl><received><blockfor><reasonmap><write_type>
%% where:
%% <cl> is the [consistency] level of the query having triggered
%% the exception.
%% <received> is an [int] representing the number of nodes having
%% answered the request.
%% <blockfor> is an [int] representing the number of replicas whose
%% acknowledgement is required to achieve <cl>.
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
-define(ECAS_PROTO_ERROR_CODE_WRITE_FAILURE,            16#1500).

%% Syntax_error: The submitted query has a syntax error.
-define(ECAS_PROTO_ERROR_CODE_SYNTAX_ERROR,             16#2000).

%% Unauthorized: The logged user doesn't have the right to perform the query.
-define(ECAS_PROTO_ERROR_CODE_UNAUTHORIZED,             16#2100).

%% Invalid: The query is syntactically correct but invalid.
-define(ECAS_PROTO_ERROR_CODE_INVALID,                  16#2200).

%% Config_error: The query is invalid because of some configuration issue
-define(ECAS_PROTO_ERROR_CODE_CONFIG_ERROR,             16#2300).

%% Already_exists: The query attempted to create a keyspace or a table that was already existing.
%% The rest of the ERROR message body will be <ks><table> where:
%%                 <ks> is a [string] representing either the keyspace that
%%                      already exists, or the keyspace in which the table that
%%                      already exists is.
%%                 <table> is a [string] representing the name of the table that
%%                         already exists. If the query was attempting to create a
%%                         keyspace, <table> will be present but will be the empty
%%                         string.
-define(ECAS_PROTO_ERROR_CODE_ALREADY_EXISTS,           16#2400).

%% Unprepared: Can be thrown while a prepared statement tries to be executed if the provided prepared statement ID is not known by this host.
%% The rest of the ERROR message body will be [short bytes] representing the unknown ID.
-define(ECAS_PROTO_ERROR_CODE_UNPREPARED,               16#2500).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RECORDS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Holds protocol state.
%%
%% Communications starts with initial options that can be altered later with various options
%% supported by the server and negociated with client.
%%
%% Possible options now are:
%%
%% - cql_version: the version of CQL to use. This option is mandatory and
%% currently the only version supported is "3.0.0". Note that this is different
%% from the protocol version.
%%
%% - cql_protocol_version: the protocol version to use
%%
%% - compression : the compression algorithm to use for frames.
%% If undefined no compression will be used.
-record(ecas_protocol_state, {
    cql_version             = <<"3.0.0">>,
    cql_protocol_version    = 4,
    cql_protocol_enforce    = false,
    cql_protocol_use_beta   = true,
    negotiated_compression  = undefined,
    stream_id               = 0
}).
-type ecas_protocol_state() :: #ecas_protocol_state{}.

%% Holds frame header information for encoding or decoding a cql frame into
-record(ecas_frame_header, {
    direction           = undefined :: undefined | 0 | 1,
    version             = undefined :: undefined | ?ECAS_PROTO_VERSION_3 | ?ECAS_PROTO_VERSION_4 | ?ECAS_PROTO_VERSION_5,
    flag_use_beta       = undefined :: undefined | true | false,
    flag_warning        = undefined :: undefined | true | false,
    flag_custom_payload = undefined :: undefined | true | false,
    flag_tracing        = undefined :: undefined | true | false,
    flag_compression    = undefined :: undefined | true | false,
    stream_id           = undefined,
    opcode              = undefined :: undefined |
}).
-type ecas_frame_header() :: #ecas_frame_header{}.

%% Holds frame information for encoding or decoding a cql frame into
-record(ecas_frame, {
    header  = undefined :: undefined | ecas_frame_header(),
    body    = undefined :: undefined | term()
}).
-type ecas_frame() :: #ecas_frame{}.

%% Holds frame information for encoding or decoding a cql error frame body
-record(ecas_frame_body_error, {
    code    = undefined :: undefined | ecas_proto_error_codes(),
    message = undefined :: undefined | binary,
    more    = undefined :: undefined | proplist(),
    rest    = undefined :: undefined | binary()
}).
-type ecas_frame_body_error() :: #ecas_frame_body_error{}.

%% Holds frame information for encoding or decoding a cql authenticate frame body
-record(ecas_frame_body_authenticate, {
    authenticator_class_name = undefined :: undefined | binary()
}).
-type ecas_frame_body_authenticate() :: #ecas_frame_body_authenticate{}.

