%%%-------------------------------------------------------------------
%%% @author madalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jun 2017 1:06 PM
%%%-------------------------------------------------------------------
-author("madalin").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CONSISTENCY
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ECAS_CONSISTENCY_ANY,          0).
-define(ECAS_CONSISTENCY_ONE,          1).
-define(ECAS_CONSISTENCY_TWO,          2).
-define(ECAS_CONSISTENCY_THREE,        3).
-define(ECAS_CONSISTENCY_QUORUM,       4).
-define(ECAS_CONSISTENCY_ALL,          5).
-define(ECAS_CONSISTENCY_LOCAL_QUORUM, 6).
-define(ECAS_CONSISTENCY_EACH_QUORUM,  7).
-define(ECAS_CONSISTENCY_SERIAL,       8).
-define(ECAS_CONSISTENCY_LOCAL_SERIAL, 9).
-define(ECAS_CONSISTENCY_LOCAL_ONE,    10).

-type ecas_consistency_integer() :: ?ECAS_CONSISTENCY_ANY .. ?ECAS_CONSISTENCY_EACH_QUORUM | ?ECAS_CONSISTENCY_LOCAL_ONE.
-type ecas_consistency_atom() :: any | one | two | three | quorum | all | local_quorum | each_quorum | local_one.

-type ecas_serial_consistency_integer() :: ?ECAS_CONSISTENCY_SERIAL | ?ECAS_CONSISTENCY_LOCAL_SERIAL.
-type ecas_serial_consistency_atom() :: serial | local_serial.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% BATCH
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ECAS_BATCH_TYPE_LOGGED,        0).
-define(ECAS_BATCH_TYPE_UNLOGGED,      1).
-define(ECAS_BATCH_TYPE_COUNTER,       2).

-type ecas_batch_type_integer() :: ?ECAS_BATCH_TYPE_LOGGED | ?ECAS_BATCH_TYPE_UNLOGGED | ?ECAS_BATCH_TYPE_COUNTER.
-type ecas_batch_type_atom() :: logged | unlogged | counter.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% COLUMN TYPES & QUERY PARAMETERS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type column_type() ::
{custom, binary()} |
{map, column_type(), column_type()} |
{set | list, column_type()} | datatype().

-type datatype() :: ascii | bigint | blob | boolean | counter | decimal | double | float | int | timestamp | uuid | varchar | varint | timeuuid | inet.

-type ecas_parameter_val() :: number() | binary() | list() | atom() | boolean().
-type ecas_parameter() :: {datatype(), ecas_parameter_val()}.
-type ecas_named_parameter() :: {atom(), ecas_parameter_val()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% COMMON RECORDS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(ecas_query, {

    keyspace                = <<>>,

    statement               = <<>>      :: iodata(),
    parameters              = []        :: [ecas_parameter() | ecas_named_parameter()] | map(),

    reusable                = undefined :: undefined | boolean(),
    named                   = false     :: boolean(),

    page_size               = 100       :: integer(),
    page_state              = undefined :: undefined | binary(),

    consistency             = ?ECAS_CONSISTENCY_ONE :: ecas_consistency_integer() | ecas_consistency_atom(),
    serial_consistency      = undefined :: undefined | ecas_serial_consistency_integer() | ecas_serial_consistency_atom(),
    value_encode_handler    = undefined :: function() | undefined,

    prepared_id             = undefined :: binary()

}).

-record(ecas_query_batch, {
    consistency             = one :: ecas_consistency_integer() | ecas_consistency_atom(),
    type                    = ?ECAS_BATCH_TYPE_LOGGED :: ecas_batch_type_integer() | ecas_batch_type_atom(),
    queries                 = [] :: list(tuple())
}).

-record(ecas_result, {
    columns                 = []        :: list(tuple()),
    dataset                 = []        :: list(list(term())),
    query                               :: #ecas_query{},
    client                              :: {pid(), reference()}
}).