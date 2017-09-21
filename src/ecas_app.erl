%%%-------------------------------------------------------------------
%%% @author Madalin Grigore-Enescu
%%% @copyright (C) 2017, <Madalin Grigore-Enescu>
%%% @doc
%%%
%%% @end
%%% Created : 09. Feb 2017 8:37 PM
%%%-------------------------------------------------------------------
-module(ecas_app).
-author("madalin").

%% API.
-export([start/2]).
-export([stop/1]).

%% @spec start(Type, Args) -> ServerRet
%% @doc application start callback.
start(_Type, _Args) ->

    %% Ensure Q is started
    {ok, _} = application:ensure_all_started(q),

    %% Start application main supervisor
    case ecas_sup:start_link() of
        {ok, Pid} -> {ok, Pid};
        Error -> Error
    end.

%% @spec stop(State) -> ServerRet
%% @doc application stop callback.
stop(_State) -> ok.

