-module(etcdq_worker_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("etcdq.hrl").

-record(state, { workers }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-compile(export_all).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
  lager:debug("---------------------> worker manager start"),
  {ok, #state{}, 3000}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  Workers = start_workers(),
  lager:debug("---------------------> worker manager start workers:~p", [Workers]),
  {noreply, State#state{ workers=Workers }};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
make_worker_name(Index) -> "worker_"++integer_to_list(Index).

start_workers() ->
  WorkerSize = application:get_env(etcdq, etcdq_worker_size, 5),
  [ begin Worker = make_worker_name(Index), {ok, Pid} = start_worker(Worker), Pid end || Index <- lists:seq(1, WorkerSize) ].

start_worker(WorkerName) ->
  Restart = transient,
  Shutdown = 200,
  ChildSpec = { WorkerName,
                {etcdq_worker, start_link, []},
                Restart, Shutdown, worker, [etcdq_worker] },
  case supervisor:start_child(etcdq_sup, ChildSpec) of
    {ok, Pid}    -> {ok, Pid};
    {ok, _, Pid} -> {ok, Pid};
    Else     -> lager:error("start child error:~p", [Else])
  end.

