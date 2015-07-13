-module(etcdq_job_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).


-include("etcdq.hrl").


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

push_job(JobEntity) ->
  gen_server:call(?SERVER, {push_job, JobEntity}).

cancle_job(JobId) ->
  gen_server:call(?SERVER, {cancle_job, JobId}).

get_status(JobId) ->
  gen_server:call(?SERVER, {get_status, JobId}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
lager:debug("-------------------> job manager start"),

  {ok, Args}.
handle_call({push_job, JobEntity}, _From, State) ->
  Reply = push_job_(JobEntity),
  {reply, Reply, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
push_job_(JobEntity) ->
  %% Make job record
  JobId = etcdq_kits:uuid(),
  Expire= etcdq_kits:expire(?DEFAULT_TTL),
  Job   = #job{ id = JobId, state = ?JOB_WAITING, entity = JobEntity , ttl = Expire },

  %% Save to cache
  ok = cache:set(JobId, Job, Expire),

  %% Put to etcd
  EtcdKey = etcdq_kits:pack_key(job, JobId),
  {ok, _Res} = etcd:insert(EtcdKey, ?JOB_WAITING, ?DEFAULT_TTL),

  {ok, JobId}.

