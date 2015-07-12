-module(etcdq_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("etcdq.hrl").

-define(DEFAULT_TIMEOUT, 1000).
-define(DEFAULT_REQUEST_TIMEOUT, 5000).

-record(state, { }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
  gen_server:start_link(?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
  lager:debug("Job worker start pid:~p", [self()]),
  {ok, #state{}, ?DEFAULT_TIMEOUT}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  maybe_do_job(),
  {noreply, State, ?DEFAULT_TIMEOUT};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
maybe_do_job() ->
  %%lager:debug("-------(1)------> Worker timeout"),
  %% List all jobs in the directory
  case etcd:read(?ETCD_JOB_DIR, true) of
    {ok, {_EtcdIndex, Response}} ->
      %% Get a job
      AllNodes = ej:get({<<"node">>, <<"nodes">>}, Response, []),
      case filter_a_job(AllNodes) of
        false ->
          true;
        Key   ->
          %% try to lock the job
          %%lager:debug("---------(get jobs)---------> ~p", [Key]),
          case etcd:update_ex(Key, ?JOB_WAITING, ?JOB_RUNNING) of 
            {ok, {_NewIndex, _Response}} ->
              %% Lock the job
              lager:debug("--------(lock success)-------------> Key:~p", [Key]),
              try
                %% unpack the key
                JobId = etcdq_kits:unpack_key(Key),
                #job{entity = Entity} = JobEntity = cache:get(JobId),
                %% Do job
                todo,
                %% Update job status
                {ok, {_, _}} = etcd:update_ex(Key, ?JOB_RUNNING, ?JOB_COMPLETE, ?DEFAULT_REQUEST_TIMEOUT),
                lager:debug("--------(operator success)-------------> done"),
                true
              catch Error:Reason ->
                      lager:error("------(operator error)----------> operator job error:~p Reason:~p stacktrace:~p", [
                                                                                                                      Error,
                                                                                                                      Reason,
                                                                                                      erlang:get_stacktrace()
                                                                                                     ]),
                      {ok, {_, _}} = etcd:update(Key, ?JOB_ERROR, ?DEFAULT_REQUEST_TIMEOUT),
                      false
              end;
            Faild ->
              lager:debug("--------(lock faild)-------------> Lock the job error:~p", [Faild]),
              true
          end
      end;
    Error ->
      %%lager:debug("---------------(Other)---------> etcd:read error:~p", [Error]),
      false
  end.

%% Filter the oldest job
filter_a_job(Jobs) ->
  case filter_waiting_jobs(Jobs, []) of
    [] -> false;
    WaitingJobs ->
      filter_old_one(WaitingJobs)
  end.

filter_waiting_jobs([Job|Tail], Acc) ->
  case ej:get({<<"value">>}, Job, undefined) of
    ?JOB_WAITING ->
      filter_waiting_jobs(Tail, [Job|Acc]);
    _Other ->
      filter_waiting_jobs(Tail, Acc)
  end;
filter_waiting_jobs([], Acc) -> Acc.

filter_old_one(WaitingJobs) ->
  [{Index, Job}|_] = lists:sort([ {ej:get({<<"createdIndex">>}, Job), Job} || Job<-WaitingJobs ]),
  ej:get({<<"key">>}, Job, false).

