-module(etcdq_puller).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("etcdq.hrl").

-define(DEFAULT_TIMEOUT, 1000).
-define(DEFAULT_REQUEST_TIMEOUT, 5000).

-define(OPERATOR_MODULE, module).
-define(OPERATOR_FUNCTION, function).
-define(EVENT_DIR, event_dir).
-define(TOTAL_PULLER, total_puller).
-define(INDEX, index).
-define(OPTIONS, options).

-record(state, { event, index }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(EventAndIndex) ->
  lager:debug("***************(Puller start_link)***********: ~p", [EventAndIndex]),
  gen_server:start_link(?MODULE, [EventAndIndex], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([{#event_deploy{event=Event, handle_module=M, handle_function=F, size=Size, options=Options }=_EventDeploy, Index}]) ->
  lager:debug("**********(Puller start)**************puller start event:~p M:~p F:~p Size:~p", [Event, M, F, Size]),
  put(?OPERATOR_MODULE, M),
  put(?OPERATOR_FUNCTION, F),
  put(?EVENT_DIR, ?ETCD_EVENT_DIR++atom_to_list(Event)),
  put(?TOTAL_PULLER, Size),
  put(?INDEX, Index),
  put(?OPTIONS, Options),
  {ok, #state{ event = Event }, ?DEFAULT_TIMEOUT}.


handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  NewIndex = maybe_pull_event(State),
  {noreply, State#state{ index = NewIndex }, ?DEFAULT_TIMEOUT};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  lager:error("Event puller terminate Reason:~p State:~p", [_Reason, _State]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
maybe_pull_event(State) ->
  #state{ event=_Event, index=Index } = State,
  EventDir = get(?EVENT_DIR),
  lager:debug("-------------> Result 0:~p", [{self(), Index}]),
  case watch(EventDir, Index) of
    {ok, Result} ->
      CurrentIndex = ej:get({<<"node">>, <<"modifiedIndex">>}, Result, Index),
      lager:debug("-------------> Result 1:~p", [{self(), CurrentIndex}]),
      case ej:get({<<"action">>}, Result) of
        <<"create">> ->
          case is_me(Index) of
            true ->
              EtcdKey = ej:get({<<"node">>, <<"key">>}, Result),
              [_, Uuid] = etcdq_kits:unpack_key(EtcdKey),
              case cache:get(Uuid) of
                undefined ->
                  CurrentIndex;
                EventEntity ->
                  lager:debug("*********(EventEntity 1)***********:~p", [{get(?INDEX), EventEntity}]),
                  #event{ entity=Entity } = EventEntity,
                  Module = get(?OPERATOR_MODULE),
                  Function = get(?OPERATOR_FUNCTION),
                  Options  = get(?OPTIONS),
                  lager:debug("*********(EventEntity 2)**********:~p", [{Module, Function, Options}]),
                  catch Module:Function(Entity, Options),
                  %%catch Module:Function(Entity),
                  CurrentIndex
              end,
              CurrentIndex;
            false ->
              CurrentIndex
          end;
        _ ->
          CurrentIndex
      end;
    Error ->
%%      lager:error("Event puller pull error ~p", [Error]),
      Index
  end.

%% Hash one puller to do this job
%% First one get the job
is_me(undefined) ->
  1 =:= get(?INDEX);
is_me(CurrentIndex) ->
  ((CurrentIndex rem get(?TOTAL_PULLER)) + 1) =:= get(?INDEX).

watch(EventDir, undefined) ->
  %%etcd:watch(EventDir, true, ?DEFAULT_REQUEST_TIMEOUT);
  etcd:watch(EventDir, true);
watch(EventDir, CurrentIndex) ->
  %%etcd:watch_ex(EventDir, CurrentIndex+1, true, ?DEFAULT_REQUEST_TIMEOUT).
  etcd:watch_ex(EventDir, CurrentIndex+1, true).
