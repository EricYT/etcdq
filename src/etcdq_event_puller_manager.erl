-module(etcdq_event_puller_manager).
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

-record(puller, { event, workers=[], deploy }).
-record(state, { pullers = [] }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_pullers() ->
  gen_server:call(?SERVER, {get_pullers}).

new_event(EventDeploy) ->
  gen_server:call(?SERVER, {new_event, EventDeploy}).

delete_event(Event) ->
  gen_server:call(?SERVER, {delete_event, Event}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
  {ok, #state{}, 3000}.

handle_call({new_event, EventDeploy}, _From, State) ->
  {Reply, NewState} = new_event_(EventDeploy, State),
  {reply, Reply, NewState};
handle_call({delete_event, Event}, _From, State) ->
  {Reply, NewState} = delete_event_(Event, State),
  {reply, Reply, NewState};
handle_call({get_pullers}, _From, State) ->
  {reply, {ok, State#state.pullers}, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  %% Init Event workers
  Events = etcdq_event_manager:get_events(),
lager:debug("******************(Get events)*************:~p", [Events]),
  Pullers = start_pullers(Events, []),
lager:debug("******************( pullers )*************:~p", [Pullers]),
  {noreply, State#state{ pullers=Pullers }};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
start_pullers([Event|Tail], Acc) ->
  Puller = start_puller(Event, Event#event_deploy.size, []),
  start_pullers(Tail, [Puller|Acc]);
start_pullers([], Acc) -> Acc.

new_event_(#event_deploy{ event=Event, size=Size }=EventDeploy, State) ->
  CurrentPullers = State#state.pullers,
  {Result, NewPullers} =
  case lists:keyfind(Event, #puller.event, CurrentPullers) of
    false ->
      Puller = start_puller(EventDeploy, Size, []),
      {ok, [Puller|CurrentPullers]};
    _ ->
      {error, CurrentPullers}
  end,
  {Result, State#state{ pullers=NewPullers }}.

delete_event_(Event, State) ->
  CurrentPullers = State#state.pullers,
  {Result, NewPullers} =
  case lists:keyfind(Event, #puller.event, CurrentPullers) of
    false ->
      {{error, not_started}, CurrentPullers};
    #puller{ workers=_Workers, deploy=Deploy }=_Puller ->
      case terminate_event(Deploy) of
        ok ->
          NewPullers_ = lists:keydelete(Event, #puller.event, CurrentPullers),
          {{ok, true}, NewPullers_};
        error ->
          {{error, stop_puller_error}, CurrentPullers}
      end
  end,
  {Result, State#state{ pullers=NewPullers }}.

terminate_event(#event_deploy{ event=Event, size=Size }) ->
  ChildName = [ make_puller_name(Event, Index) || Index<-lists:seq(1, Size) ],
  terminate_child(ChildName).

terminate_child([Child|Tail]) ->
  case supervisor:terminate_child(etcdq_sup, Child) of
    ok ->
      case supervisor:delete_child(etcdq_sup, Child) of
        ok ->
          terminate_child(Tail);
        _ ->
          error
      end;
    _ ->
      error
  end;
terminate_child([]) -> ok.

start_puller(#event_deploy{event=Event}=EventInfo, 0, Acc) -> #puller{ event=Event, workers=Acc, deploy=EventInfo };
start_puller(#event_deploy{}=EventInfo, Size, Acc) ->
  {ok, Pid} = start_puller_(EventInfo, Size),
  start_puller(EventInfo#event_deploy{}, Size-1, [Pid|Acc]).

make_puller_name(Event, Index) ->
  lists:concat([Event, "_", Index]).

start_puller_(#event_deploy{event=Event}=EventInfo, Index) ->
  EventName = make_puller_name(Event, Index),
  Restart = transient,
  Shutdown = 200,
  ChildSpec = { EventName,
                {etcdq_puller, start_link, [{EventInfo, Index}]},
                Restart, Shutdown, worker, [etcdq_puller] },
  case supervisor:start_child(etcdq_sup, ChildSpec) of
    {ok, Pid}    -> {ok, Pid};
    {ok, _, Pid} -> {ok, Pid};
    Else         -> lager:error("start event child error:~p", [Else])
  end.
