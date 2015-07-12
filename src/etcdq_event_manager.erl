-module(etcdq_event_manager).
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
%%

-record(state, { registered = maps:new() }).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Event, Size, HandlerMod, HandleFun, Options) ->
  gen_server:call(?SERVER, {register, Event, Size, HandlerMod, HandleFun, Options}).

unregister(Event) ->
  gen_server:call(?SERVER, {unregister, Event}).

get_events() ->
  gen_server:call(?SERVER, {get_events}).

publish(Event, Args) ->
  gen_server:call(?SERVER, {publish, Event, Args}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------


%%TODO: One event one directory.
init(_Args) ->
  %% Init the default events
  %% Events format: {event, pullersize, callbackModule, callbackFunction, other}
  Events = application:get_env(etcdq, events, [{test, 2, io, format, []}]),
  Registered = convert_events(Events, []),
  {ok, #state{ registered = Registered }}.

handle_call({register, Event, Size, HandlerMod, HandleFun, Options}, _From, State) ->
  {Reply, NewState} = register_(Event, Size, HandlerMod, HandleFun, Options, State),
  {reply, Reply, NewState};

handle_call({unregister, Event}, _From, State) ->
  {Reply, NewState} = unregister_(Event, State),
  {reply, Reply, NewState};

handle_call({get_events}, _From, State) ->
  Reply = get_events_(State),
  {reply, Reply, State};

handle_call({publish, Event, Args}, _From, State) ->
  Reply = publish_(Event, Args),
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
convert_events([{Event, PullerSize, HandlerMod, HandleFun, Options}|Tail], Acc) when PullerSize >= 0 ->
  Puller = #event_deploy{ event=Event, size=PullerSize, handle_module=HandlerMod, handle_function=HandleFun, options=Options },
  convert_events(Tail, [Puller|Acc]);
convert_events([{Event, _, _, _, _}|Tail], Acc) ->
  lager:error("Event puller size must be non_neg_integer:~p", [Event]),
  convert_events(Tail, Acc);
convert_events([], Acc) -> Acc.

register_(Event, Size, HandlerMod, HandleFun, Options, State) ->
  Registered = State#state.registered,
  %%TODO: Start workers to watch event
  {Result, NewRegistered} =
  case lists:keyfind(Event, #event_deploy.event, Registered) of
    false ->
      Puller = #event_deploy{ event=Event, size=Size, handle_module=HandlerMod, handle_function=HandleFun, options=Options },
      case etcdq_event_puller_manager:new_event(Puller) of
        ok ->
          {{ok, true}, [Puller|Registered]};
        error ->
          {{error, start_pullers_error}, Registered}
      end;
    _ ->
      {{error, event_already_started}, Registered}
  end,
  {Result, State#state{ registered = NewRegistered }}.

unregister_(Event, State) ->
  Registered = State#state.registered,
  lager:debug("--------------(unregister) : ~p", [Registered]),
  {Result, NewRegistered} =
  case lists:keyfind(Event, #event_deploy.event, Registered) of
    false ->
      {error, not_event};
    _ ->
      case etcdq_event_puller_manager:delete_event(Event) of
        {ok, _} ->
          %%TODO: etcdq clean
          Registered_ = lists:keydelete(Event, #event_deploy.event, Registered),
          {ok, Registered_};
        {error, _} ->
          {error, Registered}
      end
  end,
  {Result, State#state{ registered = NewRegistered }}.

get_events_(#state{ registered = Registered }) ->
  Registered.

publish_(Event, Args) ->
  try
    %% Make event record
    EventId = etcdq_kits:uuid(),
    Expire  = etcdq_kits:expire(?DEFAULT_TTL),
    Event_  = #event{ id = EventId, name = Event, entity = Args, ttl = Expire },

    %% Save to cache
    ok = cache:set(EventId, Event_, Expire),

    %% Put to etcd
    EtcdKey = etcdq_kits:pack_key(event, {Event, EventId}),
%%    lager:debug("Publish key:~p", [EtcdKey]),
    {ok, _Res} = etcd:insert(EtcdKey, Event, ?DEFAULT_TTL),

    {ok, EventId}
  catch Error:Reason ->
          lager:error("Event manager publish error:~p reason:~p stacktrace:~p", [Error,
                                                                                 Reason,
                                                                                 erlang:get_stacktrace()
                                                                                ]),
          {error, undefined}
  end.
