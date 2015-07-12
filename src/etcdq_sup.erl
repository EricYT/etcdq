-module(etcdq_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  %% etcdq job manager
  JobManager = ?CHILD(etcdq_job_manager, worker),

  %% etcdq job worker manager
  JobWorkerManager = ?CHILD(etcdq_worker_manager, worker),

  %% etcdq event manager
  EventManager = ?CHILD(etcdq_event_manager, worker),

  %% etcdq event worker manager
  EventPullerManager = ?CHILD(etcdq_event_puller_manager, worker),

  {ok, { {one_for_one, 5, 10}, [JobManager, JobWorkerManager, EventManager, EventPullerManager]} }.

