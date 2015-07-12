-define(ETCD_JOB_DIR, "/etcdq/jobs/").
-define(ETCD_EVENT_DIR, "/etcdq/events/").

-define(JOB_WAITING,  <<"waiting">>).
-define(JOB_RUNNING,  <<"running">>).
-define(JOB_COMPLETE, <<"complete">>).
-define(JOB_ERROR,    <<"error">>).

-define(DEFAULT_TTL, 300).

-define(EVENT_ENABLE,  enable).
-define(EVENT_DISABLE, disable).

%%
%% job info
-record(job, { id,
               state,
               entity,
               ttl
             }).


%% job event
-record(event, { id,
                 name,
                 state,
                 entity,
                 ttl
               }).

-record(event_deploy, { 
                  event,
                  size,
                  handle_module,
                  handle_function,
                  options
                }).
