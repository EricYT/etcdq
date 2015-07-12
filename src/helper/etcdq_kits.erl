-module(etcdq_kits).

-include("etcdq.hrl").

-compile(export_all).

uuid() ->
    uuid(32).
uuid(16) ->
    T = term_to_binary({make_ref(), now()}),
    <<I:160/integer>> = crypto:sha(T),
    string:to_lower(lists:flatten( io_lib:format("~16..0s", [erlang:integer_to_list(I, 16)]) ));
uuid(32) ->
    T = term_to_binary({make_ref(), now()}),
    <<I:160/integer>> = crypto:sha(T),
    string:to_lower(lists:flatten( io_lib:format("~32..0s", [erlang:integer_to_list(I, 16)]) )).


pack_key(job, Uuid)   -> ?ETCD_JOB_DIR++Uuid;
pack_key(event, {EventName, Uuid}) -> ?ETCD_EVENT_DIR++atom_to_list(EventName)++"/"++Uuid.

unpack_key(Key) when is_binary(Key) -> unpack_key(binary_to_list(Key));
unpack_key(?ETCD_JOB_DIR++Uuid)     -> Uuid;
unpack_key(?ETCD_EVENT_DIR++EventAndUuid)  ->
  [EventName, Uuid] = string:tokens(EventAndUuid, [$/]).


expire(TTL) ->
  Now = os:timestamp(),
  calendar:datetime_to_gregorian_seconds(
    calendar:now_to_datetime(Now)) + TTL.
