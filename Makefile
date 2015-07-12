
all: compile

get-deps:
	@./rebar get-deps

compile:
	@./rebar compile

clean:
	@./rebar clean

etcdq: compile
	(test -d _rel || mkdir _rel)
	(cd rel && ../rebar generate target_dir=../_rel/$@ overlay_vars=vars/$@.config)
