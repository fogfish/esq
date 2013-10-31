.PHONY: deps test rel

BB=../basho_bench

all: rebar deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean 
	./rebar delete-deps

test: all
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc

dialyzer: compile
	@dialyzer -Wno_return -c apps/riak_kv/ebin

rel:
	./rebar generate

run:
	erl -name esq@127.0.0.1 -setcookie nocookie -pa ./deps/*/ebin -pa ./ebin

rebar:
	curl -O https://raw.github.com/wiki/basho/rebar/rebar
	chmod ugo+x rebar

benchmark:
	$(BB)/basho_bench -N bb@127.0.0.1 -C nocookie priv/esq.benchmark
	$(BB)/priv/summary.r -i tests/current
	open tests/current/summary.png

