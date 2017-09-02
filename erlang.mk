##
## Copyright (C) 2012 Dmitry Kolesnikov
##
## This Makefile may be modified and distributed under the terms
## of the MIT license.  See the LICENSE file for details.
## https://github.com/fogfish/makefile
##
## @doc
##   This makefile is the wrapper of rebar to build and ship erlang software
##
## @version 1.0.0
.PHONY: all compile test unit clean distclean run console benchmark rel pkg

##
## config
PREFIX ?= /usr/local
APP    ?= $(notdir $(CURDIR))
ARCH    = $(shell uname -m)
PLAT   ?= $(shell uname -s)
VSN    ?= $(shell test -z "`git status --porcelain`" && git describe --tags --long | sed -e 's/-g[0-9a-f]*//' | sed -e 's/-0//' || echo "`git describe --abbrev=0 --tags`-dev")
LATEST ?= latest
REL     = ${APP}-${VSN}
PKG     = ${REL}+${ARCH}.${PLAT}
TEST   ?= ${APP}
COOKIE ?= nocookie
DOCKER ?= fogfish/erlang
IID     = ${URI}${ORG}/${APP}

## required tools
##  - rebar version (no spaces at end)
##  - path to basho benchmark 
REBAR  ?= 3.3.2
BB      = ../basho_bench


## erlang runtime configration flags
ROOT   = $(shell pwd)
ADDR   = 127.0.0.1
EFLAGS = \
	-name ${APP}@${ADDR} \
	-setcookie ${COOKIE} \
	-pa ${ROOT}/_build/default/lib/*/ebin \
	-pa ${ROOT}/rel \
	-kernel inet_dist_listen_min 32100 \
	-kernel inet_dist_listen_max 32199 \
	+P 1000000 \
	+K true +A 160 -sbt ts


## erlang common test bootstrap
BOOT_CT = \
   -module(test). \
   -export([run/1]). \
   run(Spec) -> \
      {ok, Test} = file:consult(Spec), \
      Error = case lists:keyfind(node, 1, Test) of \
         false -> element(2, ct:run_test([{spec, Spec}])); \
         true  -> ct_master:run(Spec) \
      end, \
      erlang:halt(Error).

## 
BUILDER = FROM ${DOCKER}\nARG VERSION=\nRUN mkdir ${APP}\nCOPY . ${APP}/\nRUN cd ${APP} && make VSN=\x24{VERSION} && make rel VSN=\x24{VERSION}\n
SPAWNER = FROM ${DOCKER}\nENV VERSION=${VSN}\nRUN mkdir ${APP}\nCOPY . ${APP}/\nRUN cd ${APP} && make VSN=\x24{VERSION} && make rel VSN=\x24{VERSION}\nCMD sh -c 'cd ${APP} && make console VSN=\x24{VERSION}'\n

## self extracting bundle archive
BUNDLE_INIT = PREFIX=${PREFIX}\nREL=${PREFIX}/${REL}\nAPP=${APP}\nVSN=${VSN}\nLINE=`grep -a -n "BUNDLE:$$" $$0`\nmkdir -p $${REL}\ntail -n +$$(( $${LINE%%%%:*} + 1)) $$0 | gzip -vdc - | tar -C $${REL} -xvf - > /dev/null\n
BUNDLE_FREE = exit\nBUNDLE:\n


#####################################################################
##
## build
##
#####################################################################
all: rebar3 compile test

compile:
	@./rebar3 compile


##
## execute common test and terminate node
test: _build/test.beam
	@mkdir -p /tmp/test/${APP}
	@erl ${EFLAGS} -noshell -pa _build/ -pa test/ -run test run test/${TEST}.config
	@F=`ls /tmp/test/${APP}/ct_run*/all.coverdata | tail -n 1` ;\
	cp $$F /tmp/test/${APP}/ct.coverdata

_build/test.beam: _build/test.erl
	@erlc -o _build $<

_build/test.erl:
	@mkdir -p _build && echo "${BOOT_CT}" > $@


##
## execute unit test
unit: all
	@./rebar3 skip_deps=true eunit

##
## clean 
clean:
	@./rebar3 clean
	@rm -f  test/*.beam
	@rm -Rf _build/default/rel
	@rm -f _build/spawner
	@rm -rf test.*-temp-data
	@rm -rf tests
	@rm -rf log
	@rm -f  relx.config
	@rm -f  *.tar.gz
	@rm -f  *.bundle
	@docker rmi -f ${IID}:${LATEST} || echo ""
	@docker rmi -f ${IID}:${VSN} || echo ""

distclean: clean 
	@./rebar3 unlock
	@rm -Rf _build
	@rm -Rf rebar3

#####################################################################
##
## debug
##
#####################################################################
run: 
	@erl ${EFLAGS}

console: ${PKG}.tar.gz
	@_build/default/rel/${APP}/bin/${APP} foreground

mock: mock.yml
	docker-compose -f $< up

benchmark:
	@echo "==> benchmark: ${TEST}" ;\
	$(BB)/basho_bench -N bb@127.0.0.1 -C nocookie priv/${TEST}.benchmark ;\
	$(BB)/priv/summary.r -i tests/current ;\
	open tests/current/summary.png

#####################################################################
##
## release 
##
#####################################################################
rel: ${PKG}.tar.gz

## assemble VM release
ifeq (${PLAT},$(shell uname -s))
${PKG}.tar.gz: relx.config
	@./rebar3 tar -n ${APP} -v ${VSN} ;\
	cp _build/default/rel/${APP}/${APP}-${VSN}.tar.gz $@ ;\
	echo "==> tarball: $@"

relx.config: rel/relx.config.src
	@cat $< | sed "s/release/release, {'${APP}', \"${VSN}\"}/" > $@ 
else
${PKG}.tar.gz: _build/builder
	@docker build --file=$< --force-rm=true --build-arg="VERSION=${VSN}" --tag=build/${APP}:latest . ;\
	I=`docker create build/${APP}:latest` ;\
	docker cp $$I:/${APP}/$@ $@ ;\
	docker rm -f $$I ;\
	docker rmi build/${APP}:latest ;\
	test -f $@ && echo "==> tarball: $@"

_build/builder:
	@mkdir -p _build && echo "${BUILDER}" > $@
endif

## build docker image
docker: Dockerfile
	docker build \
		--build-arg APP=${APP} \
		--build-arg VSN=${VSN} \
		-t ${IID}:${VSN} -f $< .
	docker tag ${IID}:${VSN} ${IID}:${LATEST}

service: local.yml _build/spawner
	docker-compose -f $< up

_build/spawner:
	@mkdir -p _build && echo "${SPAWNER}" > $@


pkg: ${PKG}.tar.gz ${PKG}.bundle


${PKG}.bundle: rel/bootstrap.sh
	@printf '${BUNDLE_INIT}' > $@ ;\
	cat $<  >> $@ ;\
	printf  '${BUNDLE_FREE}' >> $@ ;\
	cat  ${PKG}.tar.gz >> $@ ;\
	chmod ugo+x $@ ;\
	echo "==> bundle: $@"


#####################################################################
##
## dependencies
##
#####################################################################
rebar3:
	@echo "==> install rebar (${REBAR})" ;\
	curl -L -O -s https://github.com/erlang/rebar3/releases/download/${REBAR}/rebar3 ;\
	chmod +x $@

