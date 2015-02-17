%%
%%   Copyright 2012 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   see http://www.erlang.org/doc/apps/eunit/chapter.html
-module(esq_tests).
-include_lib("eunit/include/eunit.hrl").

%%%------------------------------------------------------------------
%%%
%%% suites
%%%
%%%------------------------------------------------------------------   

esq_proc_test_() ->
   {foreach,
      fun esq_proc_init/0,
      fun esq_proc_free/1,
      [
         fun enq/1
        ,fun deq/1
        ,fun ttl/1
        ,fun pubsub/1
      ]
   }.

esq_heap_test_() ->
   {foreach,
      fun esq_heap_init/0,
      fun esq_heap_free/1,
      [
         fun enq/1
        ,fun deq/1
      ]
   }.

esq_fspool_test_() ->
   {foreach,
      fun esq_fspool_init/0,
      fun esq_fspool_free/1,
      [
         fun enq_inf/1
        ,fun deq/1
      ]
   }.

esq_tspool_test_() ->
   {foreach,
      fun esq_tspool_init/0,
      fun esq_tspool_free/1,
      [
         fun enq_inf/1
        ,fun deq_as_text/1
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% setup
%%%
%%%------------------------------------------------------------------   

%%
%%
esq_proc_init() ->
   {ok, Pid} = esq:start_link([proc]),
   Pid.

esq_proc_free(Pid) ->
   esq:close(Pid).

%%
%%
esq_heap_init() ->
   {ok, Pid} = esq:start_link([heap]),
   Pid.

esq_heap_free(Pid) ->
   esq:close(Pid).

%%
%%
esq_fspool_init() ->
   os:cmd("rm -Rf /tmp/fspool"),
   {ok, Pid} = esq:start_link([{fspool, "/tmp/fspool"}, {segment, 0}]),
   Pid.

esq_fspool_free(Pid) ->
   esq:close(Pid),
   % os:cmd("rm -Rf /tmp/fspool"),
   ok.

%%
%%
esq_tspool_init() ->
   os:cmd("rm -Rf /tmp/tspool"),
   {ok, Pid} = esq:start_link([{tspool, "/tmp/tspool"}, {segment, 0}]),
   Pid.

esq_tspool_free(Pid) ->
   esq:close(Pid),
   % os:cmd("rm -Rf /tmp/tspool"),
   ok.

%%%------------------------------------------------------------------
%%%
%%% unit tests
%%%
%%%------------------------------------------------------------------   

enq(Pid) ->
   [
      ?_assertMatch(ok, esq:enq(Pid, <<"msg">>))
     ,?_assertEqual( 1, esq:ioctl(length, Pid))
   ].

enq_inf(Pid) ->
   [
      ?_assertMatch(ok,  esq:enq(Pid, <<"msg">>))
     ,?_assertEqual(inf, esq:ioctl(length, Pid))
   ].

deq(Pid) ->
   [
      ?_assertMatch(ok, esq:enq(Pid, <<"msg">>))
     ,?_assertMatch([<<"msg">>], esq:deq(Pid))
   ].

deq_as_text(Pid) ->
   [
      ?_assertMatch(ok, esq:enq(Pid, <<"msg\n">>))
     ,?_assertMatch([<<"msg\n">>], esq:deq(Pid))
   ].


ttl(Pid) ->
   [
      ?_assertMatch(ok, esq:ioctl(ttl, {0,0,100000}, Pid))
     
     ,?_assertMatch(ok, esq:enq(Pid, <<"msg">>))
     ,?_assertMatch(ok, timer:sleep(40))
     ,?_assertMatch([<<"msg">>], esq:deq(Pid))   

     ,?_assertMatch(ok, esq:enq(Pid, <<"msg">>))
     ,?_assertMatch(ok,  timer:sleep(150))
     ,?_assertMatch([], esq:deq(Pid))

     ,?_assertMatch(ok, esq:ioctl(ttl, undefined, Pid))
   ]. 

pubsub(Pid) ->
   [
      ?_assertMatch(ok, esq:ioctl(ready, 2, Pid))

     ,?_assertMatch(ok, esq:enq(Pid, <<"a">>))
     ,?_assertMatch(ok, esq:enq(Pid, <<"b">>))
     ,?_assertMatch(ok, esq:enq(Pid, <<"c">>))
     ,?_assertMatch(ok, esq:ioctl(sub, self(), Pid))
     ,?_assertMatch( 3, esq_msg(Pid))
     ,?_assertMatch([<<"a">>, <<"b">>, <<"c">>], esq:deq(Pid, 3))

     ,?_assertMatch(ok, esq:ioctl(sub, self(), Pid))
     ,?_assertMatch(ok, esq:enq(Pid, <<"a">>))
     ,?_assertMatch(ok, esq:enq(Pid, <<"b">>))
     ,?_assertMatch( 2, esq_msg(Pid))
     ,?_assertMatch([<<"a">>, <<"b">>], esq:deq(Pid, 2))

     ,?_assertMatch(ok, esq:ioctl(undefined, 2, Pid))
   ].


esq_msg(Pid) ->
   receive
      {esq, Pid, N} -> N
   end.

