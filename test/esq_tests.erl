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
%%
%% @todo
%%   fix test cases
-module(esq_tests).
-include_lib("eunit/include/eunit.hrl").

%%%------------------------------------------------------------------
%%%
%%% suites
%%%
%%%------------------------------------------------------------------   

esq_test_() ->
   {foreach,
      fun init/0,
      fun free/1,
      [
         fun enq_deq/1,
         fun ttl_expired/1,
         fun ack/1
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------   

init() ->
   esq:start(),
   esq:new([]).

free(Queue) ->
   esq:free(Queue).
   

%%%------------------------------------------------------------------
%%%
%%% unit
%%%
%%%------------------------------------------------------------------   

enq_deq(Queue) ->
   [
      ?_assertMatch(ok, esq:enq(Pid, <<"a">>))
     ,?_assertMatch({ok, [{_, <<"a">>}]}, esq:deq(Pid))
   ].

ttl_expired(Pid) ->
   [
      ?_assertMatch(ok, esq:ioctl({ttl, 500}, Pid))
     ,?_assertMatch(ok, esq:enq(Pid, <<"a">>))
     ,?_assertMatch(ok, esq:enq(Pid, <<"b">>))
     ,?_assertMatch({ok, [{_, <<"a">>}]}, esq:deq(Pid))
     ,?_assertMatch(ok, timer:sleep(500))
     ,?_assertMatch({ok, []}, esq:deq(Pid))   
   ].

ack(Pid) ->
   [
      ?_assertMatch(ok, esq:ioctl({ttf, 200}, Pid))
     ,?_assertMatch(ok, esq:enq(Pid, <<"a">>))
     ,?_assertMatch({ok, [{_, <<"a">>}]}, esq:deq(Pid))
     ,?_assertMatch(ok, timer:sleep(210))
     ,?_assertMatch({ok, [{_, <<"a">>}]}, esq:deq(Pid))
     ,?_assertMatch(ok, timer:sleep(210))
     ,?_assertMatch(ok, esq:ack(Pid, uid(esq:deq(Pid)) ))
     ,?_assertMatch(ok, timer:sleep(210))
     ,?_assertMatch({ok, []}, esq:deq(Pid))      
   ].


uid({ok, [{Uid, _}]}) ->
   Uid.
