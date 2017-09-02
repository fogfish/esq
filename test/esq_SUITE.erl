%%
%%   Copyright (c) 2017, Dmitry Kolesnikov
%%   All Rights Reserved.
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
-module(esq_SUITE).
-include_lib("common_test/include/ct.hrl").

%%
%% common test
-export([
   all/0
  ,groups/0
  ,init_per_suite/1
  ,end_per_suite/1
  ,init_per_group/2
  ,end_per_group/2
]).

-export([
   enq/1, 
   deq/1,
   persistence/1,
   inflight/1
]).

%%%----------------------------------------------------------------------------   
%%%
%%% suite
%%%
%%%----------------------------------------------------------------------------   
all() ->
   [
      {group, interface}
   ].

groups() ->
   [
      {interface, [parallel], 
         [enq, deq, persistence, inflight]}
   ].


%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   Config.

end_per_suite(_Config) ->
   os:cmd("rm -Rf /tmp/q"),
   ok.

%% 
%%
init_per_group(_, Config) ->
   Config.

end_per_group(_, _Config) ->
   ok.


%%%----------------------------------------------------------------------------   
%%%
%%% unit test
%%%
%%%----------------------------------------------------------------------------   

enq(_Config) ->
   {ok, Q} = esq:new("/tmp/q/enq"),
   ok = esq:enq(a, Q),
   ok = esq:free(Q),

   true = filelib:is_dir("/tmp/q/enq").

deq(_Config) ->
   {ok, Q} = esq:new("/tmp/q/deq", [{tts, 1}]),
   ok = esq:enq(a, Q),
   timer:sleep(1),
   [#{payload := a}] = esq:deq(Q),
   ok = esq:free(Q).

persistence(_Config) ->
   {ok, A} = esq:new("/tmp/q/persistence"),
   ok = esq:enq(a, A),
   ok = esq:free(A),

   {ok, B} = esq:new("/tmp/q/persistence"),
   [#{payload := a}] = esq:deq(B),
   ok = esq:free(B).

inflight(_Config) ->
   {ok, Q} = esq:new("/tmp/q/inflight", [{tts, 1}, {ttf, 10}]),
   [esq:enq(X, Q) || X <- [a, b, c, d]],
   timer:sleep(1),
   [#{payload := a, receipt := A}] = esq:deq(Q),
   ok = esq:ack(A, Q),

   timer:sleep(15),
   [#{payload := b}] = esq:deq(Q),
   timer:sleep(15),
   [#{payload := b}] = esq:deq(Q),
   ok = esq:free(Q).


