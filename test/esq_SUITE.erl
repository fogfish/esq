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
   Q0 = esq:new("/tmp/q/enq"),
   Q1 = esq:enq(a, Q0),
   ok = esq:free(Q1),

   true = filelib:is_dir("/tmp/q/enq").

deq(_Config) ->
   Q0 = esq:new("/tmp/q/deq", [{tts, 1}]),
   Q1 = esq:enq(a, Q0),
   timer:sleep(1),
   {[#{payload := a}], Q2} = esq:deq(Q1),
   ok = esq:free(Q2).

persistence(_Config) ->
   A0 = esq:new("/tmp/q/persistence"),
   A1 = esq:enq(a, A0),
   ok = esq:free(A1),

   B0 = esq:new("/tmp/q/persistence"),
   {[#{payload := a}], B1} = esq:deq(B0),
   ok = esq:free(B1).

inflight(_Config) ->
   Q0 = esq:new("/tmp/q/inflight", [{tts, 1}, {ttf, 10}]),
   Q1 = lists:foldl(fun esq:enq/2, Q0, [a, b, c, d]),
   timer:sleep(1),
   {[#{payload := a, receipt := A}], Q2} = esq:deq(Q1),
   Q3 = esq:ack(A, Q2),

   timer:sleep(15),
   {[#{payload := b}], Q4} = esq:deq(Q3),
   timer:sleep(15),
   {[#{payload := b}], Q5} = esq:deq(Q4),
   ok = esq:free(Q5).


