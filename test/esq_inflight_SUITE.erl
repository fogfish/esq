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
-module(esq_inflight_SUITE).
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
   new/1,
   enq/1, 
   deq/1
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
         [new, enq, deq]}
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   Config.

end_per_suite(_Config) ->
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

new(_Config) ->
   Heap = {h, 0, {}},
   {inflight, 1000, Heap} = esq_inflight:new(1000).


enq(_Config) ->
   Element = a, 
   Queue   = esq_inflight:new(100),
   {Uid, {inflight, _, Heap}} = esq_inflight:enq(Element, Queue),
   [{Uid, Element}] = heap:list(Heap).


deq(_Config) ->
   Element = a,
   Queue0  = esq_inflight:new(100),
   {[], Queue0} = esq_inflight:deq(esq_inflight:t(), Queue0 ),

   {Uid, Queue1} = esq_inflight:enq(Element, Queue0),
   {[],  Queue1} = esq_inflight:deq(esq_inflight:t(), Queue1),

   timer:sleep(110),
   {[{Uid, Element}], _} = esq_inflight:deq(esq_inflight:t(), Queue1).
