%%
%%   Copyright (c) 2012, Dmitry Kolesnikov
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
%% @description
%%   basho bench driver
-module(esq_benchmark).

-export([
   new/1
  ,run/4
]).

%%
%%
new(1) ->
   lager:set_loglevel(lager_console_backend, 
      basho_bench_config:get(log_level, info)
   ),
   esq:start(),
   esq:start_link(queue, 
      basho_bench_config:get(queue, [])
   ),
   {ok, queue};
new(_) ->
   {ok, queue}.

%% 
run(enq, _KeyGen, ValGen, Queue) ->
   case esq:enq(Queue, ValGen()) of
      ok              -> {ok, Queue};
      {error, Reason} -> {error, Reason, Queue}
   end;


run(deq, _KeyGen, _ValGen, Queue) ->
   case esq:deq(Queue) of
      {error, Reason} -> {error, Reason, Queue};
      {ok, []}        -> {error, not_found, Queue};
      {ok,  _}        -> {ok, Queue}
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   



