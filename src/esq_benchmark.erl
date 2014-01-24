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
   new/1, run/4
]).

%% internal state
-record(fsm, {
   pri   = undefined :: integer(),  % priority range
   batch = undefined :: integer()   % size of batch operation
}).

%%
new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      _ = init(),
      {ok,
         #fsm{
            pri   = basho_bench_config:get(esq_priority, 16#ffffffff),
            batch = basho_bench_config:get(esq_batch,    1)
         } 
      }
   catch Error:Reason ->
      lager:error("~p:~p ~p", [Error, Reason, erlang:get_stacktrace()]),
      halt(1)
   end.

%% 
run(enq, _KeyGen, ValGen, S) ->
   N   = random:uniform(S#fsm.batch),
   Pri = random:uniform(S#fsm.pri),
   Msg = [ValGen() || _ <- lists:seq(1, N)],
   case esq:enq(queue, Pri, Msg) of
      ok              -> {ok, S};
      {error, Reason} -> {error, Reason, S}
   end;

run(deq, _KeyGen, _ValGen, S) ->
   N = random:uniform(S#fsm.batch),
   case esq:deq(queue, N) of
      {error, Reason} -> {error, Reason, S};
      []              -> {error, not_found, S};
      _               -> {ok, S}
   end.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%% init application
init() ->
   case applib:boot(esq, []) of
      {error, {already_started, _}} -> 
         ok;
      _ ->
         {ok, _} = esq:start_link(queue, basho_bench_config:get(esq_queue, []))
   end.


