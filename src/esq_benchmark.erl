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

new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      {ok, init()}
   catch Error:Reason ->
      lager:error("~p:~p ~p", [Error, Reason, erlang:get_stacktrace()]),
      halt(1)
   end.

%%
%% 
run(enq, _KeyGen, ValGen, S) ->
   %Val = ValGen(),
   %<<Pri:4/integer, _/binary>> = Val,
   case esq:enq(queue, random:uniform(16#ffffffff), ValGen()) of
      ok              -> {ok, S};
      {error, Reason} -> {error, Reason, S}
   end;

run(deq, _KeyGen, _ValGen, S) ->
   case esq:deq(queue, random:uniform(10)) of
      {error, Reason} -> {error, Reason, S};
      []              -> {error, not_found, S};
      _               -> {ok, S}
   end.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
init() ->
   case applib:boot(esq, []) of
      {error, {already_started, _}} -> 
         ok;
      _ ->
         {ok, _} = esq:start_link(queue)
         % Host    = basho_bench_config:get(esq_node,  [{host, localhost}, {pool, 30}]),
         % Queue   = basho_bench_config:get(esq_queue, []),
         % {ok, _} = mets:start_link(esq,   Host),
         
         % {ok, _} = esq:start_link(oqueue, Queue)
   end.

% is_consumer(Id) ->
%    case basho_bench_config:get(esq_consumer, 0) of
%       N when N >= Id -> consumer;
%       _              -> publisher
%    end.

% is_priority() ->
%    Queue   = basho_bench_config:get(esq_queue, []),
%    opts:get([priority], simple, Queue).


