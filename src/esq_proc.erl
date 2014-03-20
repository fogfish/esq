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
%%    simple in-memory queue using datum:q
-module(esq_proc).

-export([
   init/1,
   free/2,
   enq/3,
   deq/3,
   ttl/1
]).


%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

init(_Opts) ->
   {ok, 0, q:new()}.

free(_, S) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message
enq(_Pri, Msg, Queue) ->
   {ok, q:enq(Msg, Queue)}.

%%
%% dequeue message
deq(_Pri, N, Queue) ->
   take([], N, Queue).

take(Acc, _, {} = Queue) ->
   {ok, lists:reverse(Acc), Queue};
take(Acc, 0, Queue) ->
   {ok, lists:reverse(Acc), Queue};
take(Acc, N, Queue) ->
   {Msg, NQueue} = q:deq(Queue),
   take([Msg|Acc], N - 1, NQueue).

%%
%%
ttl(S) ->
   {ok, S}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

