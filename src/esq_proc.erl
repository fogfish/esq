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
   evict/2,
   enq/4,
   deq/3
]).


%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

init(_Opts) ->
   {ok, 0, q:new()}.

free(_, _Queue) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% evict expired messages
evict(T, Queue) ->
   evict(0, T, Queue).

evict(N, _T, {}=Queue) ->
   {ok, N, Queue};
evict(N,  T, Queue) ->
   case q:head(Queue) of
      {undefined, _} ->
         {ok, N, Queue};

      {X, _} when X =< T ->
         evict(N + 1, T, q:tail(Queue));
         
      _ ->
         {ok, N, Queue}
   end.

%%
%% enqueue message
enq(TTL, _Pri, Msg, Queue) ->
   {ok, q:enq({TTL, Msg}, Queue)}.

%%
%% dequeue message
deq(_Pri, N, Queue) ->
   take([], N, Queue).

take(Acc, _, {} = Queue) ->
   {ok, lists:reverse(Acc), Queue};
take(Acc, 0, Queue) ->
   {ok, lists:reverse(Acc), Queue};
take(Acc, N, Queue) ->
   {{_, Msg}, NQueue} = q:deq(Queue),
   take([Msg|Acc], N - 1, NQueue).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

