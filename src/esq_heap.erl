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
%%    simple in-memory queue
-module(esq_heap).

-export([
   init/1,
   free/2,
   enq/3,
   deq/3
]).

%% internal state
-record(queue, {
   seq      = 0          :: integer(), %% queue seq number
   heap     = undefined  :: ets:tid()  %% queue heap
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

init(_Opts) ->
   {ok, 0,
      #queue{
         heap = ets:new(undefined, [ordered_set, protected])
      }
   }.

free(_, S) ->
   _ = ets:delete(S#queue.heap),
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message
enq(Pri, Msg, S) ->
   Seq = seq(S#queue.seq),
   _   = ets:insert(S#queue.heap, {{Pri, Seq}, Msg}),
   {ok, 
      S#queue{
         seq      = Seq 
      }
   }.

%%
%% dequeue message
deq(Pri, N, S) ->
   case ets:select(S#queue.heap, [{{{'$1','_'}, '_'}, [{ '>=', '$1', Pri}], ['$_']}], N) of
      '$end_of_table' ->
         {ok, [], S};
      {Msg, _} ->
         lists:foreach(fun({Key, _}) -> ets:delete(S#queue.heap, Key) end, Msg),
         {ok, [X || {_, X} <- Msg], S}
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% seq
seq(N) when N < 16#ffffffff ->
   N + 1;
seq(_) ->
   0.

