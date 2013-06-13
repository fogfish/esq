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
-behaviour(gen_server).

-export([
   start_link/2,
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%% internal state
-record(queue, {
   seq      = 0          :: integer(), % queue seq number
   heap     = undefined  :: ets:tid(), % queue heap
   capacity = undefined  :: integer(), % queue capacity (number of elements)
   used     = 0          :: integer()  % used capacity  
}).

%%
%% start heap instance
start_link(Name, Opts)
 when is_atom(Name) ->
   gen_server:start_link({local, Name}, ?MODULE, [Name, Opts], []);

start_link(Name, Opts) ->
   gen_server:start_link(?MODULE, [Name, Opts], []).

init([Name, Opts])
 when is_atom(Name) ->
   {ok, init(Opts, #queue{})};

init([Name, Opts])
 when is_function(Name) ->
   Name(),
   {ok, init(Opts, #queue{})}.

init([{capacity, X} | Opts], S) ->
   init(Opts, S#queue{capacity=X});
init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   S#queue{
      heap = ets:new(undefined, [ordered_set, protected])
   }.

terminate(_, _) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message
handle_call({enq, Pri, Msg}, _Tx, #queue{capacity=undefined}=S) ->
   Seq = seq(S#queue.seq),
   _   = ets:insert(S#queue.heap, {{Pri, Seq}, Msg}),
   {reply, ok, S#queue{seq=Seq}};   

handle_call({enq, _, _}, _Tx, S)
 when S#queue.capacity =< S#queue.used ->
   {reply, {error, no_capacity}, S};

handle_call({enq, Pri, Msg}, _Tx, S) ->
   Seq = seq(S#queue.seq),
   _   = ets:insert(S#queue.heap, {{Pri, Seq}, Msg}),
   {reply, ok, S#queue{seq=Seq, used=S#queue.used + 1}}; 

handle_call({'enq_', Pri, Msg}, Tx, S) ->
   handle_call({enq, Pri, Msg}, Tx, S);

%%
%% dequeue message
handle_call({deq, Pri, N}, _Tx, S) ->
   case ets:select(S#queue.heap, [{{{'$1','_'}, '_'}, [{ '>=', '$1', Pri}], ['$_']}], N) of
      '$end_of_table' ->
         {reply, [], S};
      {Msg, _} ->
         lists:foreach(fun({Key, _}) -> ets:delete(S#queue.heap, Key) end, Msg),
         {reply, [X || {_, X} <- Msg], S#queue{used = S#queue.used - length(Msg)}}
   end;

handle_call(_Req, _Tx, S) ->
   {noreply, S}.
   
%%
%%
handle_cast(_Req, S) ->
   {noreply, S}.

%%
%%  
handle_info(_Msg, S) ->
   {noreply, S}.

%%
%% 
code_change(_Vsn, S, _Extra) ->
   {ok, S}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

seq(N) when N < 16#ffffffff ->
   N + 1;
seq(_) ->
   0.




