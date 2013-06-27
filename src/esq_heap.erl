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
   % ioctl
   capacity = inf  :: inf | integer(),
   length   = 0    :: integer(),
   inbound  = inf  :: inf | integer(),
   outbound = inf  :: inf | integer(),

   %%
   seq      = 0          :: integer(), %% queue seq number
   heap     = undefined  :: ets:tid()  %% queue heap
}).

%%
%% start queue instance
start_link(Name, Opts) ->
   gen_server:start_link(?MODULE, [Name, Opts], []).

init([Name, Opts]) ->
   register(Name),
   {ok, set_ioctl(Opts, #queue{heap=ets:new(undefined, [ordered_set, protected])})}.

terminate(_, S) ->
   ets:delete(S#queue.heap),
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message
handle_call({enq, _, _}, _Tx, #queue{inbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({enq, _, _}, _Tx, #queue{capacity=C, length=L}=S) 
 when L >= C ->
   {reply, {error, busy}, S}; 

handle_call({enq, Pri, Msg}, _Tx, S) ->
   Seq = seq(S#queue.seq),
   _   = ets:insert(S#queue.heap, {{Pri, Seq}, Msg}),
   {reply, ok, 
      S#queue{
         seq      = Seq, 
         length   = S#queue.length + 1,
         inbound  = sub(S#queue.inbound,  1)
      }
   };   

%%
%% dequeue message
handle_call({deq, _, _}, _Tx, #queue{outbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({deq, Pri, N}, _Tx, S) ->
   case ets:select(S#queue.heap, [{{{'$1','_'}, '_'}, [{ '>=', '$1', Pri}], ['$_']}], N) of
      '$end_of_table' ->
         {reply, [], S};
      {Msg, _} ->
         lists:foreach(fun({Key, _}) -> ets:delete(S#queue.heap, Key) end, Msg),
         Len = length(Msg),
         {reply, [X || {_, X} <- Msg], 
            S#queue{
               length   = S#queue.length - Len,
               outbound = sub(S#queue.outbound, Len)
            }
         }
   end;

%%
%% ioctl
handle_call({ioctl, Req}, _Tx, S)
 when is_atom(Req) ->
   {reply, {ok, get_ioctl(Req, S)}, S};

handle_call({ioctl, {_, _}=Req}, _Tx, S) ->
   try
      {reply, ok, set_ioctl([Req], S)}
   catch _:Reason ->
      {reply, {error, Reason}, S}
   end;

handle_call({ioctl, Req}, _Tx, S)
 when is_list(Req) ->
   try
      {reply, ok, set_ioctl(Req, S)}
   catch _:Reason ->
      {reply, {error, Reason}, S}
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

%%
%% seq
seq(N) when N < 16#ffffffff ->
   N + 1;
seq(_) ->
   0.

%%
%%
sub(inf, _) -> inf;
sub(X,   Y) -> X - Y.

add(inf, _) -> inf;
add(X,   Y) -> X + Y.

%%
%% register queue
register(Name) 
 when is_atom(Name) ->
   erlang:register(Name, self());
register(Fun) 
 when is_function(Fun) ->
   Fun().

%%
%%
set_ioctl([{capacity, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{capacity=X});
set_ioctl([{inbound,  X} | Opts], S) ->
   set_ioctl(Opts, S#queue{inbound=X});
set_ioctl([{outbound, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{outbound=X});
set_ioctl([_ | Opts], S) ->
   set_ioctl(Opts, S);
set_ioctl([], S) ->
   S.

get_ioctl(capacity, S) ->
   S#queue.capacity;
get_ioctl(length, S) ->
   S#queue.length;
get_ioctl(inbound, S) ->
   S#queue.inbound;
get_ioctl(outbound, S) ->
   S#queue.outbound;
get_ioctl(_, _) ->
   undefined.




