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
%%   erlang simple queue
-module(esq).
-include("esq.hrl").

-export([start/0]).
-export([
   new/1
  ,free/1
  ,enq/2
  ,deq/1
  ,deq/2
  ,ack/2
]).

%%
%% start application
-spec(start/0 :: () -> ok).

start() ->
   applib:boot(?MODULE, []).

%%
%% create new queue 
%%  Options
%%    {ttl,   integer()} - message time-to-live in milliseconds,
%%                         expired messages are evicted from queue
%%    {ttf,   integer()} - message time-to-flight in milliseconds,
%%                         the time required to deliver message acknowledgment before it
%%                         reappears to client(s) again          
%%    {tts,   integer()} - message time-to-sync in milliseconds,
%%                         time to update overflow queue, any overflow message remain invisible
%%                         for read until spool segment is synced.
%%    {fspool, string()} - path to spool
-spec(new/1 :: (list()) -> #q{}).

new(Opts) ->
   new(Opts, #q{head = deq:new()}).

new([{ttl, X} | Opts], State) ->
   new(Opts, State#q{ttl = X});

new([{ttf, X} | Opts], State) ->
   new(Opts, State#q{ttf = X, heap = heap:new()});

new([{tts, X} | Opts], State) ->
   T = X div 1000,
   new(Opts, State#q{tts = T, tte = tempus:add(os:timestamp(), T)});

new([{capacity, X} | Opts], State) ->
   new(Opts, State#q{capacity = X});

new([{fspool, Path} | Opts], State) ->
   new(Opts, State#q{tail = esq_file:new(Path)});

new([_ | Opts], State) ->
   new(Opts, State);

new([], State) ->
   State.

%%
%% close queue and release all resources
-spec(free/1 :: (#q{}) -> ok).

free(#q{tail = undefined}) ->
   ok;
free(#q{tail = Tail}) ->
   esq_file:free(Tail).

%%
%% enqueue message to queue, exit if file operation fails
-spec(enq/2 :: (any(), #q{}) -> #q{}).

enq(E, State) ->
   Uid = os:timestamp(),
   enq(E, Uid, deqf(Uid, ttl(Uid, sync(State)))).

%% enqueue element to head or tail
enq(E, Uid, #q{head = Head, tail = undefined} = State) ->
   State#q{head = deq:enq({Uid, E}, Head)};

enq(E, Uid, #q{head = Head, tail = Tail, capacity = C} = State) ->
   case {esq_file:length(Tail), deq:length(Head)} of
      {X, _} when X =/= 0 ->
         State#q{tail = esq_file:enq({Uid, E}, Tail)};
      {0, X} when X >=  C ->
         State#q{tail = esq_file:enq({Uid, E}, Tail)};
      _ ->
         State#q{head = deq:enq({Uid, E}, Head)}
   end.


%%
%% dequeue message from queue, exit if file operation fails
-spec(deq/1 :: (#q{}) -> {[any()], #q{}}).
-spec(deq/2 :: (integer(), #q{}) -> {[any()], #q{}}).

deq(State) ->
   deq(1, State).

deq(N, State) ->
   Uid = os:timestamp(),
   deq(N, Uid, deqf(Uid, ttl(Uid, sync(State)))).

%%
%% dequeue message
deq(N, _Uid, #q{head = Head, tail = undefined} = State) ->
   case deq:length(Head) of
      0 ->
         {[], State};
      _ ->
         {H, T} = deq:split(N, Head),
         enqf(H, State#q{head = T})
   end;

deq(N, _Uid, #q{head = Head, tail = Tail, capacity = C} = State) ->
   case deq:length(Head) of
      0 ->
         {Chunk, File} = esq_file:deq(N + C, Tail),
         {H, T} = deq:split(N, Chunk),
         enqf(H, State#q{head = T, tail = File});

      _ ->
         {H, T} = deq:split(N, Head),
         enqf(H, State#q{head = T})
   end.

%%
%% acknowledge message
-spec(ack/2 :: (any(), #q{}) -> #q{}).

ack(_Uid, #q{heap = undefined}=State) ->
   State;

ack(Uid,  #q{heap = Heap0} = State) ->
   Heap1 = heap:dropwhile(fun(X) -> X =< Uid end, Heap0),
   State#q{heap = Heap1}.



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% dequeue expired in-flight message to head
deqf(_Uid, #q{heap = undefined} = State) ->
   State;

deqf(Uid,  #q{head = Head0, heap = Heap, ttf = TTF} = State) ->
   {Head, Tail} = heap:splitwith(fun(X) -> (timer:now_diff(Uid, X) div 1000) > TTF end, Heap),
   Head1 = lists:foldr(fun(X, Acc) -> deq:poke(X, Acc) end, Head0, heap:list(Head)),
   State#q{head = Head1, heap = Tail}.

%%
%% enqueue message to in-flight queue
enqf(Queue, #q{heap = undefined} = State) ->
   {deq:list(Queue), State};

enqf(Queue, State) ->
   enqf(Queue, deq:new(), State).

enqf({}, Acc, State) ->
   {deq:list(Acc), State};

enqf(Queue, Acc, #q{heap = Heap0} = State) ->
   Uid    = os:timestamp(),
   {_, E} = deq:head(Queue),
   Heap1  = heap:insert(Uid, E, Heap0), 
   enqf(deq:tail(Queue), deq:enq({Uid, E}, Acc), State#q{heap = Heap1}).

%%
%% remove expired message
ttl(_Uid, #q{ttl = undefined} = State) ->
   State;

ttl(Uid,  #q{head = Head0, ttl = TTL} = State) ->
   Head1 = deq:dropwhile(fun({X, _}) -> (timer:now_diff(Uid, X) div 1000) > TTL end, Head0),
   State#q{head = Head1}.

%%
%% sync file queue
sync(#q{tail = undefined} = State) ->
   State;
sync(#q{tail = Tail, tte = T, tts = Ts}=State) ->
   case os:timestamp() of
      X when X > T ->
         State#q{tail = esq_file:sync(Tail), tte = tempus:add(os:timestamp(), Ts)};
      _ ->
         State
   end.
