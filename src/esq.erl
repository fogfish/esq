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
-compile({no_auto_import,[element/2]}).

-export([start/0]).
-export([
   new/1
  ,new/2
  ,free/1
  ,enq/2
  ,deq/1
  ,deq/2
  ,ack/2
  ,head/1
]).

%%
%% data types
-type payload() :: _.
-type element() :: #{receipt => uid:l(), payload => payload()}.

%%
%% start application
-spec start() -> ok.

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
%%    {capacity, integer()} - size of the head
-spec new(list()) -> #q{}.

new(Path) ->
   new(Path, []).

new(Path, Opts) ->
   config(Opts, 
      #q{
         head     = deq:new(),
         tail     = esq_file:new(Path),
         capacity = 1,
         tts      = 1000
      }
   ).

config([{ttl, X} | Opts], State) ->
   config(Opts, State#q{ttl = X});

config([{ttf, X} | Opts], State) ->
   config(Opts, State#q{ttf = X, heap = heap:new()});

config([{tts, X} | Opts], State) ->
   config(Opts, State#q{tts = X, tte = tempus:add(os:timestamp(), tempus:t(m, X))});

config([{capacity, X} | Opts], State) ->
   config(Opts, State#q{capacity = X});

config([_ | Opts], State) ->
   config(Opts, State);

config([], State) ->
   State.

%%
%% close queue and release all resources
-spec free(#q{}) -> ok.

free(#q{tail = Tail}) ->
   esq_file:free(Tail).

%%
%% enqueue message to queue, exit if file operation fails
-spec enq(payload(), #q{}) -> #q{}.

enq(E, State) ->
   Uid = uid:encode(uid:l()),
   enq_(element(Uid, E), deqf(Uid, ttl(Uid, sync(State)))).

enq_(E, #q{tail = Tail} = State) ->
   State#q{tail = esq_file:enq(E, Tail)}.


%%
%% dequeue message from queue, exit if file operation fails
-spec deq(#q{}) -> {[element()], #q{}}.
-spec deq(integer(), #q{}) -> {[element()], #q{}}.

deq(State) ->
   deq(1, State).

deq(N, State) ->
   Uid = uid:encode(uid:l()),
   deq(N, Uid, deqf(Uid, ttl(Uid, sync(State)))).

%%
%% dequeue message
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
-spec ack(uid:l(), #q{}) -> #q{}.

ack(_Uid, #q{heap = undefined}=State) ->
   State;

ack(Uid,  #q{heap = Heap0} = State) ->
   Heap1 = heap:dropwhile(fun(X) -> X =< Uid end, Heap0),
   State#q{heap = Heap1}.

%%
%%
head(#q{head = {}}) ->
   undefined;

head(#q{head = Queue}) ->
   deq:head(Queue).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
element(Uid, E) ->
   #{receipt => Uid, payload => E}.

element({Uid, E}) ->
   element(Uid, E).

%%
%% dequeue expired in-flight message to head
deqf(_Uid, #q{heap = undefined} = State) ->
   State;

deqf(Uid,  #q{head = Head0, heap = Heap, ttf = TTF} = State) ->
   {Head, Tail} = heap:splitwith(fun(X) -> diff(Uid, X) > TTF end, Heap),
   Head1 = lists:foldr(fun(X, Acc) -> deq:poke(element(X), Acc) end, Head0, heap:list(Head)),
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
   Uid   = uid:encode(uid:l()),
   #{payload := E} = deq:head(Queue),
   Heap1 = heap:insert(Uid, E, Heap0), 
   enqf(deq:tail(Queue), deq:enq(element(Uid, E), Acc), State#q{heap = Heap1}).

%%
%% remove expired message
ttl(_Uid, #q{ttl = undefined} = State) ->
   State;

ttl(Uid,  #q{head = Head0, ttl = TTL} = State) ->
   Head1 = deq:dropwhile(fun(#{receipt := X}) -> diff(Uid, X) > TTL end, Head0),
   State#q{head = Head1}.

%%
%% sync file queue
sync(#q{tail = Tail, tte = Expire, tts = T}=State) ->
   case os:timestamp() of
      X when X > Expire ->
         State#q{
            tail = esq_file:sync(Tail),
            tte  = tempus:add(os:timestamp(), tempus:t(m, T))
         };
      _ ->
         State
   end.

%%
%%
diff(A, B) ->
   uid:t(uid:d(uid:decode(A), uid:decode(B))).

