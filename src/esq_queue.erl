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
%%   queue container process
-module(esq_queue).
-behaviour(gen_server).

-include("esq.hrl").

-export([
   start_link/1,
   start_link/2,
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,  
   code_change/3
]).

%%
%% internal state
-record(queue, {
   q        = undefined :: datum:q()     %% in-memory message queue
  ,qf       = undefined :: any()         %% overflow message queue 
  ,inflight = undefined :: datum:heap()  %% in-flight message heap
  ,capacity = undefined :: integer()     %% number of message to keep in-memory

  ,ttf      = undefined :: any()         %% message visibility timeout, time to keep message in-flight queue
  ,ttl      = undefined :: any()         %% message time-to-live
  ,tts      = undefined :: any()         %% message time-to-spool
}).

%%
%% empty queue
-define(NULL,    {}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Opts) ->
   gen_server:start_link(?MODULE, [Opts], []).

start_link(Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

%%
%%
init([Opts]) ->
   {ok,
      lists:foldl(
         fun(X, Acc) -> ioctl(X, Acc) end, 
         init(Opts, #queue{q = deq:new()}),
         Opts
      )
   }. 

init([{fspool, Path} | Opts], State) ->
   init(Opts, State#queue{qf = esq_file:new(Path)});
init([{tts, T} | Opts], State) ->
   init(Opts, State#queue{tts = tempus:timer(T, sync)});
init([_ | Opts], State) ->
   init(Opts, State);
init([], State) ->
   State.

%%
%%
terminate(_Reason, #queue{qf = undefined}) ->
   ok;
terminate(_Reason, #queue{qf = Overflow}) ->
   esq_file:free(Overflow).

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle_call({enq, Msg}, _Tx, State0) ->
   {Result, State1} = enq(Msg, uid:l(), deqf(State0)),
   {reply, Result, State1}; 

handle_call({deq,   N}, _Tx, State0) ->
   {Queue, State1} = deq(N, deqf(ttl(State0))),
   {reply, {ok, deq:list(Queue)}, State1};

%%
%%
handle_call({ioctl, Req}, _Tx, State0) ->
   case ioctl(Req, State0) of
      #queue{} = State1 ->
         {reply, ok, State1};
      Result ->
         {reply, Result, State0}
   end;
  
handle_call(_Req, _Tx, State) ->
   {noreply, State}.


%%
%%
handle_cast({enq, Msg}, State0) ->
   {_, State1} = enq(Msg, uid:l(), deqf(State0)),
   {noreply, State1}; 

handle_cast(close, State) ->
   {stop, normal, State};

handle_cast({ack, Uid}, State) ->
   {noreply, ack(Uid, State)};

handle_cast(_Req, State) ->
   {noreply, State}.


%%
%%
handle_info(sync, #queue{qf = undefined} = State) ->
   {noreply, State};

handle_info(sync, #queue{tts = T, qf = File} = State) ->
   {noreply, State#queue{tts = tempus:reset(T, sync), qf = esq_file:sync(File)}};

handle_info(_Req, State) ->
   {noreply, State}.


%%
%% 
code_change(_Vsn, State, _Extra) ->
   {ok, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message 
enq(Msg, Uid, #queue{q = Queue, qf = undefined, capacity = C} = State) ->
   case deq:length(Queue) of
      X when X >= C ->
         {{error, ebusy}, State};
      _ ->
         {ok, State#queue{q = deq:enq({Uid, Msg}, Queue)}}
   end;

enq(Msg, Uid, #queue{q = Queue, qf = Overflow, capacity = C} = State) ->
   case {esq_file:length(Overflow), deq:length(Queue)} of
      {X, _} when X =/= 0 ->
         {ok, State#queue{qf = esq_file:enq({Uid, Msg}, Overflow)}};

      {0, X} when X >=  C ->
         {ok, State#queue{qf = esq_file:enq({Uid, Msg}, Overflow)}};

      _ ->
         {ok, State#queue{q = deq:enq({Uid, Msg}, Queue)}}
   end.

%%
%% dequeue message
deq(N, #queue{q = Queue, qf = undefined} = State) ->
   case deq:length(Queue) of
      0 ->
         {Queue, State};
      _ ->
         {Head, Tail} = deq:split(N, Queue),
         enqf(Head, State#queue{q = Tail})
   end;

deq(N, #queue{q = Queue, qf = Overflow, capacity = C} = State) ->
   case deq:length(Queue) of
      0 ->
         {Chunk, File} = esq_file:deq(N + C, Overflow),
         {Head,  Tail} = deq:split(N, Chunk),
         enqf(Head, State#queue{q = Tail, qf = File});

      _ ->
         {Head, Tail} = deq:split(N, Queue),
         enqf(Head, State#queue{q = Tail})
   end.

%%
%% acknowledge message
ack(_Uid, #queue{inflight = undefined}=State) ->
   State;

ack(Uid, #queue{inflight = Heap0} = State) ->
   Heap1 = heap:dropwhile(fun(X) -> X =< Uid end, Heap0),
   State#queue{inflight = Heap1}.


%%
%% enqueue message to in-flight queue
enqf(Queue, #queue{inflight = undefined} = State) ->
   {Queue, State};

enqf(Queue, State) ->
   enqf(Queue, deq:new(), State).

enqf(?NULL, Acc, State) ->
   {Acc, State};

enqf(Queue, Acc, #queue{inflight = Heap0} = State) ->
   Uid      = uid:l(),
   {_, Msg} = deq:head(Queue),
   Heap1    = heap:insert(Uid, Msg, Heap0), 
   enqf(deq:tail(Queue), deq:enq({Uid, Msg}, Acc), State#queue{inflight = Heap1}).

%%
%% dequeue expired message from in-flight queue to head of message queue
deqf(#queue{inflight = undefined} = State) ->
   State;

deqf(#queue{q = Queue0, inflight = Heap, ttf = TTF} = State) ->
   Uid = uid:l(), 
   {Head, Tail} = heap:splitwith(fun(X) -> uid:t( uid:d(Uid, X) ) > TTF end, Heap),
   Queue1 = lists:foldr(fun(X, Acc) -> deq:poke(X, Acc) end, Queue0, heap:list(Head)),
   State#queue{q = Queue1, inflight = Tail}.


%%
%% remove expired message
ttl(#queue{ttl = undefined} = State) ->
   State;

ttl(#queue{ttl = TTL, q = Queue0} = State) ->
   Uid = uid:l(), 
   Queue1 = deq:dropwhile(fun({X, _}) -> uid:t( uid:d(Uid, X) ) > TTL end, Queue0),
   State#queue{q = Queue1}.


%%
%%
ioctl(ttl, #queue{ttl = X}) ->
   X;
ioctl({ttl, X}, State) ->
   State#queue{ttl = X};

ioctl(ttf, #queue{ttf = X}) ->
   X;
ioctl({ttf, undefined}, State) ->
   State#queue{ttf = undefined, inflight = undefined};
ioctl({ttf, X}, #queue{inflight = undefined} = State) ->
   State#queue{ttf = X, inflight = heap:new()};
ioctl({ttf, X}, State) ->
   State#queue{ttf = X};

ioctl(capacity, #queue{capacity = X}) ->
   X;
ioctl({capacity, X}, State) ->
   State#queue{capacity = X};

ioctl({_, _}, State) ->
   State;
ioctl(_,     _State) ->
   undefined.

