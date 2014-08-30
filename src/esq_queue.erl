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
%%    queue container process
-module(esq_queue).
-behaviour(gen_server).

-export([
   start_link/3,
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,  
   code_change/3
]).

%% internal state
-record(queue, {
   % ioctl
   capacity = inf  :: inf | integer(),
   inbound  = inf  :: inf | integer(),
   outbound = inf  :: inf | integer(),
   ttl      = undefined :: any(),

   ready    = undefined :: any(), %% queue ready capacity
   sub      = []        :: any(), %% subscribed processes

   length   = 0         :: integer(), %% number of elements in queue
   mod      = undefined :: atom(),
   q        = undefined :: any()
}).

%%
%% start queue instance
start_link(undefined, Mod, Opts) ->
   gen_server:start_link(?MODULE, [undefined, Mod, Opts], []);
start_link(Name, Mod, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Opts], []).

init([_Name, Mod, Opts]) ->
   {ok, Len, Q} = Mod:init(Opts),
   {ok, set_ioctl(Opts, #queue{mod = Mod, q = Q, length = Len})}.

terminate(Reason, #queue{mod=Mod}=S) ->
   Mod:free(Reason, S#queue.q).

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   


%%
%% 
handle_call(close, _Tx, State) ->
   {stop, normal, ok, State};

handle_call({enq, Pri, Msg}, _Tx, State0) ->
   {Result, State} = enqueue(Pri, Msg, State0),
   {reply, Result, State};

handle_call({deq, Pri, N}, _Tx, State0) ->
   {Result, State} = dequeue(Pri, N, State0),
   {reply, Result, State};


%%
%% ioctl
handle_call({ioctl, Req}, _Tx, S)
 when is_atom(Req) ->
   {reply, get_ioctl(Req, S), S};

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
%% enqueue message
handle_cast({enq, Pri, Msg}, State0) ->
   {_, State} = enqueue(Pri, Msg, State0),
   {noreply, State};

handle_cast(_Req, S) ->
   {noreply, S}.


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
%% plus, minus math with infinity
sub(inf, _) -> inf;
sub(X,   Y) -> X - Y.

add(inf, _) -> inf;
add(X,   Y) -> X + Y.

%%
%% set / get queue ioctrls
set_ioctl([{capacity, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{capacity=X});
set_ioctl([{inbound,  X} | Opts], S) ->
   set_ioctl(Opts, S#queue{inbound=X});
set_ioctl([{outbound, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{outbound=X});
set_ioctl([{ttl,      X} | Opts], S) ->
   set_ioctl(Opts, S#queue{ttl=X});
set_ioctl([{ready,    X} | Opts], S) ->
   set_ioctl(Opts, S#queue{ready=X});
set_ioctl([{sub,      X} | Opts], #queue{ready=R}=S)
 when is_integer(R) ->
   set_ioctl(Opts, pubsub(S#queue{sub=[X | S#queue.sub]}));   
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

%%
%% enqueue message(s)
enqueue(Pri, Msg, State) ->
   try
      {ok, pubsub(enq(Pri, Msg, evict(State)))}
   catch throw:Error ->
      {Error, State}
   end.

%%
%% dequeue message(s)
dequeue(Pri, N, State) ->
   try
      deq(Pri, N, evict(State))
   catch throw:Error ->
      {Error, State}
   end.

%%
%%
enq(_Pri, _Msg, #queue{inbound=X})
 when X =< 0 ->
   %% inbound credits are consumer, queue waits for deq operation to increase credit 
   throw({error, busy});

enq(_Pri, _Msg, #queue{capacity=C, length=L})
 when C =/= inf, L >= C ->
   %% queue is out of capacity
   throw({error, busy});


enq(Pri, Msg, State) ->
   TTL = ttl(State#queue.ttl),
   lists:foldl(
      fun(X, Acc) -> enq_msg(TTL, Pri, X, Acc) end,
      State,
      Msg
   ).

%%
%%
deq(_Pri, _N, #queue{outbound=X})
 when X =< 0 ->
   throw({error, busy});

deq(Pri, N, State) ->
   deq_msg(Pri, N, State).

%%
%%
enq_msg(TTL, Pri, Msg, #queue{mod=Mod}=State) ->
   case Mod:enq(TTL, Pri, Msg, State#queue.q) of
      {ok, Queue} ->
         State#queue{
            length   = add(State#queue.length,   1),
            inbound  = sub(State#queue.inbound,  1),
            q        = Queue
         };
      Error   ->
         throw(Error)
   end.

%%
%%
deq_msg(Pri, N, #queue{mod=Mod}=State) ->
   case Mod:deq(Pri, N, State#queue.q) of
      {ok, Msg, Q} ->
         Len = length(Msg),
         {Msg, 
            State#queue{
               length   = sub(State#queue.length,   Len),
               outbound = sub(State#queue.outbound, Len),
               q        = Q
            }
         };
      Error ->
         throw(Error)
   end.

%%
%% notify subscribers
pubsub(#queue{ready=R, length=L}=State)
 when is_integer(R), L >= R ->
   % notify subscribed queues, messages are available
   _ = lists:foreach(
      fun(X) ->
         erlang:send(X, {esq, self(), L})
      end,
      State#queue.sub
   ),
   State#queue{sub=[]};
pubsub(State) ->
   State.

%%
%% evict expired message
evict(#queue{mod=Mod}=State) ->
   case Mod:evict(os:timestamp(), State#queue.q) of
      {ok, N, Queue} ->
         State#queue{
            length   = sub(State#queue.length,   N),
            outbound = sub(State#queue.outbound, N),
            q        = Queue
         };
      Error ->
         throw(Error)
   end.

%%
%% return ttl value
ttl(undefined) ->
   undefined;
ttl(TTL) ->
   tempus:add(os:timestamp(), TTL).

