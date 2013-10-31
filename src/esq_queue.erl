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

   length   = 0         :: integer(),
   mod      = undefined :: atom(),
   q        = undefined :: any()
}).

%%
%% start queue instance
start_link(undefined, Mod, Opts) ->
   gen_server:start_link(?MODULE, [undefined, Mod, Opts], []);
start_link(Name, Mod, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Opts], []).

init([Name, Mod, Opts]) ->
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
%% enqueue message
handle_call({enq, _, _}, _Tx, #queue{inbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({enq, _, _}, _Tx, #queue{capacity=C, length=L}=S) 
 when L =/= inf, L >= C ->
   {reply, {error, busy}, S}; 

handle_call({enq, Pri, Msg}, _Tx, #queue{mod=Mod}=S) ->
   case Mod:enq(Pri, Msg, S#queue.q) of
      {ok, Q} ->
         {reply, ok, 
            S#queue{
               length   = add(S#queue.length,   1),
               inbound  = sub(S#queue.inbound,  1),
               q        = Q
            }
         };
      Error   ->
         {reply, Error, S}
   end;   

%%
%% dequeue message
handle_call({deq, _, _}, _Tx, #queue{outbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({deq, Pri, N}, _Tx, #queue{mod=Mod}=S) ->
   case Mod:deq(Pri, N, S#queue.q) of
      {ok, Msg, Q} ->
         Len      = length(Msg),
         {reply, Msg, 
            S#queue{
               length   = sub(S#queue.length,   Len),
               outbound = sub(S#queue.outbound, Len),
               q        = Q
            }
         };
      Error ->
         {reply, Error, S}
   end;

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
handle_cast({enq, _, _}, #queue{inbound=X}=S)
 when X =< 0 ->
   {noreply, S};

handle_cast({enq, _, _}, #queue{capacity=C, length=L}=S) 
 when L >= C ->
   {noreply, S}; 

handle_cast({enq, Pri, Msg}, #queue{mod=Mod}=S) ->
   case Mod:enq(Pri, Msg, S#queue.q) of
      {ok, Q} ->
         {noreply, 
            S#queue{
               length   = add(S#queue.length,   1),
               inbound  = sub(S#queue.inbound,  1),
               q        = Q
            }
         };
      Error   ->
         {reply, Error, S}
   end;   

handle_cast(_Req, S) ->
   {noreply, S}.

%%
%%  
handle_info(ttl, #queue{mod=Mod}=S) ->
   {ok, Q} = Mod:ttl(S#queue.q),
   {noreply,
      S#queue{
         ttl = tempus:reset(S#queue.ttl, ttl), 
         q   = Q
      }
   };

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
%% plus, minus math for infinity
sub(inf, _) -> inf;
sub(X,   Y) -> X - Y.

add(inf, _) -> inf;
add(X,   Y) -> X + Y.

%%
%%
set_ioctl([{capacity, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{capacity=X});
set_ioctl([{inbound,  X} | Opts], S) ->
   set_ioctl(Opts, S#queue{inbound=X});
set_ioctl([{outbound, X} | Opts], S) ->
   set_ioctl(Opts, S#queue{outbound=X});
set_ioctl([{ttl,      X} | Opts], #queue{ttl=undefined}=S) ->
   set_ioctl(Opts, S#queue{ttl=tempus:event(X * 1000, ttl)});
set_ioctl([{ttl,      X} | Opts], S) ->
   _ = tempus:cancel(S#queue.ttl),
   set_ioctl(Opts, S#queue{ttl=tempus:event(X * 1000, ttl)});
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




