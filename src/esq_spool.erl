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
%%    simple spool queue
-module(esq_spool).
-behaviour(gen_server).

-export([
   start_link/2,
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).
-define(DEF_SPOOL, "/var/spool/esq").

%% internal state
-record(queue, {
   % ioctl
   capacity = inf  :: inf | integer(),
   inbound  = inf  :: inf | integer(),
   outbound = inf  :: inf | integer(),

   %%
   spool    = undefined  :: list(),     %% path to spool folder
   q        = []         :: list()      %% list of files TODO: use priority queue
}).

%%
%% start queue instance
start_link(Name, Opts) ->
   gen_server:start_link(?MODULE, [Name, Opts], []).

init([Name, Opts]) ->
   register(Name),
   {ok, init_spool(set_ioctl(Opts, #queue{}))}.

init_spool(S) ->
   Mask = filename:join([S#queue.spool, "*"]),
   ok   = filelib:ensure_dir(Mask),
   S#queue{
      %% TODO: feta.q is legacy use datum
      q = q:new(lists:reverse([filename:basename(X) ||  X <- filelib:wildcard(Mask)]))
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
handle_call({enq, _, _}, _Tx, #queue{inbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({enq, Pri, Msg}, _Tx, S) ->
   case q:length(S#queue.q) of
      % no capacity to take job
      L when L >= S#queue.capacity ->
         {reply, {error, busy}, S}; 

      % there is capacity to take job
      _ ->
         enq_message(Pri, Msg, S)
   end;
 
%%
%% dequeue message
handle_call({deq, _, _}, _Tx, #queue{outbound=X}=S)
 when X =< 0 ->
   {reply, {error, busy}, S};

handle_call({deq, _, _}, _Tx, #queue{q={}}=S) ->
   {reply, [], S};

handle_call({deq, Pri, N}, _Tx, S) ->
   deq_message(Pri, N, S);

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
set_ioctl([{spool,    X} | Opts], S) ->
   set_ioctl(Opts, S#queue{spool=X});
set_ioctl([_ | Opts], S) ->
   set_ioctl(Opts, S);
set_ioctl([], S) ->
   S.

get_ioctl(capacity, S) ->
   S#queue.capacity;
get_ioctl(length, S) ->
   q:length(S#queue.q);
get_ioctl(inbound, S) ->
   S#queue.inbound;
get_ioctl(outbound, S) ->
   S#queue.outbound;
get_ioctl(spool, S) ->
   S#queue.spool;
get_ioctl(_, _) ->
   undefined.

%%
%%
sub(inf, _) -> inf;
sub(X,   Y) -> X - Y.

%%
%% file name
bfile(Pri) ->
   {A, B, C} = erlang:now(),
   lists:flatten(
      io_lib:format("~8.16.0b-~8.16.0b-~8.16.0b-~8.16.0b.bin", [Pri, A, B, C])
   ). 

qfile(Pri) ->
   {A, B, C} = erlang:now(),
   lists:flatten(
      io_lib:format("~8.16.0b-~8.16.0b-~8.16.0b-~8.16.0b.esq", [Pri, A, B, C])
   ). 


%%
%% write message
enq_message(Pri, Msg, S)
 when is_binary(Msg) ->
   enq_message_to_fs(bfile(Pri), Msg, S);
enq_message(Pri, Msg, S) ->
   enq_message_to_fs(qfile(Pri), erlang:term_to_binary(Msg), S).

enq_message_to_fs(File, Msg, S) ->
   MFile = filename:join([S#queue.spool, File]),
   case file:write_file(MFile, Msg) of
      ok  ->
         {reply, ok, 
            S#queue{
               inbound  = sub(S#queue.inbound,  1),
               q        = q:enq(File, S#queue.q)
            }
         };
      Err ->
         {reply, Err, S}
   end.

%%
%% read message
deq_message(_Pri, _N, S) ->
   %% TODO: take N messages
   %% TDOD: use  Pri
   {File, Q} = q:deq(S#queue.q),
   MFile = filename:join([S#queue.spool, File]),
   case file:read_file(MFile) of
      % message consumed
      {ok, Msg} ->
         ok = file:delete(MFile),
         {reply, [to_term(filename:extension(File), Msg)],  
            S#queue{
               outbound  = sub(S#queue.outbound,  1),
               q         = Q
            }
         };

      % queue got out of sync with fs
      {error, enoent} ->
         {reply, [],  S#queue{q = Q}};

      % file read error
      Err ->
         {reply, Err, S}
   end. 

to_term(".bin", Msg) -> Msg;
to_term(".esq", Msg) -> erlang:binary_to_term(Msg);
to_term(_,      Msg) -> Msg.


