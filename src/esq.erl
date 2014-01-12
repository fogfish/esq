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
-module(esq).
-include("esq.hrl").

-export([start/0]).
-export([
   start_link/1, 
   start_link/2,
   ioctl/2, 
   ioctl/3,
   enq/2, 
   enq/3,
   enq_/2,
   enq_/3,
   deq/1, 
   deq/2, 
   deq/3
]).

-type(msg() :: any()).
-type(pri() :: integer()).


%%
%% start application
-spec(start/0 :: () -> ok).

start() ->
   applib:boot(?MODULE, []).

%%
%% create new queue
-spec(start_link/1 :: (any()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (any(), list()) -> {ok, pid()} | {error, any()}).

start_link(Name)
 when is_atom(Name) ->
   start_link(Name, []);
start_link(Opts)
 when is_list(Opts) ->
   start_link(undefined, Opts).

start_link(Name, Opts) ->
   case opts:get([heap, spool, fspool, tspool], heap, Opts) of
      heap ->
         esq_queue:start_link(Name, esq_heap, Opts);
      {spool,  _} ->
         esq_queue:start_link(Name, esq_spool_dets, Opts);
      {fspool, _} ->
         esq_queue:start_link(Name, esq_spool_fs, Opts);
      {tspool, _} ->
         esq_queue:start_link(Name, esq_spool_text, Opts)   
   end.

%%
%% synchronous message enqueue
-spec(enq/2  :: (pid(), msg()) -> ok | {error, any()}).
-spec(enq/3  :: (pid(), pri(), msg()) -> ok | {error, any()}).

enq(Pid, Msg) ->
   enq(Pid, ?ESQ_PRI_LOW, Msg).

enq(Pid, Pri, Msg)
 when is_integer(Pri) ->  
   gen_server:call(Pid, {enq, Pri, Msg}, ?ESQ_TIMEOUT).


%%
%% asynchronous message enqueue
-spec(enq_/2  :: (pid(), msg()) -> ok | {error, any()}).
-spec(enq_/3  :: (pid(), pri(), msg()) -> ok | {error, any()}).

enq_(Pid, Msg) ->
   enq_(Pid, ?ESQ_PRI_LOW, Msg).

enq_(Pid, Pri, Msg)
 when is_integer(Pri) ->  
   gen_server:cast(Pid, {enq, Pri, Msg}).


%%
%% dequeue message
-spec(deq/1 :: (pid()) -> [msg()] | {error, any()}).
-spec(deq/2 :: (pid(), pri()) -> [msg()] | {error, any()}).

deq(Pid) ->
   deq(Pid, 1).

deq(Pid, N)
 when is_integer(N) ->
   deq(Pid, ?ESQ_PRI_HIGH, N).

deq(Pid, Pri, N)
 when is_integer(Pri), is_integer(N) ->
   gen_server:call(Pid, {deq, Pri, N}, ?ESQ_TIMEOUT).


%%
%% queue i/o control:
%%    capacity  - set high water mark for queue outstanding messages
%%    length    - number of messages in queue (read-only)
%%    inbound   - 
%%    outbound  -
-spec(ioctl/2 :: (any(), pid()) -> any() | undefined).
-spec(ioctl/3 :: (atom(), any(), pid()) -> ok | {error, any()}).

ioctl(Req, Pid) ->
   gen_server:call(Pid, {ioctl, Req}).

ioctl(Key, Val, Pid) ->
   ioctl({Key, Val}, Pid).




