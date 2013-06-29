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

-export([
   start/0,

   start_link/1, start_link/2,
   ioctl/2, ioctl/3,

   enq/2, enq/3,
   deq/1, deq/2, deq/3
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

start_link(Name) ->
   start_link(Name, []).

start_link(Name, Opts) ->
   start_link(opts:get([heap, spool], heap, Opts), Name, Opts).

start_link(heap,  Name, Opts) ->
   esq_heap:start_link(Name, Opts);
start_link({spool, _}, Name, Opts) ->
   esq_spool:start_link(Name, Opts).

%%
%% enqueue message
-spec(enq/2  :: (pid(), msg()) -> ok | {error, any()}).
-spec(enq/3  :: (pid(), pri(), msg()) -> ok | {error, any()}).

enq(Pid, Msg) ->
   enq(Pid, ?ESQ_PRI_LOW, Msg).

enq(Pid, Pri, Msg)
 when is_integer(Pri) ->  
   gen_server:call(Pid, {enq, Pri, Msg}, ?ESQ_TIMEOUT).

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




