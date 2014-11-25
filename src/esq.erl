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
   start_link/1, 
   start_link/2,
   close/1,
   ioctl/2, 
   ioctl/3,
   enq/2, 
   enq/3,
   enq/4,
   enq_/2,
   enq_/3,
   deq/1, 
   deq/2, 
   deq/3,
   deq/4
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
%%  Queue type
%%    proc   - process container stores message as state 
%%    heap   - ets ordered set stores message
%%    spool  - dets stores messages (deprecated)
%%    fspool - binary file stores message on file system
%%    tspool - text file stores message on file system
%%
%%  Options
%%    {capacity, integer()} - limit queue capacity, enqueue operation is rejected
%%    {ttl,      integer()} - defines message time-to-live, expired message are dropped
%%    {ready,    integer()} - queue ready watermark, the message {esq, pid(), integer()}
%%                            is delivered to each subscriber when number of in-flight 
%%                            messages exceeds the defined thresholds   
-spec(start_link/1 :: (any()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (any(), list()) -> {ok, pid()} | {error, any()}).

start_link(Name)
 when is_atom(Name) ->
   start_link(Name, []);
start_link(Opts)
 when is_list(Opts) ->
   start_link(undefined, Opts).

start_link(Name, Opts) ->
   case opts:get([proc, heap, spool, fspool, tspool], proc, Opts) of
      proc ->
         esq_queue:start_link(Name, esq_proc, Opts);
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
%% close queue
-spec(close/1 :: (pid()) -> ok).

close(Pid) ->
   gen_server:call(Pid, close).

%%
%% synchronous message enqueue
-spec(enq/2  :: (pid(), msg()) -> ok | {error, any()}).
-spec(enq/3  :: (pid(), pri(), msg()) -> ok | {error, any()}).
-spec(enq/4  :: (pid(), pri(), msg(), timeout()) -> ok | {error, any()}).

enq(Pid, Msg) ->
   enq(Pid, ?ESQ_PRI_LOW, Msg).

enq(Pid, Pri, Msg)
 when is_integer(Pri) ->  
   enq(Pid, Pri, Msg, ?ESQ_TIMEOUT).

enq(Pid, Pri, Msg, Timeout)
 when is_integer(Pri), is_list(Msg) ->  
   gen_server:call(Pid, {enq, Pri, Msg}, Timeout);

enq(Pid, Pri, Msg, Timeout)
 when is_integer(Pri) ->
   enq(Pid, Pri, [Msg], Timeout).

%%
%% asynchronous message enqueue
-spec(enq_/2  :: (pid(), msg()) -> ok | {error, any()}).
-spec(enq_/3  :: (pid(), pri(), msg()) -> ok | {error, any()}).

enq_(Pid, Msg) ->
   enq_(Pid, ?ESQ_PRI_LOW, Msg).

enq_(Pid, Pri, Msg)
 when is_integer(Pri), is_list(Msg) ->  
   gen_server:cast(Pid, {enq, Pri, Msg});

enq_(Pid, Pri, Msg)
 when is_integer(Pri) ->
   enq_(Pid, Pri, [Msg]).


%%
%% dequeue message
-spec(deq/1 :: (pid()) -> [msg()] | {error, any()}).
-spec(deq/2 :: (pid(), integer()) -> [msg()] | {error, any()}).
-spec(deq/3 :: (pid(), pri(), integer()) -> [msg()] | {error, any()}).
-spec(deq/4 :: (pid(), pri(), integer(), timeout()) -> [msg()] | {error, any()}).

deq(Pid) ->
   deq(Pid, 1).

deq(Pid, N)
 when is_integer(N) ->
   deq(Pid, ?ESQ_PRI_HIGH, N).

deq(Pid, Pri, N)
 when is_integer(Pri), is_integer(N) ->
   deq(Pid, Pri, N, ?ESQ_TIMEOUT).

deq(Pid, Pri, N, Timeout)
 when is_integer(Pri), is_integer(N) ->
   gen_server:call(Pid, {deq, Pri, N}, Timeout).


%%
%% queue i/o control:
%%    capacity  - set high water mark for queue outstanding messages
%%    length    - number of messages in queue (read-only)
%%    inbound   - inbound credit
%%    outbound  - outbound credit
%%    sub       - subscribe process to receive notification when queue is ready
%%    ready     - set ready for reading threshold
-spec(ioctl/2 :: (any(), pid()) -> any() | undefined).
-spec(ioctl/3 :: (atom(), any(), pid()) -> ok | {error, any()}).

ioctl(Req, Pid) ->
   gen_server:call(Pid, {ioctl, Req}, infinity).

ioctl(Key, Val, Pid) ->
   ioctl({Key, Val}, Pid).




