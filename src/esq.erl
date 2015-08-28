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
   enq_/2,
   deq/1, 
   deq/2,
   % deq/3,
   % deq/4
   ack/2
]).

-type(msg() :: any()).


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
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   esq_queue:start_link(Opts).

start_link(Name, Opts) ->
   esq_queue:start_link(Name, Opts).

%%
%% close queue
-spec(close/1 :: (pid()) -> ok).

close(Pid) ->
   gen_server:cast(Pid, close).

%%
%% synchronous message enqueue
-spec(enq/2  :: (pid(), msg()) -> ok | {error, any()}).

enq(Queue, Msg) ->
   gen_server:call(Queue, {enq, Msg}).

%%
%% asynchronous message enqueue
-spec(enq_/2  :: (pid(), msg()) -> ok | {error, any()}).

enq_(Queue, Msg) ->
   gen_server:cast(Queue, {enq, Msg}).

%%
%% dequeue message
-spec(deq/1 :: (pid()) -> {ok, [{uid:l(), msg()}]} | {error, any()}).
-spec(deq/2 :: (pid(), integer()) -> {ok, [{uid:l(), msg()}]} | {error, any()}).
% -spec(deq/3 :: (pid(), pri(), integer()) -> [msg()] | {error, any()}).
% -spec(deq/4 :: (pid(), pri(), integer(), timeout()) -> [msg()] | {error, any()}).

deq(Queue) ->
   deq(Queue, 1).

deq(Queue, N)
 when is_integer(N) ->
   gen_server:call(Queue, {deq, N}).
   % deq(Pid, ?ESQ_PRI_HIGH, N).

% deq(Pid, Pri, N)
%  when is_integer(Pri), is_integer(N) ->
%    deq(Pid, Pri, N, ?ESQ_TIMEOUT).

% deq(Pid, Pri, N, Timeout)
%  when is_integer(Pri), is_integer(N) ->
%    gen_server:call(Pid, {deq, Pri, N}, Timeout).

%%
%% acknowledge message

ack(Queue, Uid) ->
   gen_server:cast(Queue, {ack, Uid}).


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




