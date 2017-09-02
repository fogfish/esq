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
-type queue()   :: pid(). 

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
-spec new(list()) -> {ok, queue()}.

new(Path) ->
   new(Path, []).

new(Path, Opts) ->
   esq_queue:start_link(Path, Opts).


%%
%% close queue and release all resources
-spec free(queue()) -> ok.

free(Queue) ->
   pipe:free(Queue).


%%
%% enqueue message to queue, exit if file operation fails
-spec enq(payload(), queue()) -> ok.

enq(E, Queue) ->
   pipe:call(Queue, {enq, E}, infinity).


%%
%% dequeue message from queue, exit if file operation fails
-spec deq(queue()) -> [element()].
-spec deq(integer(), queue()) -> [element()].

deq(Queue) ->
   deq(1, Queue).

deq(N, Queue) ->
   pipe:call(Queue, {deq, N}, infinity).


%%
%% acknowledge message
-spec ack(uid:l(), queue()) -> ok.

ack(Uid, Queue) ->
   pipe:call(Queue, {ack, Uid}, infinity).


%%
%%
-spec head(queue()) -> element() | undefined.

head(Queue) ->
   pipe:call(Queue, head, infinity).
   
