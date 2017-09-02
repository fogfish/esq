%%
%%   Copyright (c) 2017, Dmitry Kolesnikov
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
%% @doc
%%   in-flight queue
-module(esq_inflight).

-export([
   new/1,
   t/0,
   enq/2,
   deq/2,
   ack/2
]).

-record(inflight, {
   ttf  = undefined :: _,
   q    = undefined :: datum:heap()   
}).

%%
%%
new(TTF) ->
   #inflight{
      ttf = TTF,
      q   = heap:new()
   }.

%%
%%
t() ->
   uid:encode(uid:l()).

%%
%% 
enq(E, #inflight{q = Queue0} = InFlight) ->
   Uid    = t(),
   Queue1 = heap:insert(Uid, E, Queue0), 
   {Uid, InFlight#inflight{q = Queue1}}.

%%
%%
deq(Uid, #inflight{ttf = TTF, q = Queue0} = InFlight) ->
   {Head, Queue1} = heap:splitwith(fun(X) -> diff(Uid, X) > TTF end, Queue0),
   {heap:list(Head), InFlight#inflight{q = Queue1}}.

%%
%%
ack(Uid, #inflight{q = Queue0} = InFlight) ->
   Queue1 = heap:dropwhile(fun(X) -> X =< Uid end, Queue0),
   InFlight#inflight{q = Queue1}.

%%
%%
diff(A, B) ->
   uid:t(uid:d(uid:decode(A), uid:decode(B))).
