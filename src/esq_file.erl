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
-module(esq_file).

-export([
   new/1
  ,free/1
  ,length/1
  ,enq/2
  ,deq/2
  ,sync/1
]).

%%
%% internal state
-record(file, {
   writer = undefined :: any()
  ,reader = undefined :: any()
  ,length = undefined :: integer() 
}).

%%
%%
new(Root) ->
   #file{
      writer = esq_writer:new(Root)
     ,reader = esq_reader:new(Root)
     ,length = esq_reader:length(Root)
   }.

%%
%%
free(#file{writer = Writer, reader = Reader}) ->
   esq_writer:free(Writer),
   esq_reader:free(Reader).

%%
%%
length(#file{length = X}) ->
   X.

%%
%%
enq(Msg, #file{writer = Writer, length = Len}=State) ->
   State#file{
      writer = esq_writer:enq(Msg, Writer),
      length = inc(Len)
   }.

%%
%%
deq(N, State) ->
   deq(N, deq:new(), State).
deq(0, Acc, State) ->
   {Acc, State};
deq(N, Acc, #file{reader = Reader0, writer = Writer, length = Len}=State) ->
   case esq_reader:deq(Reader0) of
      {eof, Reader1} ->
         case esq_writer:length(Writer) of
            0 ->
               {Acc, State#file{reader = Reader1, length = 0}};
            _ ->
               {Acc, State#file{reader = Reader1, length = dec(Len)}}
         end;
      {Msg, Reader1} ->
         deq(N - 1, deq:enq(Msg, Acc), State#file{reader = Reader1, length = dec(Len)})
   end.

%%
%%
sync(#file{writer = Writer} = State) ->
   State#file{writer = esq_writer:close(Writer)}.


inc(inf) -> inf;
inc(X)   -> X+1.

dec(inf) -> inf;
dec(X)   -> X-1.

