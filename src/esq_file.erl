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

