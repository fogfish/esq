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
%%    file system spool
%%
%% @todo
%%   * deq N messages
%%   * implement deq priority
-module(esq_spool_fs).

-export([
   init/1,
   free/2,
   evict/2,
   enq/4,
   deq/3
]).

%% internal state
-record(spool, {
   fs  = undefined  :: list()        %% path to spool folder
  ,iq  = undefined  :: any()         %% input  queue file
  ,oq  = undefined  :: any()         %% output queue file

  ,q     = undefined  :: datum:q()   %% in-memory output queue buffer  
  ,dirty = 0          :: integer()   %% number of dirty (in-flight) messages    

  ,written = 0         :: integer()  %% number of written bytes to segment
  ,segment = undefined :: integer()
  ,policy  = undefined :: atom()     %% queue policy
}).

-define(DEFAULT_SEGMENT, 512 * 1024).
-define(DEFAULT_POLICY,  fifo).
-define(WRITER_EXT,      ".spool").
-define(READER_EXT,      ".[0-9]*").

-define(TYPE_BIN,         0).
-define(TYPE_ERL,         1).


init(Opts) ->
   Fs = opts:val(fspool, Opts),
   ok = shift_file(Fs),
   {ok, inf, 
      #spool{
         fs      = Fs
        ,q       = deq:new()
        ,dirty   = opts:val(dirty, 0 , Opts)
        ,segment = opts:val(segment, ?DEFAULT_SEGMENT, Opts)
        ,policy  = opts:val(policy,  ?DEFAULT_POLICY, Opts)
      }
   }.

free(_, #spool{}=S) ->
   close_file(S#spool.oq),
   close_file(S#spool.iq).


%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% evict messages (shift queue segment)
evict(_T, #spool{oq=undefined}=S) ->
   {ok, 0, S};
evict(_T, S) ->
   {ok, File} = esq_file:filename(S#spool.oq),
   _  = esq_file:close(S#spool.oq),
   ok = shift_file(S#spool.fs, File),
   {ok, 0, S#spool{
      oq      = undefined,
      written = 0
   }}.


%%
%% enqueue message
enq(_TTL, _Pri, Msg, #spool{dirty=0}=State) ->
   enq_to_file(encode(Msg), State);
enq(_TTL, _Pri, Msg, #spool{}=State) ->
   Queue = deq:enq(encode(Msg), State#spool.q),
   case deq:length(Queue) of
      X when X >= State#spool.dirty ->
         enq_to_file(deq:list(Queue), State#spool{q=deq:new()});
      _ ->
         {ok, State#spool{q=Queue}}
   end.


%%
%%
enq_to_file(Msg, _)
 when byte_size(Msg) > 16#ffff ->
   {error, overflow}; 
enq_to_file(Msg, #spool{oq=undefined}=S) ->
   case open_writer(S#spool.fs) of
      {ok, FD} -> 
         enq_to_file(Msg, S#spool{oq=FD});
      Error    -> 
         Error
   end;
enq_to_file(Msg, #spool{}=S) ->
   case esq_file:write(S#spool.oq, Msg) of
      {ok, Size} ->
         {ok, shift_queue(S#spool{written = S#spool.written + Size})};
      Error ->
         Error
   end.

 
%%
%% dequeue message
deq(Pri, N, #spool{iq=undefined, policy=Policy}=S) ->
   case open_reader(Policy, S#spool.fs) of
      {error, enoent} -> 
         {Result, Q} = deq_from_dirty(N, S#spool.q),
         {ok, Result, S#spool{q=Q}};
      {ok,  FD} -> 
         deq(Pri, N, S#spool{iq=FD});
      Error -> 
         Error
   end;
deq(Pri, N, S) ->
   case deq_from_file(S#spool.iq, N, []) of
      {ok,  []} ->
         {ok, File} = esq_file:filename(S#spool.iq),
         esq_file:delete(S#spool.iq),
         file:del_dir(filename:dirname(File)),         
         deq(Pri, N, S#spool{iq=undefined});
      {ok, Msg} ->
         {ok, Msg, S};
      Error     ->
         Error
   end.

deq_from_file(_File, 0, Acc) ->
   {ok, lists:reverse(Acc)};
deq_from_file(File, N, Acc) ->
   case esq_file:read(File) of
      {ok, <<?TYPE_BIN:8, Msg/binary>>} ->
         deq_from_file(File, N - 1, [Msg | Acc]);
      {ok, <<?TYPE_ERL:8, Msg/binary>>} ->
         deq_from_file(File, N - 1, [erlang:binary_to_term(Msg) | Acc]);
      eof   ->
         deq_from_file(File, 0, Acc);    
      Error ->
         Error
   end.

deq_from_dirty(_, {}) ->
   {[], {}};
deq_from_dirty(0, Queue) ->
   {[], Queue};
deq_from_dirty(_, Queue) ->
   {[decode(X) || X <- deq:list(Queue)], deq:new()}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% encode queue message
encode(Msg) 
 when is_binary(Msg) ->
   <<?TYPE_BIN:8, Msg/binary>>;
encode(Msg) ->
   <<?TYPE_ERL:8, (erlang:term_to_binary(Msg))/binary>>.

decode(<<?TYPE_BIN:8, Msg/binary>>) ->
   Msg;
decode(<<?TYPE_ERL:8, Msg/binary>>) ->
   erlang:binary_to_term(Msg).

%%
%%
wx_filename(File, Ext) ->
   filename:join([File, tempus:encode("%Y%m%d", os:timestamp()), "q" ++ Ext]).

rx_filename(File, Ext) ->
   filename:join([File, "*", "q" ++ Ext]).

%%
%%
open_writer(File) ->
   FName = wx_filename(File, ?WRITER_EXT),
   case filelib:ensure_dir(FName) of
      ok ->
         esq_file:start_link(FName, [append, exclusive]);
      Error ->
         Error
   end.

%%
%%
open_reader(fifo, File) ->
   FName = rx_filename(File, ?READER_EXT),
   case filelib:wildcard(FName) of
      [] ->
         {error, enoent};
      [Head | _] ->
         esq_file:start_link(Head, [])
   end;

open_reader(lifo, File) ->
   FName = rx_filename(File, ?READER_EXT),
   case filelib:wildcard(FName) of
      [] ->
         {error, enoent};
      List ->
         esq_file:start_link(lists:last(List), [])
   end.

%%
%%
close_file(undefined) ->
   ok;
close_file(File) ->
   esq_file:close(File).


%%
%%
shift_file(File) ->
   FName = rx_filename(File, ?WRITER_EXT),
   case filelib:wildcard(FName) of
      [] ->
         ok;
      [Head | _] ->
         ok = shift_file(File, Head),
         ok = shift_file(File)
   end.

shift_file(File, Segment) ->
   {A, B, C} = erlang:now(),
   Now = lists:flatten(
      io_lib:format(".~6..0b~6..0b~6..0b", [A, B, C])
   ),
   New = wx_filename(File, Now),
   ok  = filelib:ensure_dir(New),
   ok  = file:rename(Segment, New).

%%
%%
shift_queue(#spool{written=Out, segment=Len}=S)
 when Out > Len ->
   {ok, File} = esq_file:filename(S#spool.oq),
   _  = esq_file:close(S#spool.oq),
   ok = shift_file(S#spool.fs, File),
   S#spool{
      oq      = undefined,
      written = 0
   };
shift_queue(#spool{}=S) ->
   S.



