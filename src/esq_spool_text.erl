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
%%    file system spool - (text spool)
%%
%% @todo
%%   * deq N messages
%%   * implement deq priority
-module(esq_spool_text).

-export([
   init/1,
   free/2,
   enq/3,
   deq/3,
   ttl/1
]).

%% internal state
-record(spool, {
   fs   = undefined  :: list(),        %% path to spool folder
   iq   = undefined  :: any(),         %% input  queue file
   oq   = undefined  :: any(),         %% output queue file

   written = 0         :: integer(),  %% number of written bytes to segment
   segment = undefined :: integer(),
   ttl     = undefined :: any()
}).

-define(DEFAULT_CONTENT, binary).
-define(DEFAULT_SEGMENT, 512 * 1024).
-define(WRITER_EXT,      ".spool").
-define(READER_EXT,      ".[0-9]*").

init(Opts) ->
   Fs  = opts:val(tspool, Opts),
   _   = shift_file(Fs),
   {ok, inf, 
      #spool{
         fs      = Fs,
         segment = opts:val(segment, ?DEFAULT_SEGMENT, Opts)
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
%% enqueue message
enq(_Pri, Msg, S) 
 when is_binary(Msg) ->
   case binary:last(Msg) of
      $\n -> enq_to_file(Msg, S);
      _   -> enq_to_file(<<Msg/binary, $\n>>, S)
   end;
enq(_Pri, Msg, S) ->
   {error, badarg}.

%%
%%
enq_to_file(Msg, #spool{oq=undefined}=S) ->
   case open_writer(S#spool.fs) of
      {ok, FD} -> 
         enq_to_file(Msg, S#spool{oq=FD});
      Error    -> 
         Error
   end;
enq_to_file(Msg, #spool{}=S) ->
   case file:write(S#spool.oq, Msg) of
      ok ->
         {ok, shift_queue(S#spool{written = S#spool.written + byte_size(Msg)})};
      Error ->
         Error
   end.

 
%%
%% dequeue message
deq(Pri, N, #spool{iq=undefined}=S) ->
   case open_reader(S#spool.fs) of
      {error, enoent} -> 
         {ok, [], S};
      {ok,  FD} -> 
         deq(Pri, N, S#spool{iq=FD});
      Error -> 
         Error
   end;
deq(Pri, N, S) ->
   case deq_from_file(S#spool.iq, N, []) of
      {ok,  []} ->
         file:delete(
            hd(filelib:wildcard(S#spool.fs ++ ?READER_EXT))
         ),
         deq(Pri, N, S#spool{iq=undefined});
      {ok, Msg} ->
         {ok, Msg, S};
      Error     ->
         Error
   end.

deq_from_file(_File, 0, Acc) ->
   {ok, lists:reverse(Acc)};
deq_from_file(File, N, Acc) ->
   case file:read_line(File) of
      {ok, Msg} ->
         deq_from_file(File, N - 1, [Msg | Acc]);
      eof   ->
         deq_from_file(File, 0, Acc);    
      Error ->
         Error
   end.

%%
%%
ttl(#spool{oq=undefined}=S) ->
   {ok, S};
ttl(S) ->
   _ = file:close(S#spool.oq),
   _ = shift_file(S#spool.fs),
   {ok, S#spool{
      oq      = undefined,
      written = 0
   }}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
open_writer(File) ->
   case filelib:ensure_dir(File) of
      ok ->
         file:open(File ++ ?WRITER_EXT, [binary, append, exclusive]);
      Error ->
         Error
   end.

%%
%%
open_reader(File) ->
   case filelib:wildcard(File ++ ?READER_EXT) of
      [] ->
         {error, enoent};
      [Head | _] ->
         file:open(Head, [binary])
   end.

%%
%%
close_file(undefined) ->
   ok;
close_file(File) ->
   file:close(File).


%%
%%
shift_file(File) ->
   {A, B, C} = erlang:now(),
   Now = lists:flatten(
      io_lib:format(".~6..0b~6..0b~6..0b", [A, B, C])
   ),
   file:rename(File ++ ?WRITER_EXT, File ++ Now).

%%
%%
shift_queue(#spool{written=Out, segment=Len}=S)
 when Out > Len ->
   _ = file:close(S#spool.oq),
   _ = shift_file(S#spool.fs),
   S#spool{
      oq      = undefined,
      written = 0
   };
shift_queue(#spool{}=S) ->
   S.



