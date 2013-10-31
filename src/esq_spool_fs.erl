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
   enq/3,
   deq/3
]).

%% internal state
-record(spool, {
   fs   = undefined  :: list(),        %% path to spool folder
   iq   = undefined  :: any(),         %% 
   oq   = undefined  :: any(),         %%

   written = 0         :: integer(),  %% number of written bytes to segment
   segment = undefined :: integer(),
   content = undefined :: atom()
}).

-define(DEFAULT_CONTENT, binary).
-define(DEFAULT_SEGMENT, 512 * 1024).
-define(WRITER_EXT,      ".spool").
-define(READER_EXT,      ".[0-9]*").

-define(WRITER_MODE,     [raw, binary, append, exclusive]).
-define(TYPE_BIN,        0).
-define(TYPE_ERL,        1).
-define(READER_MODE,     [raw, binary, {read_ahead, 16 * 1024}]).


init(Opts) ->
   Fs  = opts:val(fspool, Opts),
   _   = esq_file:rotate(Fs ++ ?WRITER_EXT),
   {ok, inf, 
      #spool{
         fs      = Fs,
         segment = opts:val(segment, ?DEFAULT_SEGMENT, Opts),
         content = opts:val(content, ?DEFAULT_CONTENT, Opts)
      }
   }.

free(_, #spool{}=S) ->
   esq_file:close(S#spool.oq),
   esq_file:close(S#spool.iq).


%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% enqueue message
enq(_Pri, Msg, S) 
 when is_binary(Msg) ->
   enq_to_file(?TYPE_BIN, Msg, S);

enq(_Pri, _Msg, #spool{content=text}) ->
   {error, not_supported};
enq(_Pri, Msg, S) ->
   enq_to_file(?TYPE_ERL, erlang:term_to_binary(Msg), S).

%%
%%
enq_to_file(Type, Msg, #spool{oq=undefined}=S) ->
   case esq_file:writer(S#spool.fs ++ ?WRITER_EXT, ?WRITER_MODE) of
      {ok, File} -> enq_to_file(Type, Msg, S#spool{oq=File});
      Error      -> Error
   end;

enq_to_file(Type, Msg, #spool{content=binary}=S) ->
   case esq_file:write_record(S#spool.oq, Type, Msg) of
      {ok, Size} ->
         {ok, rotate_file(S#spool{written = S#spool.written + Size})};
      Error ->
         Error
   end;
enq_to_file(_Type, Msg, #spool{content=text}=S) ->
   case esq_file:write_string(S#spool.oq, Msg) of
      {ok, Size} ->
         {ok, rotate_file(S#spool{written = S#spool.written + Size})};
      Error ->
         Error
   end.

%%
%%
rotate_file(#spool{written=Out, segment=Len}=S)
 when Out > Len ->
   esq_file:rotate(S#spool.oq),
   S#spool{
      oq      = undefined,
      written = 0
   };
rotate_file(S) ->
   S.
 
%%
%% dequeue message
deq(_Pri, _N, #spool{content=text}) ->
   {error, not_supported};
deq(Pri, N, #spool{iq=undefined}=S) ->
   case esq_file:reader(S#spool.fs ++ ?READER_EXT, ?READER_MODE) of
      {error, enoent} -> {ok, [], S};
      {ok,      File} -> deq(Pri, N, S#spool{iq=File});
      Error           -> Error
   end;

deq(_Pri, N, S) ->
   case deq_from_file(S#spool.iq, N, []) of
      {ok,  []} ->
         esq_file:remove(S#spool.iq),
         {ok, [], S#spool{iq=undefined}};
      {ok, Msg} ->
         {ok, Msg, S};
      Error     ->
         Error
   end.

deq_from_file(_File, 0, Acc) ->
   {ok, lists:reverse(Acc)};
deq_from_file(File, N, Acc) ->
   case esq_file:read_record(File) of
      {ok, ?TYPE_BIN, Msg} ->
         deq_from_file(File, N - 1, [Msg | Acc]);
      {ok, ?TYPE_ERL, Msg} ->
         deq_from_file(File, N - 1, [erlang:binary_to_term(Msg) | Acc]);
      eof   ->
         deq_from_file(File, 0, Acc);    
      Error ->
         Error
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

