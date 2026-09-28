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
-module(esq_reader).
-include("esq.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
   new/1
  ,free/1
  ,deq/1
  ,length/1
]).

%%
%%
-record(reader, {
   fd     = undefined :: stdio:stream()  %% file descriptor to active segment
  ,root   = undefined :: string()        %% root path to queue segment
  ,file   = undefined :: string()        %% path to active segment
  ,chunk  = <<>>      :: binary()
  ,skipped = 0        :: integer()       %% number of skipped frames in active segment
}).

%%
%%
new(Root) ->
   #reader{root = Root}.

%%
%%
free(_State) ->
   %% @todo: close file description but keep file
   ok.

%%
%%
deq(#reader{chunk = <<>>} = State0) ->
   case open(State0) of
      eof ->
         {eof, State0};
      State1 ->
         deq( read(State1) )   
   end;

deq(#reader{chunk = Chunk0} = State0) ->
   case decode(Chunk0) of
      noent ->
         deq( read(State0) );

      {skip, Chunk1} ->
         deq(skipped(State0#reader{chunk = Chunk1}));

      {msg, Msg, Chunk1} ->
         {Msg, State0#reader{chunk = Chunk1}}
   end.   

%% utility function to check length of file segments 
length(#reader{root = Root}) ->
   esq_reader:length(Root);
length(Root) ->
   File = filename:join([Root, "*", ["q", ?READER]]),
   case filelib:wildcard(File) of
      [] ->
         0;
      _  ->
         inf
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% open stream
open(#reader{fd = undefined, root = Root} = State) ->
   File = filename:join([Root, "*", ["q", ?READER]]),
   case filelib:wildcard(File) of
      [] ->
         eof;
      [Head | _] ->
         {ok, FD} = file:open(Head, [raw, binary, read, {read_ahead, ?CHUNK}]),
         State#reader{fd = FD, file = Head}         
   end;

open(State) ->
   State.
 
%%
%% close any open file and rotate active head
close(#reader{fd = undefined} = State) ->
   State;

close(#reader{fd = FD, file = File, skipped = Skipped} = State) ->
   Skipped > 1 andalso
      ?LOG_ERROR("esq: skipped ~b corrupted or undecodable frames in segment ~s",
         [Skipped, File]),
   ok = file:close(FD),
   ok = file:delete(File),
   file:del_dir(filename:dirname(File)), 
   State#reader{fd = undefined, file = undefined, chunk = <<>>, skipped = 0}.

%%
%% count skipped frame, log the first one in segment immediately,
%% the total is logged once segment is closed
skipped(#reader{file = File, skipped = 0} = State) ->
   ?LOG_ERROR("esq: skipped corrupted or undecodable frame in segment ~s, "
      "further frames will be counted and reported when segment is closed",
      [File]),
   State#reader{skipped = 1};

skipped(#reader{skipped = Skipped} = State) ->
   State#reader{skipped = Skipped + 1}.

%%
%% read chunk of data
read(#reader{fd = FD, chunk = Head} = State) ->
   case file:read(FD, ?CHUNK) of
      eof ->
         close(State);
      {ok, Chunk} ->
         State#reader{chunk = <<Head/binary, Chunk/binary>>}
   end.

%%
%% decode message from memory buffer
decode(<<0:16, Len:32, Hash:32, Tail/binary>>) ->
   case byte_size(Tail) of
      X when X < Len ->
         noent;
      _ ->
         <<Msg:Len/binary, Rest/binary>> = Tail,
         case ?HASH32(Msg) of
            Hash -> binary_to_term_or_skip(Msg, Rest);
            _    -> {skip, Rest}
         end
   end;

decode(X)
 when byte_size(X) < 64 ->
   noent;

decode(<<_:8, Tail/binary>>) ->
   decode(Tail).

%%
%% skip message that cannot be decoded (e.g. encoded by incompatible OTP release)
%% instead of crashing on it again and again after each restart
binary_to_term_or_skip(Msg, Rest) ->
   try
      {msg, erlang:binary_to_term(Msg), Rest}
   catch error:badarg ->
      {skip, Rest}
   end.


