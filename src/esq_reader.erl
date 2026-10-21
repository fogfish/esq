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
  ,new/2
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
  ,quarantined = 0    :: integer()       %% number of skipped frames moved to dead letter queue
  ,dlq      = ?DLQ    :: integer()       %% max size of dead letter queue in bytes
  ,dlq_used = 0       :: integer()       %% size of dead letter queue in bytes
  ,dlq_fd   = undefined :: any()         %% file descriptor to dead letter file of active segment
}).

%%
%%
new(Root) ->
   new(Root, []).

new(Root, Opts) ->
   #reader{
      root     = Root
     ,dlq      = proplists:get_value(dlq, Opts, ?DLQ)
     ,dlq_used = dlq_used(Root)
   }.

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

      {skip, Frame, Chunk1} ->
         deq(skipped(Frame, State0#reader{chunk = Chunk1}));

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

close(#reader{fd = FD, file = File} = State) ->
   close_dlq(State),
   ok = file:close(FD),
   ok = file:delete(File),
   file:del_dir(filename:dirname(File)), 
   State#reader{fd = undefined, file = undefined, chunk = <<>>,
      skipped = 0, quarantined = 0, dlq_fd = undefined}.

%%
%% close dead letter file of active segment, report skipped frames
close_dlq(#reader{skipped = 0}) ->
   ok;

close_dlq(State) ->
   close_dlq_file(State),
   log_skipped(State).

close_dlq_file(#reader{dlq_fd = undefined}) ->
   ok;

close_dlq_file(#reader{dlq_fd = FD, file = File, quarantined = 0}) ->
   %% nothing fits into dead letter queue, remove empty file
   ok = file:close(FD),
   ok = file:delete(dlq_file(File));

close_dlq_file(#reader{dlq_fd = FD}) ->
   ok = file:close(FD).

log_skipped(#reader{file = File, skipped = Skipped, quarantined = 0}) ->
   ?LOG_ERROR("esq: skipped ~b corrupted or undecodable frames in segment ~s, "
      "all dropped (dead letter queue is disabled or full)",
      [Skipped, File]);

log_skipped(#reader{file = File, skipped = Skipped, quarantined = Skipped}) ->
   ?LOG_ERROR("esq: skipped ~b corrupted or undecodable frames in segment ~s, "
      "all moved to dead letter file ~s",
      [Skipped, File, dlq_file(File)]);

log_skipped(#reader{file = File, skipped = Skipped, quarantined = Quarantined}) ->
   ?LOG_ERROR("esq: skipped ~b corrupted or undecodable frames in segment ~s, "
      "~b moved to dead letter file ~s, ~b dropped (dead letter queue is full)",
      [Skipped, File, Quarantined, dlq_file(File), Skipped - Quarantined]).

%%
%% count skipped frame, log the first one in segment immediately,
%% the total is logged once segment is closed
skipped(Frame, #reader{file = File, skipped = 0} = State) ->
   ?LOG_ERROR("esq: skipped corrupted or undecodable frame in segment ~s, "
      "further frames will be counted and reported when segment is closed",
      [File]),
   quarantine(Frame, State#reader{skipped = 1});

skipped(Frame, #reader{skipped = Skipped} = State) ->
   quarantine(Frame, State#reader{skipped = Skipped + 1}).

%%
%% write skipped frame to dead letter file of active segment if it fits into dead letter queue
quarantine(_Frame, #reader{dlq = 0} = State) ->
   State;

quarantine(Frame, #reader{dlq_fd = undefined, file = File, dlq_used = Used} = State) ->
   %% segment is read again from the beginning after restart,
   %% dead letter file written by previous reader is re-written
   Dead = dlq_file(File),
   Size = filelib:file_size(Dead),
   {ok, FD} = file:open(Dead, [raw, binary, write]),
   quarantine(Frame, State#reader{dlq_fd = FD, dlq_used = Used - Size});

quarantine(Frame, #reader{dlq = Limit, dlq_used = Used} = State)
 when Used + byte_size(Frame) > Limit ->
   State;

quarantine(Frame, #reader{dlq_fd = FD, dlq_used = Used, quarantined = Quarantined} = State) ->
   ok = file:write(FD, Frame),
   State#reader{dlq_used = Used + byte_size(Frame), quarantined = Quarantined + 1}.

%%
%% dead letter file of segment, it does not match segment pattern
dlq_file(File) ->
   filename:join(filename:dirname(File), "dl" ++ filename:basename(File)).

%%
%% size of all dead letter files in bytes
dlq_used(Root) ->
   File = filename:join([Root, "*", ["dlq", ?READER]]),
   lists:sum([filelib:file_size(X) || X <- filelib:wildcard(File)]).

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
decode(<<0:16, Len:32, Hash:32, Tail/binary>> = Chunk) ->
   case byte_size(Tail) of
      X when X < Len ->
         noent;
      _ ->
         <<Msg:Len/binary, Rest/binary>> = Tail,
         case ?HASH32(Msg) of
            Hash ->
               %% skip message that cannot be decoded (e.g. encoded by incompatible OTP release)
               %% instead of crashing on it again and again after each restart
               try
                  {msg, erlang:binary_to_term(Msg), Rest}
               catch error:badarg ->
                  {skip, frame(Chunk, Rest), Rest}
               end;
            _ ->
               {skip, frame(Chunk, Rest), Rest}
         end
   end;

decode(X)
 when byte_size(X) < 64 ->
   noent;

decode(<<_:8, Tail/binary>>) ->
   decode(Tail).

%%
%% raw frame (header and message) at the beginning of chunk, it is needed only for skipped frame
frame(Chunk, Rest) ->
   binary:part(Chunk, 0, byte_size(Chunk) - byte_size(Rest)).


