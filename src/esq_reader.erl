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

      {<<>>, Chunk1} ->
         deq(State0#reader{chunk = Chunk1});

      {Msg,  Chunk1} ->
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
   ok = file:close(FD),
   ok = file:delete(File),
   file:del_dir(filename:dirname(File)), 
   State#reader{fd = undefined, file = undefined, chunk = <<>>}.

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
decode(<<0:16, Len:16, Hash:32, Tail/binary>>) ->
   case byte_size(Tail) of
      X when X < Len ->
         noent;
      _ ->
         <<Msg:Len/binary, Rest/binary>> = Tail,
         case ?HASH32(Msg) of
            Hash -> {erlang:binary_to_term(Msg),  Rest};
            _    -> {<<>>, Rest}
         end
   end;

decode(X)
 when byte_size(X) < 64 ->
   noent;

decode(<<_:8, Tail/binary>>) ->
   decode(Tail).


