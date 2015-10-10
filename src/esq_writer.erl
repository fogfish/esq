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
-module(esq_writer).
-include("esq.hrl").

-export([
   new/1
  ,free/1
  ,length/1
  ,enq/2
  ,close/1
]).

%%
%%
-record(writer, {
   fd      = undefined :: any()     %% file description to write message
  ,root    = undefined :: string()  %% root path to queue segments 
  ,file    = undefined :: string()  %% path to active segment 
  ,written = 0         :: integer() %% number of written bytes  
}).

%%
%%
new(Root) ->
   close(#writer{root = Root}).

%%
%%
free(State) ->
   _ = close(State), 
   ok.

%%
%%
enq(Msg, State0) ->
   #writer{fd = FD, written = Out} = State1 = open(State0),
   Pack = encode(Msg),
   ok   = file:write(FD, Pack),
   shift(State1#writer{written = Out + erlang:iolist_size(Pack)}).

%%
%% number of written messages
length(#writer{fd = undefined}) ->
   0;
length(_) ->
   inf.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% open file
open(#writer{fd = undefined, root = Root} = State) ->
   Now  = tempus:encode("%Y%m%d", os:timestamp()),
   File = filename:join([Root, Now, "q" ++ ?WRITER]),
   ok   = filelib:ensure_dir(File),
   {ok, FD} = file:open(File, [raw, binary, append, exclusive, {delayed_write, ?CHUNK, ?DELAY}]),
   State#writer{fd = FD, file = File};

open(State) ->
   State.

%%
%% close any open file and rotate existed spools
close(#writer{fd = undefined, root = Root} = State) ->
   File = filename:join([Root, "*", "q" ++ ?WRITER]),
   lists:foreach(fun rename/1, filelib:wildcard(File)),
   State;

close(#writer{fd = FD, file = File} = State) ->
   ok = file:close(FD),
   ok = rename(File),
   State#writer{fd = undefined, file = undefined, written = 0}.

%%
%% rename file
rename(File) ->
   Path = filename:dirname(File),
   Name = filename:basename(File, ?WRITER),
   {A, B, C}  = os:timestamp(),
   Ext = scalar:c(bits:btoh(<<A:32,B:32,C:32>>)),
   file:rename(File, filename:join([Path, [Name, $., Ext]])).

%%
%%
shift(#writer{written = Out} = State)
 when Out >= ?SEGMENT ->
   close(State);

shift(State) ->
   State.

%%
%% encode message
encode(Msg)
 when is_list(Msg) ->
   [encode(X) || X <- Msg];
encode(Msg) ->
   pack(erlang:term_to_binary(Msg)).

pack(Msg)
 when is_binary(Msg) ->
   Hash   = ?HASH32(Msg),
   <<0:16, (byte_size(Msg)):16, Hash:32, Msg/binary>>.
