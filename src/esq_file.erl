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
%%    binary file 
-module(esq_file).
-behaviour(gen_server).

-export([
   start_link/2
  ,init/1 
  ,terminate/2
  ,handle_call/3 
  ,handle_cast/2 
  ,handle_info/2  
  ,code_change/3
   %% file api
  ,close/1
  ,sync/1
  ,write/2
  ,read/1
  ,delete/1
]).

%%
%%
-define(PAGE,             64 * 1024).
-define(HEAD,                     8).
-define(HASH32(X),  erlang:crc32(X)).
 
-record(io, {
   file  = undefined :: list()
  ,fd    = undefined :: any()
  ,cache = undefined :: binary()
}).

%%
%% open file and start file process
-spec(start_link/2 :: (list(), list()) -> {ok, pid()} | {error, any()}).

start_link(File, Opts) ->
   case gen_server:start_link(?MODULE, [self(), Ref = make_ref(), File, Opts], []) of
      {ok, Fd} ->
         {ok, Fd};
      ignore   ->
         receive
            {Ref, Error} ->
               Error
         end;
      Error ->
         Error
   end.

init([Pid, Ref, File, Opts]) ->
   %% terminate process gracefully and close the file
   process_flag(trap_exit, true),
   case file:open(File, [raw, binary] ++ Opts) of
      {ok, FD} ->
         {ok, #io{file=File, fd=FD, cache = <<>>}};
      Error    ->
         Pid ! {Ref, Error},
         ignore
   end.

terminate(_Reason, #io{fd=undefined}) ->
   ok;
terminate(_Reason, #io{}=S) ->
   file:close(S#io.fd).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% close file and terminate i/o process
-spec(close/1 :: (pid()) -> ok).

close(FD)
 when is_pid(FD) ->
   gen_server:call(FD, close). 

%%
%% sync file
-spec(sync/1 :: (pid()) -> ok).

sync(FD)
 when is_pid(FD) ->
   gen_server:call(FD, sync). 

%%
%% delete file and terminate i/o process
-spec(delete/1 :: (pid()) -> ok).

delete(FD)
 when is_pid(FD) ->
   gen_server:call(FD, delete). 


%%
%% write data to file
-spec(write/2 :: (pid(), binary()) -> {ok, integer()} | {error, any()}).

write(FD, Data)
 when is_pid(FD) ->
   gen_server:call(FD, {write, Data}).

%%
%% read data from file
-spec(read/1 :: (pid()) -> {ok, binary()}).

read(FD)
 when is_pid(FD) ->
   gen_server:call(FD, read). 



%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle_call(close, _Tx, S) ->
   {stop, normal, ok, S}; 

handle_call(delete, _Tx, #io{}=S) ->
   _ = file:close(S#io.fd),
   _ = file:delete(S#io.file),
   {stop, normal, ok, S#io{fd=undefined}};

handle_call({write, Bin}, _Tx, S) ->
   Chunk = encode(Bin),
   case file:write(S#io.fd, Chunk) of
      ok    -> 
         {reply, {ok, byte_size(Chunk)}, S};
      Error -> 
         {reply, Error, S}
   end;

handle_call(read, Tx, #io{cache = <<>>}=S) ->
   case file:read(S#io.fd, ?PAGE) of
      {ok, Data} ->
         handle_call(read, Tx, S#io{cache = Data});
      Error ->
         {reply, Error, S}
   end; 

handle_call(read, Tx, #io{}=S) ->
   case decode(S#io.cache) of
      {error, no_message} ->
         case file:read(S#io.fd, ?PAGE) of
            {ok, Data} ->
               handle_call(read, Tx, S#io{cache = <<(S#io.cache)/binary, Data/binary>>});
            Error ->
               {reply, Error, S}
         end;
      {<<>>, Cache} ->
         handle_call(read, Tx, S#io{cache=Cache});
      {Msg,  Cache} ->
         {reply, {ok, Msg}, S#io{cache=Cache}}
   end;   

handle_call(_Req, _Tx, S) ->
   {noreply, S}.

%%
%%
handle_cast(_Req, S) ->
   {noreply, S}.

%%
%%
handle_info({'EXIT', _, normal}, S) ->
    {noreply, S};
handle_info({'EXIT', _, Reason}, S) ->
    {stop, Reason, S};
handle_info(_Msg, S) ->
   {noreply, S}.

%%
%% 
code_change(_Vsn, S, _Extra) ->
   {ok, S}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% encode binary to file cells
%% see for optimization
%%    http://erlang.org/pipermail/erlang-questions/2013-April/073292.html
%%    https://gist.github.com/nox/5359459/raw/0b86154804b43b9043a3fed00debe284f4702f10/prealloc_bin.S
encode(Msg) ->
   %% @todo double zero escape
   Hash   = ?HASH32(Msg),
   <<0:16, (byte_size(Msg)):16, Hash:32, Msg/binary>>.

decode(<<0:16, Len:16, Hash:32, Tail/binary>>) ->
   case byte_size(Tail) of
      X when X < Len ->
         {error, no_message};
      _ ->
         <<Msg:Len/binary, Rest/binary>> = Tail,
         case ?HASH32(Msg) of
            Hash -> {Msg,  Rest};
            _    -> {<<>>, Rest}
         end
   end;
decode(X)
 when byte_size(X) < ?HEAD ->
   {error, no_message};
decode(<<_:8, Tail/binary>>) ->
   decode(Tail).

