%%
%%   Copyright (c) 2017, Dmitry Kolesnikov
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
-module(esq_SUITE).
-include_lib("common_test/include/ct.hrl").

%%
%% common test
-export([
   all/0
  ,groups/0
  ,init_per_suite/1
  ,end_per_suite/1
  ,init_per_group/2
  ,end_per_group/2
  ,init_per_testcase/2
  ,end_per_testcase/2
]).

-export([
   enq/1, 
   deq/1,
   persistence/1,
   inflight/1,
   corrupted/1,
   dlq_limit/1,
   dlq_disabled/1,
   dlq_restart/1,
   dlq_option/1
]).

%%
%% logger handler
-export([log/2, filter/2]).

%%%----------------------------------------------------------------------------   
%%%
%%% suite
%%%
%%%----------------------------------------------------------------------------   
all() ->
   [
      {group, interface}
   ].

groups() ->
   [
      {interface, [parallel], 
         [enq, deq, persistence, inflight, corrupted,
          dlq_limit, dlq_disabled, dlq_restart, dlq_option]}
   ].


%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   Config.

end_per_suite(_Config) ->
   os:cmd("rm -Rf /tmp/q"),
   ok.

%% 
%%
init_per_group(_, Config) ->
   Config.

end_per_group(_, _Config) ->
   ok.

%%
%% capture esq log events, the handler is removed even if test case fails
init_per_testcase(corrupted, Config) ->
   ok = logger:add_handler(esq_SUITE, ?MODULE, #{
      config  => #{pid => self()},
      filters => [{esq, {fun ?MODULE:filter/2, {esq_reader, self()}}}],
      filter_default => stop
   }),
   Config;
init_per_testcase(_, Config) ->
   Config.

end_per_testcase(corrupted, _Config) ->
   _ = logger:remove_handler(esq_SUITE),
   ok;
end_per_testcase(_, _Config) ->
   ok.


%%%----------------------------------------------------------------------------   
%%%
%%% unit test
%%%
%%%----------------------------------------------------------------------------   

enq(_Config) ->
   {ok, Q} = esq:new("/tmp/q/enq"),
   ok = esq:enq(a, Q),
   ok = esq:free(Q),

   true = filelib:is_dir("/tmp/q/enq").

deq(_Config) ->
   {ok, Q} = esq:new("/tmp/q/deq", [{tts, 1}]),
   ok = esq:enq(a, Q),
   timer:sleep(1),
   [#{payload := a}] = esq:deq(Q),
   ok = esq:free(Q).

persistence(_Config) ->
   {ok, A} = esq:new("/tmp/q/persistence"),
   ok = esq:enq(a, A),
   ok = esq:free(A),

   {ok, B} = esq:new("/tmp/q/persistence"),
   [#{payload := a}] = esq:deq(B),
   ok = esq:free(B).

inflight(_Config) ->
   {ok, Q} = esq:new("/tmp/q/inflight", [{tts, 1}, {ttf, 10}]),
   [esq:enq(X, Q) || X <- [a, b, c, d]],
   timer:sleep(1),
   [#{payload := a, receipt := A}] = esq:deq(Q),
   ok = esq:ack(A, Q),

   timer:sleep(15),
   [#{payload := b}] = esq:deq(Q),
   timer:sleep(15),
   [#{payload := b}] = esq:deq(Q),
   ok = esq:free(Q).



corrupted(_Config) ->
   Root = "/tmp/q/corrupted",
   File = segment(Root),

   R = esq_reader:new(Root),
   [a, <<>>, skip, b] = read_all(R),

   {ok, Dead} = file:read_file(dlq_file(File)),
   Dead = iolist_to_binary(bad_frames()),

   [First, Summary] = logged(),
   {match, _} = re:run(First, File),
   {match, _} = re:run(Summary, "skipped 4 .* " ++ File ++ ", all moved to dead letter file " ++ dlq_file(File)).

dlq_limit(_Config) ->
   Root = "/tmp/q/dlq_limit",
   File = segment(Root),
   %% dead letter file of previously consumed segment counts towards the limit
   Prev = filename:join([Root, "20160101", "dlq.0000000000000000"]),
   ok = filelib:ensure_dir(Prev),
   ok = file:write_file(Prev, <<0:800>>),
   [Bad1, Bad2 | _] = bad_frames(),

   R = esq_reader:new(Root, [{dlq, 100 + byte_size(Bad1) + byte_size(Bad2)}]),
   [a, <<>>, skip, b] = read_all(R),

   {ok, Dead} = file:read_file(dlq_file(File)),
   Dead = <<Bad1/binary, Bad2/binary>>,
   {ok, <<0:800>>} = file:read_file(Prev).

dlq_disabled(_Config) ->
   Root = "/tmp/q/dlq_disabled",
   File = segment(Root),

   R = esq_reader:new(Root, [{dlq, 0}]),
   [a, <<>>, skip, b] = read_all(R),

   false = filelib:is_file(dlq_file(File)).

dlq_restart(_Config) ->
   Root = "/tmp/q/dlq_restart",
   File = segment(Root),
   Dead = iolist_to_binary(bad_frames()),

   %% reader is gone (e.g. crashed) after all bad frames are quarantined, segment is not consumed
   R0 = esq_reader:new(Root, [{dlq, byte_size(Dead)}]),
   {a,    R1} = esq_reader:deq(R0),
   {<<>>, R2} = esq_reader:deq(R1),
   {skip, _ } = esq_reader:deq(R2),
   {ok, Dead} = file:read_file(dlq_file(File)),

   %% segment is read again, dead letter file is re-written but not duplicated
   R = esq_reader:new(Root, [{dlq, byte_size(Dead)}]),
   [a, <<>>, skip, b] = read_all(R),
   {ok, Dead} = file:read_file(dlq_file(File)).

dlq_option(_Config) ->
   Root = "/tmp/q/dlq_option",
   File = segment(Root),

   %% segment contains bare terms, queue itself writes #{receipt, payload} to disk
   {ok, Q} = esq:new(Root, [{dlq, 0}]),
   [a, <<>>, skip, b] = esq:deq(10, Q),
   ok = esq:free(Q),

   false = filelib:is_file(dlq_file(File)).

%%
%% segment with good and bad frames
segment(Root) ->
   File = filename:join([Root, "20170101", "q.0000000000000000"]),
   ok = filelib:ensure_dir(File),
   [Bad1, Bad2, Bad3, Bad4] = bad_frames(),
   ok = file:write_file(File, [
      frame(term_to_binary(a)),
      Bad1,
      frame(term_to_binary(<<>>)),
      Bad2,
      Bad3,
      Bad4,
      frame(term_to_binary(skip)),
      frame(term_to_binary(b))
   ]),
   File.

%%
%% three undecodable frames and one with CRC mismatch
bad_frames() ->
   Undecodable = <<131, 255, 1, 2, 3>>,
   BadHash = term_to_binary(bad_hash),
   [
      frame(Undecodable),
      frame(Undecodable),
      frame(Undecodable),
      <<0:16, (byte_size(BadHash)):32, (erlang:crc32(BadHash) + 1):32, BadHash/binary>>
   ].

dlq_file(File) ->
   filename:join(filename:dirname(File), "dl" ++ filename:basename(File)).

read_all(R0) ->
   case esq_reader:deq(R0) of
      {eof, _} -> [];
      {Msg, R1} -> [Msg | read_all(R1)]
   end.

frame(Msg) ->
   <<0:16, (byte_size(Msg)):32, (erlang:crc32(Msg)):32, Msg/binary>>.

logged() ->
   receive
      {log, Msg} -> [Msg | logged()]
   after 0 ->
      []
   end.

log(#{msg := {Format, Args}}, #{config := #{pid := Pid}}) ->
   Pid ! {log, lists:flatten(io_lib:format(Format, Args))}.

filter(#{meta := #{mfa := {Mod, _, _}, pid := Pid}} = Event, {Mod, Pid}) ->
   Event;
filter(_, _) ->
   stop.
