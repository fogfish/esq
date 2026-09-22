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
   corrupted/1
]).

%%
%% logger handler
-export([log/2]).

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
         [enq, deq, persistence, inflight, corrupted]}
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
      filters => [{esq, {fun logger_filters:domain/2, {log, sub, [esq]}}}],
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
   File = filename:join([Root, "20170101", "q.0000000000000000"]),
   ok = filelib:ensure_dir(File),
   Undecodable = <<131, 255, 1, 2, 3>>,
   BadHash = term_to_binary(bad_hash),
   ok = file:write_file(File, [
      frame(term_to_binary(a)),
      frame(Undecodable),
      frame(term_to_binary(<<>>)),
      frame(Undecodable),
      frame(Undecodable),
      <<0:16, (byte_size(BadHash)):32, (erlang:crc32(BadHash) + 1):32, BadHash/binary>>,
      frame(term_to_binary(skip)),
      frame(term_to_binary(b))
   ]),

   R0 = esq_reader:new(Root),
   {a,    R1} = esq_reader:deq(R0),
   {<<>>, R2} = esq_reader:deq(R1),
   {skip, R3} = esq_reader:deq(R2),
   {b,    R4} = esq_reader:deq(R3),
   {eof,  _ } = esq_reader:deq(R4),

   [First, Summary] = logged(),
   {match, _} = re:run(First, File),
   {match, _} = re:run(Summary, "skipped 4 .* " ++ File).

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
