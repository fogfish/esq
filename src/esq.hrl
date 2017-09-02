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

%%
%% file extension for queue segments
-define(WRITER,      ".spool").
-define(READER,      ".[0-9a-f]*").

%%
%% queue segment size 64MB
-define(SEGMENT,     67108864).
-define(CHUNK,          65536).
-define(DELAY,           2000).

%%
%% message hash function
-define(HASH32(X),  erlang:crc32(X)).

%%
%% queue data structure
-record(q, {
   head     = undefined :: datum:q()     %% in-memory queue head
  ,tail     = undefined :: any()         %% on-disk overflow queue tail
  ,inflight = undefined :: datum:heap()  %% in-flight heap

  ,capacity = undefined :: integer()     %% number of message to keep in-memory
  ,ttf      = undefined :: any()         %% message visibility timeout, time to keep message in-flight queue
  ,ttl      = undefined :: any()         %% message time-to-live
  ,tts      = undefined :: any()         %% message time-to-sync
  ,tte      = undefined :: any()         %% time to expired
}).
