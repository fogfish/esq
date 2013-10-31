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
%%   file utils
-module(esq_file).

-export([
	writer/2,
	reader/2,
	close/1,
	rotate/1,
	remove/1,
	write_record/3,
	write_string/2,
	read_record/1
]).

%%
%% open writer file
-spec(writer/2 :: (list(), list()) -> {ok, {file, list(), any()}} | {error, any()}).

writer(File, Mode) ->
	case filelib:ensure_dir(File) of
		ok ->
			case file:open(File, Mode) of
				{ok, FD} -> {ok, {file, File, FD}};
				Error    -> Error
			end;
		Error ->
			Error
	end.

%%
%% open reader file
-spec(reader/2 :: (list(), list()) -> {ok, {}}).

reader(File, Mode) ->
	case filelib:wildcard(File) of
      [] -> 
         {error, enoent};
      [Head | _] ->
         case file:open(Head, Mode) of
            {ok, FD} -> 
            	{ok, {file, Head, FD}};
            Error    -> 
            	Error
         end
   end.

%%
%% close file
close(undefined) ->
	ok;
close({file, _, FD}) ->
	file:close(FD).


%%
%% rotate file
rotate(undefined) ->
	ok;
rotate({file, File, FD}) ->
	file:close(FD),
	rotate(File);
rotate(File) ->
   {A, B, C} = erlang:now(),
   Now = lists:flatten(
      io_lib:format(".~6..0b~6..0b~6..0b", [A, B, C])
   ),
   file:rename(File, filename:rootname(File) ++ Now).

%%
%% remove file
remove(undefined) ->
	ok;
remove({file, File, FD}) ->
	file:close(FD),
	remove(File);
remove(File) ->
	file:delete(File).

%%
%% 
write_record({file, _, FD}, Type, Msg) ->
   Size = size(Msg),
   case file:write(FD, [<<Type:8>>, <<Size:56>>, Msg]) of
      ok    -> {ok, Size};
      Error -> Error
   end.

write_string({file, _, FD}, Msg) ->
   Size = size(Msg),
   case file:write(FD, [Msg, $\n]) of
      ok    -> {ok, Size};
      Error -> Error
   end.

%%
%%
read_record({file, _, FD}) ->
	case file:read(FD, 8) of
      {ok, <<Type:8, Size:56>>} ->
         case file:read(FD, Size) of
            {ok, Msg} ->
               {ok, Type, Msg};
            Error     ->
               Error
         end;
      Error ->
      	Error
   end.



