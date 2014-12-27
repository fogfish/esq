%% @description
%%    dets-based spool 
%%
%% @deprecated
-module(esq_spool_dets).

-export([
   init/1,
   free/2,
   evict/2,
   enq/4,
   deq/3
]).

%% internal state
-record(spool, {
   enq   = undefined :: integer(), % position to enq message
   deq   = undefined :: integer(), % position to deq message 
   q     = undefined :: atom(),    % queue reference

   bulk  = undefined :: integer(), % 
   heap  = undefined :: datum:q(), %
   opts  = []        :: list()     % dets options
}).
% queue i/o positions
-define(LEN,  2).
-define(ENQ,  3).
-define(DEQ,  4).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

init(Opts) ->
   init(Opts, #spool{opts = dets_opts(Opts)}).

init([{spool, X} | Opts], S) ->
   ok = filelib:ensure_dir(X),
   init(Opts, S#spool{q = list_to_atom(X)});

init([{bulk,  X} | Opts], S) ->
   init(Opts, S#spool{bulk = X, heap = deq:new()});

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   % open file and read queue position
   {ok, _} = dets:open_file(S#spool.q, S#spool.opts),
   case dets:lookup(S#spool.q, q) of
      [] ->
         dets:insert(S#spool.q, {q, 0, 0, 0}),
         {ok,   0, S};
      [{q, Len, Enq, Deq}] ->
         {ok, Len, S#spool{deq=Deq, enq=Enq}}
   end.

free(_, S) ->
   dets:close(S#spool.q).

%%%----------------------------------------------------------------------------   
%%%
%%% queue
%%%
%%%----------------------------------------------------------------------------   

%%
%% evict messages
evict(_T, Queue) ->
   {ok, 0, Queue}.

%%
%% enqueue message
enq(_TTL, _Pri, Msg, #spool{bulk=undefined}=S) ->
   write(Msg, S);

enq(_TTL, _Pri, Msg, S) ->
   Heap = deq:enq(Msg, S#spool.heap),
   case deq:length(Heap) of
      X when X >= S#spool.bulk ->
         write({bulk, deq:list(Heap)}, S#spool{heap = deq:new()});
      _ ->
         {ok, S#spool{heap = Heap}}
   end.
 
%%
%% dequeue message
deq(_Pri, _N, #spool{enq=X, deq=Y, bulk=undefined}=S)
 when Y =:= X ->
   {ok, [], S};

deq(_Pri, _N, #spool{enq=X, deq=Y, heap={}}=S)
 when Y =:= X ->
   {ok, [], S};

deq(_Pri, _N, #spool{enq=X, deq=Y}=S)
 when Y =:= X ->
   {ok, deq:list(S#spool.heap), S#spool{heap = deq:new()}};

deq(_Pri, _N, S) ->
   Id  = dets:update_counter(S#spool.q, q, {?DEQ,  1}),
   case dets:lookup(S#spool.q, Id) of
      Msg when is_list(Msg) ->
         _   = dets:delete(S#spool.q, Id),
         _   = dets:update_counter(S#spool.q, q, {?LEN, -1}),  
         {ok, unpack(Msg),
            S#spool{
               deq = Id
            }
         };
      Error ->
         Error
   end.

unpack([{_, {bulk, X}} | Tail]) ->
   X ++ unpack(Tail);
unpack([{_, X} | Tail]) ->
   [X | unpack(Tail)];
unpack([]) ->
   [].

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% dets bucket options
dets_opts(Opts) ->
   dets_opts(Opts, [{estimated_no_objects, 100000}]).

dets_opts([{spool, X} | Opts], Acc) ->
   dets_opts(Opts, [{file, X} | Acc]);

dets_opts([{sync,  X} | Opts], Acc) ->
   dets_opts(Opts, [{auto_save, X} | Acc]);

dets_opts([_ | Opts], Acc) ->
   dets_opts(Opts, Acc);

dets_opts([], Acc) ->
   Acc.


%%
%% write message(s)
write(Msg, S) ->
   Id = dets:update_counter(S#spool.q, q, {?ENQ, 1}),
   case dets:insert(S#spool.q, {Id, Msg}) of
      ok -> 
         _ = dets:update_counter(S#spool.q, q, {?LEN, 1}),  
         {ok, 
            S#spool{
               enq = Id
            }
         };
      Error ->
         Error
   end. 
