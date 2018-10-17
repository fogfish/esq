-module(esq_queue).
-behavior(pipe).
-compile({parse_transform, category}).

-include("esq.hrl").
-include_lib("datum/include/datum.hrl").

-export([
   start_link/2,
   init/1,
   free/2,
   handle/3
]).

%%-----------------------------------------------------------------------------
%%
%% factory
%%
%%-----------------------------------------------------------------------------

start_link(Path, Opts) ->
   pipe:start_link(?MODULE, [Path, Opts], []).

init([Path, Opts]) ->
   {ok, handle, 
      config(Opts, #q{
         head     = deq:new(),
         tail     = esq_file:new(Path),
         capacity = 1,
         tts      = 1000
      })
   }.

config([{ttl, X} | Opts], State) ->
   config(Opts, State#q{ttl = X});

config([{ttf, X} | Opts], State) ->
   config(Opts, State#q{inflight = esq_inflight:new(X)});

config([{tts, X} | Opts], State) ->
   config(Opts, 
      State#q{
         tts = X, 
         tte = typecast:t( typecast:i(os:timestamp()) + X * 1000 )
      }
   );

config([{capacity, X} | Opts], State) ->
   config(Opts, State#q{capacity = X});

config([_ | Opts], State) ->
   config(Opts, State);

config([], State) ->
   State.


free(_, #q{tail = Tail}) ->
   esq_file:free(Tail),
   ok.

%%-----------------------------------------------------------------------------
%%
%% state machine
%%
%%-----------------------------------------------------------------------------

handle({enq, E}, Pipe, State0) ->
   State1 = enq(E, State0),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle({deq, N}, Pipe, State0) ->
   {Head, State1} = deq(N, State0),
   pipe:ack(Pipe, Head),
   {next_state, handle, State1};

handle({ack, Uid}, Pipe, State0) ->
   State1 = ack(Uid, State0),
   pipe:ack(Pipe, ok),
   {next_state, handle, State1};

handle(head, Pipe, State0) ->
   {Head, State1} = head(State0),
   pipe:ack(Pipe, Head),
   {next_state, handle, State1};

handle(free, Pipe, State) ->
   pipe:ack(Pipe, ok),
   {stop, normal, State};

handle(_, _, State) ->
   {next_state, handle, State}.


%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%%
enq(E, State) ->
   Uid = uid:encode(uid:l()),
   [identity ||
      sync_on_disk_tail(State),
      evict_with_ttl(Uid, _),
      deq_in_flight(Uid, _),
      enq_(pack(Uid, E), _)
   ].

enq_(E, #q{tail = Tail} = State) ->
   State#q{tail = esq_file:enq(E, Tail)}.


%%
%%
deq(N, State) ->
   Uid = uid:encode(uid:l()),
   [identity ||
      sync_on_disk_tail(State),
      evict_with_ttl(Uid, _),
      deq_in_flight(Uid, _),
      shift_on_disk_tail(N, _),
      deq_(N, _)
   ].

%%
%% dequeue message
deq_(N, #q{head = Head} = State) ->
   {H, T} = deq:split(N, Head),
   enq_in_flight(H, State#q{head = T}).

%%
%%
ack(_Uid, #q{inflight = undefined}=State) ->
   State;

ack(Uid,  #q{inflight = InFlight0} = State) ->
   InFlight1 = esq_inflight:ack(Uid, InFlight0),
   State#q{inflight = InFlight1}.

%%
%%
head(State) ->
   Uid = uid:encode(uid:l()),
   [identity ||
      sync_on_disk_tail(State),
      evict_with_ttl(Uid, _),
      deq_in_flight(Uid, _),
      shift_on_disk_tail(0, _),
      head_(_)
   ].

head_(#q{head = #queue{length = 0}} = State) ->
   {undefined, State};

head_(#q{head = Head} = State) ->
   {deq:head(Head), State}.


%%
%%
pack(Uid, E) ->
   #{receipt => Uid, payload => E}.

pack({Uid, E}) ->
   pack(Uid, E).

%%
%%
inject(Elements, Queue) ->
   lists:foldr(
      fun(E, Acc) -> 
         deq:enqh(pack(E), Acc) 
      end, 
      Queue,
      Elements 
   ).

%%
%% sync file queue
sync_on_disk_tail(#q{tail = Tail, tte = Expire, tts = T}=State) ->
   case os:timestamp() of
      X when X > Expire ->
         State#q{
            tail = esq_file:sync(Tail),
            tte  = typecast:t( typecast:i(X) + T * 1000 )
         };
      _ ->
         State
   end.

%%
%%
shift_on_disk_tail(N, #q{head = #queue{length = 0}, tail = Queue, capacity = C} = State) ->
   {H, T} = esq_file:deq(N + C, Queue),
   State#q{head = H, tail = T};

shift_on_disk_tail(_, State) ->
   State.


%%
%% remove expired message
evict_with_ttl(_Uid, #q{ttl = undefined} = State) ->
   State;

evict_with_ttl(Uid,  #q{head = Head0, ttl = TTL} = State) ->
   Head1 = deq:dropwhile(fun(#{receipt := X}) -> diff(Uid, X) > TTL end, Head0),
   State#q{head = Head1}.


%%
%% dequeue (recover) expired in-flight messages to queue head
deq_in_flight(_, #q{inflight = undefined} = State) ->
   State;
deq_in_flight(Uid, #q{inflight = Queue0, head = Head} = State) ->
   {Elements, Queue1} = esq_inflight:deq(Uid, Queue0),
   State#q{head = inject(Elements, Head), inflight = Queue1}.

%%
%% enqueue message to in-flight queue
enq_in_flight(Queue, #q{inflight = undefined} = State) ->
   {deq:list(Queue), State};

enq_in_flight(Queue, State) ->
   enq_in_flight(Queue, deq:new(), State).

enq_in_flight(#queue{length = 0}, Acc, State) ->
   {deq:list(Acc), State};

enq_in_flight(Queue, Acc, #q{inflight = InFlight0} = State) ->
   #{payload := E}  = deq:head(Queue),
   {Uid, InFlight1} = esq_inflight:enq(E, InFlight0),
   enq_in_flight(
      deq:tail(Queue), 
      deq:enq(pack(Uid, E), Acc), 
      State#q{inflight = InFlight1}
   ).


%%
%%
diff(A, B) ->
   uid:t(uid:d(uid:decode(A), uid:decode(B))).
