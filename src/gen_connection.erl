%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
-module(gen_connection).

-behaviour(gen_statem).

%% public API

-export([start_link/3,
         start_link/4,
         call/2,
         call/3,
         cast/2,
         reply/2]).

%% gen_statem callbacks

-export([init/1,
         handle_event/4,
         code_change/4,
         format_status/2,
         terminate/3]).

-type connection() :: pid() | atom() | {atom(), node()} | {global, term()} |
                      {via, module(), term()}.

-export_type([connection/0]).

-callback init(Arg :: term()) ->
    {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate} |
    {connect, Info :: term(), State :: term()} |
    {backoff, timeout, State :: term()} |
    {backoff, timeout(), State :: term(), timeout() | hibernate} |
    ignore | {stop, Reason :: term()}.

-callback connect(Info :: backoff | term(), State :: term()) ->
    {ok, NState :: term()} | {ok, NState :: term(), timeout() | hibernate} |
    {backoff, timeout(), NState :: term()} |
    {backoff, timeout(), NState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NState :: term()}.

-callback disconnect(Info :: backoff | term(), State :: term()) ->
    {connect, Info :: term(), NState :: term()} |
    {backoff, timeout(), NState :: term()} |
    {backoff, timeout(), NState :: term(), timeout() | hibernate} |
    {noconnect, NState :: term()} |
    {noconnect, NState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NState :: term()}.

-callback handle_call(Req :: term(), From :: {pid(), any()}, State :: term()) ->
    {reply, Reply :: term(), NState :: term()} |
    {reply, Reply :: term(), NState :: term(), timeout() | hibernate} |
    {noreply, NState :: term()} |
    {noreply, NState :: term(), timeout() | hibernate} |
    {disconnect | connect, Info :: term(), Reply :: term(), NState :: term()} |
    {disconnect | connect, Info :: term(), NState :: term()} |
    {stop, Reason :: term(), Reply :: term(), NState :: term()} |
    {stop, Reason :: term(), NState :: term()}.

-callback handle_cast(Req :: term(), State :: term()) ->
    {noreply, NState :: term()} |
    {noreply, NState :: term(), timeout() | hibernate} |
    {disconnect | connect, Info :: term(), NState :: term()} |
    {stop, Reason :: term(), Reply :: term(), NState :: term()} |
    {stop, Reason :: term(), NState :: term()}.

-callback handle_info(Req :: term(), State :: term()) ->
    {noreply, NState :: term()} |
    {noreply, NState :: term(), timeout() | hibernate} |
    {disconnect | connect, Info :: term(), NState :: term()} |
    {stop, Reason :: term(), Reply :: term(), NState :: term()} |
    {stop, Reason :: term(), NState :: term()}.

-callback code_change(OldVsn :: term(), State :: term(), Extra :: term()) ->
    {ok, NState :: term()}.

-callback format_status(Opt :: normal | terminate,
                        [(PDict :: [{term(), term()}]) | (State :: term())]) ->
    Status :: term().

-callback terminate(Reason :: term(), State :: term()) -> term().

-optional_callbacks([format_status/2]).

-record(data, {mod :: module(),
               backoff :: reference() | undefined}).

-spec start_link(Mod, Args, Opts) -> {ok, Pid} | ignore | {error, Reason} when
      Mod :: module(),
      Args :: term(),
      Opts :: [gen_server:option()],
      Pid :: pid(),
      Reason :: any().
start_link(Mod, Args, Opts) ->
    gen_statem:start_link(?MODULE, {Mod, Args}, Opts).

-spec start_link(Name, Mod, Args, Opts) ->
    {ok, Pid} | ignore | {error, Reason} when
      Name :: {local, atom()} | {global, term()} | {via, module(), term()},
      Mod :: module(),
      Args :: term(),
      Opts :: [gen_server:option()],
      Pid :: pid(),
      Reason :: any().
start_link(Name, Mod, Args, Opts) ->
    gen_statem:start_link(Name, Mod, Args, Opts).

-spec call(Conn, Request) -> Result when
      Conn :: connection(),
      Request :: term(),
      Result :: term().
call(Conn, Request) ->
    gen_statem:call(Conn, Request).

-spec call(Conn, Request, Timeout) -> Result when
      Conn :: connection(),
      Request :: term(),
      Timeout :: timeout(),
      Result :: term().
call(Conn, Request, Timeout) ->
    gen_statem:call(Conn, Request, Timeout).

-spec cast(Conn, Request) -> ok when
      Conn :: connection(),
      Request :: term().
cast(Conn, Request) ->
    gen_statem:cast(Conn, Request).

-spec reply(From, Response) -> ok when
      From :: {pid(), term()},
      Response :: term().
reply(From, Response) ->
    gen_statem:reply(From, Response).

%% @private
init({Mod, Args}) ->
    _ = put('$initial_call', {Mod, init, 1}),
    try Mod:init(Args) of
        Result ->
            handle_init(Result, #data{mod=Mod})
    catch
        throw:Result ->
            handle_init(Result, #data{mod=Mod})
    end.

%% @private
handle_event({call, From}, Req, State, #data{mod=Mod} = Data) ->
    try Mod:handle_call(Req, From, State) of
        Result ->
            handle_call(Result, From, Data)
    catch
        throw:Result ->
            handle_call(Result, From,Data)
    end;
handle_event(cast, Req, State, #data{mod=Mod} = Data) ->
    try Mod:handle_cast(Req, State) of
        Result ->
            handle_common(Result, Data)
    catch
        throw:Result ->
            handle_common(Result, Data)
    end;
handle_event(info, {timeout, Backoff, backoff}, _,
             #data{backoff=Backoff} = Data)
  when is_reference(Backoff) ->
    {keep_state, Data#data{backoff=undefined}, connect(backoff)};
handle_event(info, Req, State, #data{mod=Mod} = Data) ->
    try Mod:handle_info(Req, State) of
        Result ->
            handle_common(Result, Data)
    catch
        throw:Result ->
            handle_common(Result, Data)
    end;
handle_event(timeout, Mod, State, #data{mod=Mod} = Data) ->
    try Mod:handle_info(timeout, State) of
        Result ->
            handle_common(Result, Data)
    catch
        throw:Result ->
            handle_common(Result, Data)
    end;
handle_event(internal, {connect, Info}, State, #data{mod=Mod} = Data) ->
    NData = cancel_backoff(Data),
    try Mod:connect(Info, State) of
        Result ->
            handle_connect(Result, NData)
    catch
        throw:Result ->
            handle_connect(Result, NData)
    end;
handle_event(internal, {disconnect, Info}, State, #data{mod=Mod} = Data) ->
    NData = cancel_backoff(Data),
    try Mod:disconnect(Info, State) of
        Result ->
            handle_disconnect(Result, NData)
    catch
        throw:Result ->
            handle_disconnect(Result, NData)
    end.

%% @private
code_change(OldVsn, State, #data{mod=Mod} = Data, Extra) ->
    try Mod:code_change(OldVsn, State, Extra) of
        Result ->
            handle_code_change(Result, Data)
    catch
        throw:Result ->
            handle_code_change(Result, Data)
    end.

%% @private
format_status(terminate, [PDict, _, {stop, State, Data}]) ->
    format_status(terminate, [PDict, State, Data]);
format_status(Opt, [PDict, State, #data{mod=Mod}]) ->
    case erlang:function_exported(Mod, format_status, 2) of
        true ->
            Mod:format_status(Opt, [PDict, State]);
        false when Opt == normal ->
            [{data, [{"State", State}]}];
        false when Opt == terminate ->
            State
    end.

%% @private
terminate(Reason, _, {stop, State, Data}) ->
    terminate(Reason, State, Data);
terminate(Reason, State, #data{mod=Mod}) ->
    Mod:terminate(Reason, State).

%% Internal

handle_init({connect, Info, State}, Data) ->
    {handle_event_function, State, Data, connect(Info)};
handle_init({backoff, Time, State}, Data) ->
    {handle_event_function, State, backoff(Time, Data)};
handle_init({backoff, Time, State, hibernate}, Data) ->
    {handle_event_function, State, backoff(Time, Data), hibernate};
handle_init({backoff, Time, State, Timeout}, Data) ->
    {handle_event_function, State, backoff(Time, Data), timeout(Timeout, Data)};
handle_init({ok, State}, Data) ->
    {handle_event_function, State, Data};
handle_init({ok, State, hibernate}, Data) ->
    {handle_event_function, State, Data, hibernate};
handle_init({ok, State, Timeout}, Data) ->
    {handle_event_function, State, Data, timeout(Timeout, Data)};
handle_init({stop, _} = Stop, _) ->
    Stop;
handle_init(ignore, _) ->
    ignore;
handle_init(Other, _) ->
    {stop, {bad_return, Other}}.

handle_call({reply, Reply, State}, From, Data) ->
    {next_state, State, Data, do_reply(From, Reply)};
handle_call({reply, Reply, State, hibernate}, From, Data) ->
    {next_state, State, Data, [do_reply(From, Reply), hibernate]};
handle_call({reply, Reply, State, Timeout}, From, Data) ->
    {next_state, State, Data, [do_reply(From, Reply), timeout(Timeout, Data)]};
handle_call({connect, Info, Reply, State}, From, Data) ->
    {next_state, State, Data, [do_reply(From, Reply), connect(Info)]};
handle_call({disconnect, Info, Reply, State}, From, Data) ->
    {next_state, State, Data, [do_reply(From, Reply), disconnect(Info)]};
handle_call({stop, Reason, Reply, State}, From, Data) ->
    {stop_and_reply, Reason, [do_reply(From, Reply)], {stop, State, Data}};
handle_call(Other, _, Data) ->
    handle_common(Other, Data).

handle_common({noreply, State}, Data) ->
    {next_state, State, Data};
handle_common({noreply, State, hibernate}, Data) ->
    {next_state, State, Data, hibernate};
handle_common({noreply, State, Timeout}, Data) ->
    {next_state, State, Data, timeout(Timeout, Data)};
handle_common({stop, Reason, State}, Data) ->
    {stop, Reason, {stop, State, Data}};
handle_common({connect, Info, State}, Data) ->
    {next_state, State, Data, connect(Info)};
handle_common({disconnect, Info, State}, Data) ->
    {next_state, State, Data, disconnect(Info)};
handle_common(Other, _) ->
    {stop, {bad_return, Other}}.

handle_connect({ok, State}, Data) ->
    {next_state, State, Data};
handle_connect({ok, State, hibernate}, Data) ->
    {next_state, State, Data, hibernate};
handle_connect({ok, State, Timeout}, Data) ->
    {next_state, State, Data, timeout(Timeout, Data)};
handle_connect(Other, Data) ->
    handle_special(Other, Data).

handle_disconnect({connect, Info, State}, Data) ->
    {next_state, State, Data, connect(Info)};
handle_disconnect({noconnect, State}, Data) ->
    {next_state, State, Data};
handle_disconnect({noconnect, State, hibernate}, Data) ->
    {next_state, State, Data, hibernate};
handle_disconnect({noconnect, State, Timeout}, Data) ->
    {next_state, State, Data, timeout(Timeout, Data)};
handle_disconnect(Other, Data) ->
    handle_special(Other, Data).

handle_special({backoff, Time, State}, Data) ->
    {next_state, State, backoff(Time, Data)};
handle_special({backoff, Time, State, hibernate}, Data) ->
    {next_state, State, backoff(Time, Data), hibernate};
handle_special({backoff, Time, State, Timeout}, Data) ->
    {next_state, State, backoff(Time, Data), timeout(Timeout, Data)};
handle_special({stop, Reason, State}, Data) ->
    {stop, Reason, {stop, State, Data}};
handle_special(Other, _) ->
    {stop, {bad_return, Other}}.

connect(Info) ->
    {next_event, internal, {connect, Info}}.

disconnect(Info) ->
    {next_event, internal, {disconnect, Info}}.

timeout(Timeout, #data{mod=Mod}) ->
    {timeout, Timeout, Mod}.

do_reply(From, Reply) ->
    {reply, From, Reply}.

backoff(infinity, #data{backoff=undefined} = Data) ->
    Data;
backoff(Time, #data{backoff=undefined} = Data) ->
    Data#data{backoff=erlang:start_timer(Time, self(), backoff)};
backoff(Time, Data) ->
    backoff(Time, cancel_backoff(Data)).

cancel_backoff(#data{backoff=undefined} = Data) ->
    Data;
cancel_backoff(#data{backoff=Ref} = Data) ->
    case erlang:cancel_timer(Ref) of
        false ->
            flush_backoff(Data);
        _ ->
            Data#data{backoff=undefined}
    end.

flush_backoff(#data{backoff=Ref} = Data) ->
    receive
        {timeout, Ref, backoff} ->
            ok
    after
        0 ->
            ok
    end,
    Data#data{backoff=undefined}.

handle_code_change({ok, State}, Data) ->
    {handle_event_function, State, Data};
handle_code_change({Callback, _, _} = Arg, Data) when
      Callback == handle_event_function; Callback == state_functions ->
    error(badarg, [Arg, Data]);
handle_code_change(Other, _) ->
    Other.
