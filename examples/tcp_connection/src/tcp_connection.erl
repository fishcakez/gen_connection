-module(tcp_connection).

-export([start_link/3,
         send/2,
         recv/3,
         close/1,
         test/0]).

-behaviour(gen_connection).
-export([init/1,
         connect/2,
         disconnect/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-record(state, {host :: inet:ip_address() | inet:hostname(),
                port :: inet:port_number(),
                connect_timeout :: pos_integer(),
                socket :: gen_tcp:socket() | undefined}).


start_link(Host, Port, ConnectTimeout) ->
    gen_connection:start_link(?MODULE, [Host, Port, ConnectTimeout], []).

send(Conn, Data) ->
    gen_connection:call(Conn, {send, Data}).

recv(Conn, Length, Timeout) ->
    gen_connection:call(Conn, {recv, Length, Timeout}).

close(Conn) ->
    gen_connection:call(Conn, close).


test() ->
    start_link("localhost", 10000, 1000).

%% @hidden
init([Host, Port, ConnectTimeout]) ->
    {connect, initial, #state{
        host = Host,
        port = Port,
        connect_timeout = ConnectTimeout
    }}.

%% @hidden
connect(_Info, #state{host = Host, port = Port, connect_timeout = ConnectTimeout} = State) ->
    case gen_tcp:connect(Host, Port, [{active, false}], ConnectTimeout) of
        {ok, Socket} ->
            {ok, State#state{socket = Socket}};
        {error, _Reason} ->
            %error_logger:error_msg("Connection failed: ~s~n", [inet:format_error(Reason)]),
            {backoff, 1000, State}
    end.

%% @hidden
disconnect(Info, #state{socket = Socket} = State) ->
    _ = gen_tcp:close(Socket),
    case Info of
        {close, From} ->
            gen_connection:reply(From, ok),
            {noconnect, State#state{socket = undefined}};
        {error, closed} ->
            error_logger:error_msg("Connection closed~n"),
            {connect, reconnect, State#state{socket = undefined}};
        {error, Reason} ->
            error_logger:error_msg("Connection error: ~s~n", [inet:format_error(Reason)]),
            {connect, reconnect, State#state{socket = undefined}}
    end.

%% @hidden
handle_call(_, _, #state{socket = undefined} = Socket) ->
    {reply, {error, closed}, Socket};
handle_call({send, Data}, _, #state{socket = Socket} = State) ->
    io:format("calling gen_tcp:send(~p, ~p)", [Socket, Data]),
    case gen_tcp:send(Socket, Data) of
        ok ->
            io:format(" -> ok~n"),
            {reply, ok, State};
        {error, _} = Error ->
            io:format(" -> ~p~n", [Error]),
            {disconnect, Error, Error, State}
    end;
handle_call({recv, Length, Timeout}, _, #state{socket = Socket} = State) ->
    case gen_tcp:recv(Socket, Length, Timeout) of
        {ok, _} = Result ->
            {reply, Result, State};
        {error, Reason} = Result when Reason =:= timeout; Reason =:= einval ->
            {reply, Result, State};
        {error, _} = Error ->
            {disconnect, Error, Error, State}
    end;
handle_call(close, From, State) ->
    {disconnect, {close, From}, State}.

%% @hidden
handle_cast(_, State) ->
    {noreply, State}.

%% @hidden
handle_info(_, State) ->
    {noreply, State}.

%% @hidden
code_change(_, State, _Extra) ->
    {ok, State}.

%% @hidden
terminate(_, _) ->
    ok.
