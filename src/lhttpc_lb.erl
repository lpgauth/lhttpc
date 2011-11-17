-module(lhttpc_lb).

-export([
        start_link/1
    ]).
-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2
    ]).

-behaviour(gen_server).

-record(httpc_man, {
        host :: string(),
        port = 80 :: integer(),
        ssl = false :: true | false,
        max_connections = 10 :: non_neg_integer(),
        connection_timeout = 300000 :: non_neg_integer(),
        sockets,
        available_sockets = []
    }).

%% @spec (any()) -> {ok, pid()}
%% @doc Starts and link to the gen server.
%% This is normally called by a supervisor.
%% @end
-spec start_link(any()) -> {ok, pid()}.
start_link([Dest, Opts]) ->
    gen_server:start_link(?MODULE, [Dest, Opts], []).

%% @hidden
-spec init(any()) -> {ok, #httpc_man{}}.
init([{Host, Port, Ssl}, {MaxConnections, ConnectionTimeout}]) ->
    State = #httpc_man{
        host = Host,
        port = Port,
        ssl = Ssl,
        max_connections = MaxConnections,
        connection_timeout = ConnectionTimeout,
        sockets = ets:new(sockets, [set])
    },
    {ok, State}.

%% @hidden
-spec handle_call(any(), any(), #httpc_man{}) ->
    {reply, any(), #httpc_man{}}.
handle_call({socket, Pid, ConnectOptions, ConnectTimeout}, _, State) ->
    {Reply, NewState} = find_socket(Pid, ConnectOptions, ConnectTimeout, State),
    {reply, Reply, NewState};
handle_call(_, _, State) ->
    {reply, {error, unknown_request}, State}.

%% @hidden
-spec handle_cast(any(), #httpc_man{}) -> {noreply, #httpc_man{}}.
handle_cast({store, Socket}, State) ->
    NewState = store_socket(Socket, State),
    {noreply, NewState};
handle_cast({remove, Socket}, State) ->
    NewState = remove_socket(Socket, State),
    {noreply, NewState};
handle_cast({terminate}, State) ->
    terminate(undefined, State),
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(any(), #httpc_man{}) -> {noreply, #httpc_man{}}.
handle_info({tcp_closed, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({ssl_closed, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({timeout, Socket}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({tcp_error, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({ssl_error, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)};
handle_info({tcp, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)}; % got garbage
handle_info({ssl, Socket, _}, State) ->
    {noreply, remove_socket(Socket, State)}; % got garbage
handle_info(_, State) ->
    {noreply, State}.

%% @hidden
-spec terminate(any(), #httpc_man{}) -> ok.
terminate(_, State) ->
    close_sockets(State#httpc_man.sockets, State#httpc_man.ssl).

%% @hidden
-spec code_change(any(), #httpc_man{}, any()) -> #httpc_man{}.
code_change(_, State, _) ->
    State.

find_socket(Pid, ConnectOptions, ConnectTimeout, State) ->
    Host = State#httpc_man.host,
    Port = State#httpc_man.port,
    Ssl = State#httpc_man.ssl,
    case State#httpc_man.available_sockets of
        [Socket|Available] ->
            case lhttpc_sock:controlling_process(Socket, Pid, Ssl) of
                ok ->
                    [{Socket,Timer}] = ets:lookup(State#httpc_man.sockets, Socket),
                    cancel_timer(Timer, Socket),
                    NewState = State#httpc_man{available_sockets = Available},
                    {{ok, Socket}, NewState};
                {error, badarg} ->
                    lhttpc_sock:setopts(Socket, [{active, once}], Ssl),
                    {{error, no_pid}, State};
                {error, _Reason} ->
                    NewState = State#httpc_man{available_sockets = Available},
                    find_socket(Pid, ConnectOptions, ConnectTimeout, remove_socket(Socket, NewState))
            end;
        [] ->
            MaxSockets = State#httpc_man.max_connections,
            Size = ets:info(State#httpc_man.sockets, size),
            case MaxSockets > Size andalso Size =/= undefined of
                true ->
                    SocketOptions = [binary, {packet, http}, {active, false} | ConnectOptions],
                    case lhttpc_sock:connect(Host, Port, SocketOptions, ConnectTimeout, Ssl) of
                        {ok, Socket} ->
                            find_socket(Pid, ConnectOptions, ConnectTimeout, store_socket(Socket, State));
                        {error, etimedout} ->
                            {{error, sys_timeout}, State};
                        {error, timeout} ->
                            {{error, timeout}, State};
                        {error, Reason} ->
                            {{error, Reason}, State}
                    end;
            false ->
                {{error, retry_later}, State}
        end
    end.

remove_socket(Socket, State) ->
    case ets:lookup(State#httpc_man.sockets, Socket) of
        [{Socket,Timer}] ->
            cancel_timer(Timer, Socket),
            lhttpc_sock:close(Socket, State#httpc_man.ssl),
            ets:delete(State#httpc_man.sockets, Socket);
        [] ->
            ok
    end,
    State.

store_socket(Socket, State) ->
    Timeout = State#httpc_man.connection_timeout,
    Timer = case Timeout of
        infinity -> undefined;
        _Other -> erlang:send_after(Timeout, self(), {timeout, Socket})
    end,
    lhttpc_sock:setopts(Socket, [{active, once}], State#httpc_man.ssl),
    ets:insert(State#httpc_man.sockets, {Socket, Timer}),
    State#httpc_man{available_sockets = [Socket|State#httpc_man.available_sockets]}.

close_sockets(Sockets, Ssl) ->
    ets:foldl(
        fun({Socket, undefined}, _) ->
                lhttpc_sock:close(Socket, Ssl);
            ({Socket, Timer}, _) ->
                erlang:cancel_timer(Timer),
                lhttpc_sock:close(Socket, Ssl)
        end, ok, Sockets
    ).

cancel_timer(undefined, _Socket) ->
    ok;
cancel_timer(Timer, Socket) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                {timeout, Socket} -> ok
            after
                0 -> ok
            end;
        _ -> ok
    end.
