-module(grpcbox_subchannel).

-include_lib("kernel/include/logger.hrl").

-behaviour(gen_statem).

-export([start_link/5,
         conn/1,
         conn/2,
         stop/2]).
-export([init/1,
         callback_mode/0,
         terminate/3,

         %% states
         ready/3,
         disconnected/3]).

-define(RECONNECT_INTERVAL, 5000).

-record(data, {name :: any(),
               endpoint :: grpcbox_channel:endpoint(),
               channel :: grpcbox_channel:t(),
               info :: #{authority := binary(),
                         scheme := binary(),
                         encoding := grpcbox:encoding(),
                         stats_handler := module() | undefined
                        },
               conn :: h2_stream_set:stream_set() | undefined,
               conn_pid :: pid() | undefined,
               timer_ref :: reference(),
               idle_interval :: timer:time()}).

start_link(Name, Channel, Endpoint, Encoding, StatsHandler) ->
    gen_statem:start_link(?MODULE, [Name, Channel, Endpoint, Encoding, StatsHandler], []).

conn(Pid) ->
    conn(Pid, infinity).
conn(Pid, Timeout) ->
    try
        gen_statem:call(Pid, conn, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout}
    end.

stop(Pid, Reason) ->
    gen_statem:stop(Pid, Reason, infinity).

init([Name, Channel, Endpoint, Encoding, StatsHandler]) ->
    process_flag(trap_exit, true),
    gproc_pool:connect_worker(Channel, Name),
    Data = #data{name=Name,
                 timer_ref = undefined,
                 conn=undefined,
                 info=info_map(Endpoint, Encoding, StatsHandler),
                 endpoint=Endpoint,
                 channel=Channel},
    {ok, disconnected, Data, [{next_event, internal, connect}]}.

%% In case of unix socket transport
%% (defined as tuple {local, _UnixPath} in gen_tcp),
%% there is no standard on what the authority field value
%% should be, as HTTP/2 over UDS is not formally specified.
%% To follow other gRPC implementations' behavior,
%% the "localhost" value is used.
info_map({Scheme, {local, _UnixPath} = Host, Port, _, _}, Encoding, StatsHandler) ->
    case {Scheme, Port} of
        %% The ssl layer is not functional over unix sockets currently,
        %% and the port is strictly required to be 0 by gen_tcp.
        {http, 0} ->
            #{authority => <<"localhost">>,
              scheme => <<"http">>,
              encoding => Encoding,
              stats_handler => StatsHandler};
        _ ->
            error({badarg, [Scheme, Host, Port]})
    end;
info_map({http, Host, 80, _, _}, Encoding, StatsHandler) ->
    #{authority => list_to_binary(Host),
      scheme => <<"http">>,
      encoding => Encoding,
      stats_handler => StatsHandler};
info_map({https, Host, 443, _, _}, Encoding, StatsHandler) ->
    #{authority => list_to_binary(Host),
      scheme => <<"https">>,
      encoding => Encoding,
      stats_handler => StatsHandler};
info_map({Scheme, Host, Port, _, _}, Encoding, StatsHandler) ->
    #{authority => list_to_binary(Host ++ ":" ++ integer_to_list(Port)),
      scheme => atom_to_binary(Scheme, utf8),
      encoding => Encoding,
      stats_handler => StatsHandler}.

callback_mode() ->
    state_functions.

ready({call, From}, conn, #data{conn=Conn,
                                info=Info}) ->
    {keep_state_and_data, [{reply, From, {ok, Conn, Info}}]};
ready(info, {'EXIT', Pid, _}, Data=#data{conn=Pid0, name=Name, channel=Channel})
    when Pid =:= Pid0 ->
    gproc_pool:disconnect_worker({Channel, active}, Name),
    TimerRef = erlang:start_timer(?RECONNECT_INTERVAL, self(), connect),
    {next_state, disconnected, Data#data{conn=undefined, timer_ref=TimerRef}};
ready(info, {timeout, _TimerRef, connect}, _Data) ->
    keep_state_and_data;
ready(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

disconnected(internal, connect, Data) ->
    do_connect(Data);
disconnected(info, {timeout, TimerRef, connect}, #data{timer_ref=TimerRef0}=Data)
    when TimerRef =:= TimerRef0 ->
    do_connect(Data);
disconnected(info, {timeout, _TimerRef, connect}, _Data) ->
    keep_state_and_data;
disconnected({call, From}, conn, Data) ->
    connect(Data, From, [postpone]);
disconnected(info, {'EXIT', _, _}, #data{conn=undefined}) ->
    keep_state_and_data;
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event({call, From}, info, #data{info=Info}) ->
    {keep_state_and_data, [{reply, From, Info}]};
handle_event({call, From}, shutdown, _) ->
    {stop_and_reply, normal, {reply, From, ok}};
handle_event(Type, Content, Data) ->
    ?LOG_INFO("Unexpected Event type: ~p, content: ~p, data: ~p", [Type, Content, Data]),
    keep_state_and_data.

terminate(_Reason, _State, #data{conn=undefined,
                                    name=Name,
                                    channel=Channel}) ->
    gproc_pool:disconnect_worker(Channel, Name),
    gproc_pool:remove_worker(Channel, Name),
    gproc_pool:remove_worker({Channel, active}, Name),
    ok;
terminate(normal, _State, #data{conn=Pid,
                                    name=Name,
                                    channel=Channel}) ->
    h2_connection:stop(Pid),
    gproc_pool:disconnect_worker(Channel, Name),
    gproc_pool:remove_worker(Channel, Name),
    gproc_pool:disconnect_worker({Channel, active}, Name),
    gproc_pool:remove_worker({Channel, active}, Name),
    ok;
terminate(Reason, _State, #data{conn=Pid,
                                    name=Name,
                                    channel=Channel}) ->
    gproc_pool:disconnect_worker(Channel, Name),
    gproc_pool:remove_worker(Channel, Name),
    gproc_pool:disconnect_worker({Channel, active}, Name),
    gproc_pool:remove_worker({Channel, active}, Name),
    exit(Pid, Reason),
    ok.

do_connect(Data=#data{name=Name, channel=Channel,
                conn=undefined, endpoint=Endpoint}) ->
    case start_h2_client(Endpoint) of
        {ok, Pid} ->
            ?LOG_DEBUG("connect success name: ~p, channel: ~p", [Name, Channel]),
            gproc_pool:connect_worker({Channel, active}, Name),
            {next_state, ready, Data#data{conn=Pid, timer_ref=undefined}};
        {error, Reason} ->
            ?LOG_INFO("connect fail reason: ~p name: ~p, channel: ~p", [Reason, Name, Channel]),
            TimerRef = erlang:start_timer(?RECONNECT_INTERVAL, self(), connect),
            {keep_state, Data#data{timer_ref=TimerRef}}
    end.

connect(Data=#data{name=Name, channel=Channel,
                conn=undefined, endpoint=Endpoint},
                From, Actions) ->
    case start_h2_client(Endpoint) of
        {ok, Pid} ->
            gproc_pool:connect_worker({Channel, active}, Name),
            {next_state, ready, Data#data{conn=Pid, timer_ref=undefined}, Actions};
        {error, _}=Error ->
            {next_state, disconnected, Data, [{reply, From, Error}]}
    end;
connect(Data=#data{conn=Conn, conn_pid=Pid}, From, Actions) when is_pid(Pid) ->
    h2_connection:stop(Conn),
    connect(Data#data{conn=undefined, conn_pid=undefined}, From, Actions).

options(https, Options) ->
    [{client_preferred_next_protocols, {client, [<<"h2">>]}} | Options];
options(http, Options) ->
    Options.

start_h2_client({Transport, Host, Port, EndpointOptions}) ->
    % Get and delete non-ssl options from endpoint options, these are passed as connection settings
    SocketOptions = proplists:get_value(socket_options, EndpointOptions, []),
    ConnectTimeout = proplists:get_value(connect_timeout, EndpointOptions, 5000),
    TcpUserTimeout = proplists:get_value(tcp_user_timeout, EndpointOptions, 0),
    EndpointOptions2 = proplists:delete(connect_timeout, EndpointOptions),
    EndpointOptions3 = proplists:delete(tcp_user_timeout, EndpointOptions2),
    h2_client:start_link(Transport, Host, Port, options(Transport, EndpointOptions3),
                                #{garbage_on_end => true,
                                stream_callback_mod => grpcbox_client_stream,
                                connect_timeout => ConnectTimeout,
                                tcp_user_timeout => TcpUserTimeout,
                                socket_options => SocketOptions}).