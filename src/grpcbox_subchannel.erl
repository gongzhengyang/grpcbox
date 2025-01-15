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
        conn=undefined,
        conn_pid=undefined,
        timer_ref = undefined,
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
ready(info, {'EXIT', Pid, _}, Data=#data{conn_pid=Pid0, name=Name, channel=Channel})
    when Pid =:= Pid0 ->
    gproc_pool:disconnect_worker({Channel, active}, Name),
    TimerRef = erlang:start_timer(?RECONNECT_INTERVAL, self(), connect),
    {next_state, disconnected, Data#data{conn=undefined, conn_pid=undefined, timer_ref=TimerRef}};
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
disconnected(info, {'EXIT', _, _}, #data{conn=undefined, conn_pid=undefined}) ->
    keep_state_and_data;
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event({call, From}, info, #data{info=Info}) ->
    {keep_state_and_data, [{reply, From, Info}]};
handle_event({call, From}, shutdown, _) ->
    {stop_and_reply, normal, {reply, From, ok}};
handle_event(info, {'EXIT', _, econnrefused}, #data{conn=undefined, conn_pid=undefined}) ->
    keep_state_and_data;
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
terminate(normal, _State, #data{conn=Conn,
                                name=Name,
                                channel=Channel}) ->
    h2_connection:stop(Conn),
    gproc_pool:disconnect_worker(Channel, Name),
    gproc_pool:remove_worker(Channel, Name),
    gproc_pool:disconnect_worker({Channel, active}, Name),
    gproc_pool:remove_worker({Channel, active}, Name),
    ok;
terminate(Reason, _State, #data{conn_pid=Pid,
                                name=Name,
                                channel=Channel}) ->
    exit(Pid, Reason),
    gproc_pool:disconnect_worker(Channel, Name),
    gproc_pool:remove_worker(Channel, Name),
    gproc_pool:disconnect_worker({Channel, active}, Name),
    gproc_pool:remove_worker({Channel, active}, Name),
    ok.

do_connect(Data=#data{conn=undefined,
                      channel=Channel,
                      name=Name,
                      endpoint={Transport, Host, Port, SSLOptions, ConnectionSettings}}) ->
    case h2_client:start_link(Transport, Host, Port, options(Transport, SSLOptions),
                              ConnectionSettings#{garbage_on_end => true,
                                                  stream_callback_mod => grpcbox_client_stream}) of
        {ok, Conn} ->
            Pid = h2_stream_set:connection(Conn),
            gproc_pool:connect_worker({Channel, active}, Name),
            {next_state, ready, Data#data{conn=Conn, conn_pid=Pid, timer_ref=undefined}};
        {error, Reason}=_Error ->
            ?LOG_INFO("connect fail reason: ~p name: ~p, channel: ~p", [Reason, Name, Channel]),
            TimerRef = erlang:start_timer(?RECONNECT_INTERVAL, self(), connect),
            {keep_state, Data#data{timer_ref=TimerRef}}
    end.

connect(Data=#data{conn=undefined,
                   channel=Channel,
                   name=Name,
                   endpoint={Transport, Host, Port, SSLOptions, ConnectionSettings}}, From, Actions) ->
    case h2_client:start_link(Transport, Host, Port, options(Transport, SSLOptions),
                              ConnectionSettings#{garbage_on_end => true,
                                                  stream_callback_mod => grpcbox_client_stream}) of
        {ok, Conn} ->
            Pid = h2_stream_set:connection(Conn),
            gproc_pool:connect_worker({Channel, active}, Name),
            {next_state, ready, Data#data{conn=Conn, conn_pid=Pid}, Actions};
        {error, _}=Error ->
            {next_state, disconnected, Data#data{conn=undefined}, [{reply, From, Error}]}
    end;
connect(Data=#data{conn=Conn, conn_pid=Pid}, From, Actions) when is_pid(Pid) ->
    h2_connection:stop(Conn),
    connect(Data#data{conn=undefined, conn_pid=undefined}, From, Actions).

options(https, Options) ->
    [{client_preferred_next_protocols, {client, [<<"h2">>]}} | Options];
options(http, Options) ->
    Options.
