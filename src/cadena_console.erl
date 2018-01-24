-module(cadena_console).

-export([join/1,
         create/1,
         ensemble_status/1]).

-export([put_data/2,
         get_data/1,
         delete_data/1,
         update_data/2]).

%%
% CLUSTER COMMANDS
%%
join([NodeStr]) ->
    % node name comes as a list string, we need it as an atom
	Node = list_to_atom(NodeStr),
    % check that the node exists and is alive
	case net_adm:ping(Node) of
        % if not, return an error
		pang ->
			{error, not_reachable};
        % if it replies, let's join him passing our node reference
		pong ->
			riak_ensemble_manager:join(Node, node())
	end.

create([]) ->
    % enable riak_ensemble_manager
    riak_ensemble_manager:enable(),
    % wait until it stabilizes
    wait_stable().

ensemble_status([]) ->
	cluster_status(),
    ok.

cluster_status() ->
    case riak_ensemble_manager:enabled() of
        false ->
            {error, not_enabled};
        true ->
            Nodes = lists:sort(riak_ensemble_manager:cluster()),
            io:format("Nodes in cluster: ~p~n",[Nodes]),
            LeaderNode = node(riak_ensemble_manager:get_leader_pid(root)),
            io:format("Leader: ~p~n",[LeaderNode])
    end.

%% Internal functions

wait_stable() ->
    case check_stable() of
        true ->
            ok;
        false ->
            wait_stable()
    end.

check_stable() ->
    case riak_ensemble_manager:check_quorum(root, 1000) of
        true ->
            case riak_ensemble_peer:stable_views(root, 1000) of
                {ok, true} ->
                    true;
                _ ->
                    false
            end;
        false ->
            false
    end.

%%
% DATA COMMANDS
%%

put_data(Key, Value) ->
    Timeout = 1000,
    Ensemble = root,
    riak_ensemble_client:kover(Ensemble, Key, Value, Timeout).

get_data(Key) ->
    Timeout = 1000,
    Ensemble = root,
    riak_ensemble_client:kget(Ensemble, Key, Timeout).

delete_data(Key) ->
    Timeout = 1000,
    Ensemble = root,
    riak_ensemble_client:kdelete(Ensemble, Key, Timeout).

update_data(Key1, NewVal) ->
    Timeout = 1000,
    Ensemble = root,
    DefaultVal = <<"v0">>,

    riak_ensemble_peer:kmodify(node(), Ensemble, Key1,
        fun({Epoch, Seq}, CurVal) ->
            io:format("CurVal: ~p ~p ~p to ~p~n", [Epoch, Seq, CurVal, NewVal]),
            NewVal
        end,
    DefaultVal, Timeout).