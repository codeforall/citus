-- get_rebalance_table_shards_plan shows the actual events that will be performed
-- if a rebalance operation will be performed with the same arguments, which allows users
-- to understand the impact of the change overall availability of the application and
-- network trafic.
--
DROP FUNCTION pg_catalog.get_rebalance_table_shards_plan;
CREATE OR REPLACE FUNCTION pg_catalog.get_rebalance_table_shards_plan(
        relation regclass default NULL,
        threshold float4 default NULL,
        max_shard_moves int default 1000000,
        excluded_shard_list bigint[] default '{}',
        drain_only boolean default false,
        rebalance_strategy name default NULL,
        improvement_threshold float4 DEFAULT NULL
);

-- get_rebalance_plan_for_two_nodes shows the shard movements to balance shards
-- between two specified worker nodes.
CREATE OR REPLACE FUNCTION pg_catalog.get_rebalance_plan_for_two_nodes(
    source_node_name text,
    source_node_port integer,
    target_node_name text,
    target_node_port integer,
    relation regclass DEFAULT NULL,
    excluded_shard_list bigint[] DEFAULT '{}',
    rebalance_strategy name DEFAULT NULL
    )
    RETURNS TABLE (table_name regclass,
                   shardid bigint,
                   shard_size bigint,
                   sourcename text,
                   sourceport int,
                   targetname text,
                   targetport int,
                   action text) -- New column
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;

COMMENT ON FUNCTION pg_catalog.get_rebalance_table_shards_plan(regclass, float4, int, bigint[], boolean, name, float4)
    IS 'returns the list of shard placement moves to be done on a rebalance operation';

COMMENT ON FUNCTION pg_catalog.get_rebalance_plan_for_two_nodes(text, int, text, int, regclass, bigint[], name)
    IS 'shows all shards on the source node and indicates if they should MOVE or STAY to balance with the target node';
