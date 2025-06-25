-- Setup: Create distributed tables and add worker nodes
SELECT master_add_node('localhost', 5432);
SELECT master_add_node('localhost', 5433);
SELECT master_add_node('localhost', 5434); -- Third node for some tests

-- Create some distributed tables
CREATE TABLE dist_table1 (id int primary key, val text);
SELECT create_distributed_table('dist_table1', 'id');

CREATE TABLE dist_table2 (key int primary key, data jsonb);
SELECT create_distributed_table('dist_table2', 'key');

-- Insert some data to create shards
INSERT INTO dist_table1 SELECT i, 'value_' || i FROM generate_series(1, 20) i;
INSERT INTO dist_table2 SELECT i, jsonb_build_object('i', i) FROM generate_series(1, 10) i;

-- Initially, all shards will be on the first node (5432) or distributed by default placement strategy.
-- For consistent testing, let's move some shards explicitly to node1 (localhost:5432)
-- This requires knowing shard names, which can be complex.
-- A simpler approach for testing is to assume default strategy places them on the first available node
-- or use rebalance_table_shards to consolidate on one node first if needed.
-- For this test, we'll assume shards are somewhat distributed and then test moving specific ones.

-- To ensure node 1 (localhost:5432) has shards and node 2 (localhost:5433) is empty:
-- We might need a helper to move all shards of a table to a specific node first.
-- For now, let's rely on the UDF to pick up shards from node1.
-- Manually verify initial shard distribution if tests are flaky.
SELECT table_name, shardid, nodename, nodeport FROM pg_dist_placement WHERE nodename = 'localhost' AND nodeport = 5432 ORDER BY shardid;

-- Test 1: Basic rebalance from node1 to node2 (empty)
-- Node1: localhost:5432, Node2: localhost:5433
SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5433)
ORDER BY shardid;

-- Manually move some shards to node 2 based on the plan for next tests
-- This is tricky as shardids are dynamic.
-- Instead, let's test with existing distribution and then specific table.

-- Test 2: Rebalance for a specific table (dist_table1)
SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5433, 'dist_table1'::regclass)
ORDER BY shardid;

-- Test 3: Rebalance with excluded shards
-- To get some shard IDs to exclude:
CREATE TEMP TABLE all_shards_on_node1 AS
SELECT shardid FROM pg_dist_shard_placement
JOIN pg_dist_shard USING (shardid)
WHERE nodename = 'localhost' AND nodeport = 5432;

CREATE TEMP TABLE excluded_ids AS SELECT shardid FROM all_shards_on_node1 LIMIT 2;
SELECT * FROM excluded_ids; -- show excluded ids

SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes(
    'localhost', 5432, 'localhost', 5433,
    excluded_shard_list := (SELECT array_agg(shardid) FROM excluded_ids)
)
ORDER BY shardid;

DROP TABLE excluded_ids;
DROP TABLE all_shards_on_node1;


-- Test 4: Source node has no shards (or no relevant shards for a table)
-- Move all shards of dist_table2 to node3 (localhost:5434)
-- This part is hard to automate perfectly without knowing shard names / more complex shard moving UDFs.
-- For now, we'll simulate by trying to rebalance dist_table2 from node1, assuming it has no dist_table2 shards.
-- This requires that dist_table2 shards are NOT on localhost:5432.
-- A more robust way would be to create a new table that is guaranteed to not be on node1.
CREATE TABLE dist_empty_source_test (id int primary key);
SELECT create_distributed_table('dist_empty_source_test', 'id');
-- Ensure its shards are on node3 if possible, or just check that node1 has none.
-- For this test, we assume dist_empty_source_test shards are not on localhost:5432.
-- Or, simpler, try to rebalance a table that does not exist on source.
SELECT master_move_shard_placement((SELECT shardid FROM pg_dist_shard JOIN pg_dist_placement USING(shardid) WHERE logicalrelid = 'dist_table2'::regclass LIMIT 1), 'localhost', 5432, 'localhost', 5434);
SELECT master_move_shard_placement((SELECT shardid FROM pg_dist_shard JOIN pg_dist_placement USING(shardid) WHERE logicalrelid = 'dist_table2'::regclass OFFSET 1 LIMIT 1), 'localhost', 5432, 'localhost', 5434);


SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5433, 'dist_empty_source_test'::regclass)
ORDER BY shardid;

-- Test 5: Target node is the same as source node (should produce no plan)
SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5432)
ORDER BY shardid;


-- Test 6: Using a different rebalance strategy (if one exists that affects placement decisions)
-- This requires defining a custom strategy or using one that behaves differently.
-- For now, assume default 'by_count' or 'by_disk_size' (if available)
-- Create a strategy that disallows moves to target to test shardAllowedOnNode
INSERT INTO pg_dist_rebalance_strategy VALUES
('no_move_to_target_strategy', false, 'citus_shard_cost_by_disk_size', 'citus_node_capacity_by_disk_size', 'custom_shard_allowed_on_node', 0.1, 0.1);

-- This function needs to be defined in C or PL/pgSQL and loaded.
-- For a test, we can use a plpgsql function.
CREATE OR REPLACE FUNCTION custom_shard_allowed_on_node(shard_id bigint, node_id int)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    target_node_id int;
BEGIN
    -- Get node_id for localhost:5433
    SELECT n.nodeid INTO target_node_id FROM pg_dist_node n WHERE n.nodename = 'localhost' AND n.nodeport = 5433;
    IF node_id = target_node_id THEN
        RETURN false; -- Disallow moves to target node (localhost:5433)
    END IF;
    RETURN true;
END;
$$;

SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5433, rebalance_strategy => 'no_move_to_target_strategy')
ORDER BY shardid;

-- Clean up custom strategy and function
DELETE FROM pg_dist_rebalance_strategy WHERE strategy_name = 'no_move_to_target_strategy';
DROP FUNCTION custom_shard_allowed_on_node(bigint, int);


-- Test 7: No distributed tables exist (after dropping them)
DROP TABLE dist_table1;
DROP TABLE dist_table2;
DROP TABLE dist_empty_source_test;

SELECT shardid, sourcename, sourceport, targetname, targetport
FROM pg_catalog.get_rebalance_plan_for_two_nodes('localhost', 5432, 'localhost', 5433)
ORDER BY shardid;

-- Clean up nodes
SELECT master_remove_node('localhost', 5432);
SELECT master_remove_node('localhost', 5433);
SELECT master_remove_node('localhost', 5434);
