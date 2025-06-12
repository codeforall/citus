-- Tests for Citus replica promotion and related UDFs
-- Version 2: Accounts for replica verification and two-stage promote/finalize

CREATE EXTENSION citus;

-- Setup: Coordinator and one worker node
SELECT citus_set_coordinator_host('localhost', पीजी_REGRESS_FUNCS_PORT_MASTER);
SELECT citus_add_node('localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_1, groupid => 1, noderole => 'primary') AS worker1_node_id \gset
SELECT pg_catalog.pg_dist_node.nodeid AS worker1_actual_nodeid FROM pg_catalog.pg_dist_node WHERE nodename = 'localhost' AND nodeport = पीजी_REGRESS_FUNCS_PORT_WORKER_1 \gset


\echo === Initial pg_dist_node state ===
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Create distributed and reference tables
CREATE TABLE dist_table (id int primary key, value text);
SELECT create_distributed_table('dist_table', 'id');
INSERT INTO dist_table SELECT i, 'value ' || i FROM generate_series(1, 10) i;

CREATE TABLE ref_table (id int primary key, data text);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table SELECT i, 'ref data ' || i FROM generate_series(1, 5) i;

-- Test citus_add_replica_node
\echo === Testing citus_add_replica_node (with verification) ===

-- Attempt to add a replica. This is EXPECTED TO FAIL in CI because
-- worker1_port is not a primary with a streaming replica on replica_port_1.
-- The UDF will try to connect to worker1 and query pg_stat_replication.
\echo Attempting to add a replica (localhost: पीजी_REGRESS_FUNCS_PORT_REPLICA_1) for primary (localhost: पीजी_REGRESS_FUNCS_PORT_WORKER_1) - EXPECTED TO FAIL
SELECT pg_catalog.citus_add_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_1, 'localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_1);

\echo Current pg_dist_node state (replica should not have been added):
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Error Case: Primary node does not exist
\echo Attempting to add replica for a non-existent primary - EXPECTED TO FAIL
SELECT pg_catalog.citus_add_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_2, 'nonexistent_primary', 1234);

-- Error Case: Trying to add an existing primary node as its own replica (should fail verification)
\echo Attempting to add worker1 as its own replica - EXPECTED TO FAIL
SELECT pg_catalog.citus_add_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_1, 'localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_1);


-- Setup for Promotion and Finalize Tests
\echo === Setup for Promotion and Finalize Rebalance Tests ===
-- Add a new primary node for these tests
SELECT citus_add_node('localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_2, groupid => 2, noderole => 'primary') AS worker2_node_id \gset
SELECT pg_catalog.pg_dist_node.nodeid AS worker2_actual_nodeid FROM pg_catalog.pg_dist_node WHERE nodename = 'localhost' AND nodeport = पीजी_REGRESS_FUNCS_PORT_WORKER_2 \gset
SELECT pg_catalog.pg_dist_node.groupid AS worker2_group_id FROM pg_catalog.pg_dist_node WHERE nodeid = :worker2_actual_nodeid \gset


-- Manually insert a "replica" entry for testing purposes, as citus_add_replica_node verification will fail in CI.
-- This simulates a replica that *was* successfully added (e.g., in an environment where verification could pass).
\echo Manually inserting a replica entry for worker2 to simulate successful citus_add_replica_node
INSERT INTO pg_catalog.pg_dist_node (nodename, nodeport, groupid, noderole, nodeisreplica, nodeprimarynodeid, isactive, shouldhaveshards, nodecluster)
VALUES ('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_2, :worker2_group_id, 'primary', true, :worker2_actual_nodeid, false, false, 'default')
RETURNING nodeid AS replica_for_worker2_nodeid \gset

\echo Current pg_dist_node state (with manually added replica):
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Test citus_promote_replica_and_rebalance
\echo === Testing citus_promote_replica_and_rebalance (expect failure at pg_promote) ===
-- This call is expected to fail when it tries to connect to the replica port ( पीजी_REGRESS_FUNCS_PORT_REPLICA_2)
-- to execute pg_promote(), as there's no real promotable instance there in CI.
\echo Attempting to promote replica node ID :replica_for_worker2_nodeid (replica of node ID :worker2_actual_nodeid)
SELECT pg_catalog.citus_promote_replica_and_rebalance(:replica_for_worker2_nodeid);

-- Simulate state after citus_promote_replica_and_rebalance *would have* updated pg_dist_node,
-- (if pg_promote call had succeeded).
\echo === Simulating successful pg_dist_node update part of promote_replica_and_rebalance for node :replica_for_worker2_nodeid ===
SELECT pg_catalog.citus_internal_get_next_group_id() AS new_group_id_for_promoted_node \gset
UPDATE pg_catalog.pg_dist_node
SET isactive = true, nodeisreplica = false, nodeprimarynodeid = 0, groupid = :new_group_id_for_promoted_node, shouldhaveshards = true
WHERE nodeid = :replica_for_worker2_nodeid;

\echo pg_dist_node state after manually simulating promotion of node :replica_for_worker2_nodeid:
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive, shouldhaveshards FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Create a test table on the original primary's group (worker2_group_id) for rebalancing
\echo === Setup dist_rebal_table for metadata rebalancing test ===
DROP TABLE IF EXISTS dist_rebal_table;
CREATE TABLE dist_rebal_table (id int primary key, value text);
-- Explicitly place on worker2_group_id. This requires knowing the groupid.
-- If create_distributed_table doesn't allow specifying group, this setup is harder.
-- Assuming worker2 is the only active primary in group 2.
SELECT create_distributed_table('dist_rebal_table', 'id', colocate_with => 'none', shard_count => 4);
-- Ensure shards are on worker2_group_id. This might require manually moving them if default placement is different.
-- For simplicity, assume create_distributed_table placed them on an existing primary group.
-- If it creates on group 1 (worker1), we need to adjust.
-- Let's try to force placement on group_id of worker2 by finding a table in that group or using a trick.
-- For now, let's assume create_distributed_table will use one of the available primary groups.
-- If it used group 1, this test needs adjustment.
-- The PL/pgSQL function iterates colocation groups on the *specified* original_group_id.
DO $$
DECLARE
    target_group_id int := (SELECT groupid FROM pg_dist_node WHERE nodeid = :worker2_actual_nodeid);
    current_group_id int;
    first_shardid bigint;
    shard_row RECORD;
BEGIN
    -- Check current placement and move if necessary
    FOR shard_row IN SELECT s.shardid, pl.groupid AS current_placement_group
                     FROM pg_dist_shard s
                     LEFT JOIN pg_dist_placement pl ON s.shardid = pl.shardid
                     WHERE s.logicalrelid = 'dist_rebal_table'::regclass
    LOOP
        IF shard_row.current_placement_group IS NULL THEN
            -- If no placement, create one on the target_group_id
            INSERT INTO pg_dist_placement (shardid, groupid, placementid, shardstate, shardlength)
            VALUES (shard_row.shardid, target_group_id, citus_next_placement_id(), 1, 0); -- Assuming shardstate 1=active, length 0
            RAISE NOTICE 'No placement for shard %, creating on group %', shard_row.shardid, target_group_id;
        ELSIF shard_row.current_placement_group != target_group_id THEN
            RAISE NOTICE 'dist_rebal_table shard % is on group %, moving to group % (worker2_group_id)', shard_row.shardid, shard_row.current_placement_group, target_group_id;
            UPDATE pg_dist_placement SET groupid = target_group_id WHERE shardid = shard_row.shardid;
        END IF;
    END LOOP;
END;
$$;
INSERT INTO dist_rebal_table SELECT i, 'rebal value ' || i FROM generate_series(1, 20) i;


\echo Shard placements for dist_rebal_table BEFORE finalize (should be on group :worker2_group_id):
SELECT p.shardid, n.nodename, n.nodeport, n.groupid, s.logicalrelid::regclass
FROM pg_dist_placement p JOIN pg_dist_node n ON p.groupid = n.groupid
JOIN pg_dist_shard s ON s.shardid = p.shardid
WHERE s.logicalrelid = 'dist_rebal_table'::regclass AND p.groupid = :worker2_group_id
ORDER BY p.shardid;

-- Test citus_finalize_replica_rebalance_metadata
\echo === Testing citus_finalize_replica_rebalance_metadata ===
-- Original primary was worker2 (nodeid :worker2_actual_nodeid, group :worker2_group_id)
-- Promoted replica is node :replica_for_worker2_nodeid (now in group :new_group_id_for_promoted_node)
SELECT pg_catalog.citus_finalize_replica_rebalance_metadata(:worker2_group_id, :new_group_id_for_promoted_node, :worker2_actual_nodeid, :replica_for_worker2_nodeid);

\echo Shard placements for dist_rebal_table AFTER finalize:
SELECT p.shardid, n.nodename, n.nodeport, n.groupid, s.logicalrelid::regclass
FROM pg_dist_placement p JOIN pg_dist_node n ON p.groupid = n.groupid
JOIN pg_dist_shard s ON s.shardid = p.shardid
WHERE s.logicalrelid = 'dist_rebal_table'::regclass
ORDER BY p.shardid, n.groupid; -- Order by groupid as well to see distribution

\echo Shard counts per group for dist_rebal_table AFTER finalize:
SELECT p.groupid, count(*) as shard_count
FROM pg_dist_placement p JOIN pg_dist_shard s ON s.shardid = p.shardid
WHERE s.logicalrelid = 'dist_rebal_table'::regclass
GROUP BY p.groupid ORDER BY p.groupid;

\echo pg_dist_cleanup entries (expecting entries for shards of dist_rebal_table):
SELECT object_type, left(object_name, 40) as object_name_prefix, group_id, cleanup_policy FROM pg_catalog.pg_dist_cleanup
WHERE object_name LIKE 'public.dist_rebal_table_%' -- Handle schema qualification more reliably
ORDER BY object_name_prefix, group_id;

-- Verify data integrity
\echo === Data Integrity Checks ===
SELECT count(*) AS count_dist_table FROM dist_table;
SELECT sum(id) AS sum_dist_table FROM dist_table;
SELECT count(*) AS count_ref_table FROM ref_table;
SELECT sum(id) AS sum_ref_table FROM ref_table;
SELECT count(*) AS count_dist_rebal_table FROM dist_rebal_table;
SELECT sum(id) AS sum_dist_rebal_table FROM dist_rebal_table;

-- Cleanup (optional, as pg_regress cleans up the instance)
DROP TABLE IF EXISTS dist_table;
DROP TABLE IF EXISTS ref_table;
DROP TABLE IF EXISTS dist_rebal_table;


-- Test citus_remove_replica_node
\echo === Testing citus_remove_replica_node ===

-- Add a new primary node for these tests
SELECT citus_add_node('localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_3, groupid => 4, noderole => 'primary') AS worker3_node_id \gset
SELECT pg_catalog.pg_dist_node.nodeid AS worker3_actual_nodeid FROM pg_catalog.pg_dist_node WHERE nodename = 'localhost' AND nodeport = पीजी_REGRESS_FUNCS_PORT_WORKER_3 \gset
SELECT pg_catalog.pg_dist_node.groupid AS worker3_group_id FROM pg_catalog.pg_dist_node WHERE nodeid = :worker3_actual_nodeid \gset


-- Manually insert an inactive replica for worker3 to test successful removal
\echo Manually inserting an inactive replica for worker3 (nodeid :worker3_actual_nodeid) to test removal
INSERT INTO pg_catalog.pg_dist_node (nodename, nodeport, groupid, noderole, nodeisreplica, nodeprimarynodeid, isactive, shouldhaveshards, nodecluster)
VALUES ('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_3, :worker3_group_id, 'primary', true, :worker3_actual_nodeid, false, false, 'default')
RETURNING nodeid AS replica_for_worker3_to_remove_nodeid \gset

-- Manually insert an *active* replica for worker3 for error testing removal
\echo Manually inserting an *active* replica for worker3 for error testing removal
INSERT INTO pg_catalog.pg_dist_node (nodename, nodeport, groupid, noderole, nodeisreplica, nodeprimarynodeid, isactive, shouldhaveshards, nodecluster)
VALUES ('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_4, :worker3_group_id, 'primary', true, :worker3_actual_nodeid, true, true, 'default')
RETURNING nodeid AS active_replica_for_error_test_nodeid \gset

\echo pg_dist_node state before remove tests:
SELECT nodeid, nodename, nodeport, groupid, isactive, nodeisreplica, nodeprimarynodeid FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Success Case: Remove the inactive replica
\echo Attempting to remove inactive replica (localhost: पीजी_REGRESS_FUNCS_PORT_REPLICA_3, nodeid :replica_for_worker3_to_remove_nodeid) - EXPECTED TO SUCCEED
SELECT pg_catalog.citus_remove_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_3);
\echo Verify replica (localhost: पीजी_REGRESS_FUNCS_PORT_REPLICA_3) is removed:
SELECT nodeid, nodename, nodeport FROM pg_catalog.pg_dist_node WHERE nodename = 'localhost' AND nodeport = पीजी_REGRESS_FUNCS_PORT_REPLICA_3; -- Should be empty

-- Error Case: Node does not exist
\echo Attempting to remove a non-existent replica - EXPECTED TO FAIL
SELECT pg_catalog.citus_remove_replica_node('nonexistent_host', 1234);

-- Error Case: Node is not a replica (target worker1 :worker1_actual_nodeid)
\echo Attempting to remove a primary node (worker1 at localhost: पीजी_REGRESS_FUNCS_PORT_WORKER_1) using citus_remove_replica_node - EXPECTED TO FAIL
SELECT pg_catalog.citus_remove_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_WORKER_1);

-- Error Case: Node is a replica but is active (target active_replica_for_error_test_nodeid)
\echo Attempting to remove an *active* replica (localhost: पीजी_REGRESS_FUNCS_PORT_REPLICA_4, nodeid :active_replica_for_error_test_nodeid) - EXPECTED TO FAIL
SELECT pg_catalog.citus_remove_replica_node('localhost', पीजी_REGRESS_FUNCS_PORT_REPLICA_4);

\echo pg_dist_node state after remove tests (active replica should still be there, worker3 and its inactive replica removed):
SELECT nodeid, nodename, nodeport, groupid, isactive, nodeisreplica, nodeprimarynodeid FROM pg_catalog.pg_dist_node ORDER BY nodeid;


\echo Final pg_dist_node state:
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive, shouldhaveshards FROM pg_catalog.pg_dist_node ORDER BY nodeid;

SELECT citus_version();
```
