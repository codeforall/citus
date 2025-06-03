-- Tests for Citus replica promotion and related UDFs

CREATE EXTENSION citus;

-- Setup: Coordinator and one worker node
SELECT citus_set_coordinator_host('localhost', 5432); -- Use a common port for coordinator
SELECT citus_add_node('localhost', 5433, groupid => 1); -- Worker 1, Group 1

-- Verify initial node setup
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Create distributed and reference tables
CREATE TABLE dist_table (id int primary key, value text);
SELECT create_distributed_table('dist_table', 'id');
INSERT INTO dist_table SELECT i, 'value ' || i FROM generate_series(1, 10) i;

CREATE TABLE ref_table (id int primary key, data text);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table SELECT i, 'ref data ' || i FROM generate_series(1, 5) i;

-- Test citus_add_replica_node
\echo === Testing citus_add_replica_node ===

-- Success Case
SELECT pg_catalog.citus_add_replica_node('localhost', 5434, 'localhost', 5433) AS replica_node_id_1;

-- Verify replica node entry
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node WHERE nodename = 'localhost' AND nodeport = 5434 ORDER BY nodeid;
-- Check that primary node (ID 2, assuming worker1 was ID 2) is unchanged except perhaps for its replicas list (not directly visible here)
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node WHERE nodeid = 2 ORDER BY nodeid;


-- Error Case: Primary node does not exist
SELECT pg_catalog.citus_add_replica_node('localhost', 5435, 'nonexistent_primary', 1234);

-- Error Case: Replica hostname/port already exists (try adding the same replica again)
SELECT pg_catalog.citus_add_replica_node('localhost', 5434, 'localhost', 5433);

-- Error Case: Primary is itself a replica (cannot test this easily yet without promoting a replica first)
-- We will add a placeholder for this test idea
\echo TODO: Test error case where primary is itself a replica.

-- Error Case: Replica hostname/port already exists but is a primary
SELECT pg_catalog.citus_add_replica_node('localhost', 5433, 'localhost', 5433); -- Trying to add worker1 as its own replica.

-- Test citus_promote_replica_and_rebalance
\echo === Testing citus_promote_replica_and_rebalance ===

-- Setup for promotion: Add another replica that we will attempt to promote
-- Use different port for this new replica to avoid conflict with previous tests
SELECT pg_catalog.citus_add_node('localhost', 5436, groupid => 2) AS original_primary_for_promo_test_node_id; -- This will be our "primary" (nodeid 4)
SELECT pg_catalog.citus_add_replica_node('localhost', 5437, 'localhost', 5436) AS replica_to_promote_node_id; -- This is the replica (nodeid 5)

SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

-- Get the nodeid of the replica we just added (nodeid 5)
-- Note: In pg_regress, we can't easily use variables from previous SELECTs directly in subsequent DML/UDF calls.
-- We'll assume replica_to_promote_node_id is 5 based on sequential nodeid assignment.
-- A more robust test might use a DO block or helper functions if this becomes flaky.
-- For now, we rely on the typical sequential node ID assignment.
-- Node IDs: Coord=1, Worker1=2, Replica1=3, PrimaryForPromo=4, ReplicaToPromote=5

\echo Attempting to promote replica node ID 5 (replica of node ID 4)

-- To simulate external promotion for testing the rebalance part:
-- 1. The UDF internally waits for WAL catchup (cannot easily test this part in CI without actual replication).
-- 2. The UDF then asks the user to promote the PG instance and waits for pg_is_in_recovery() to be false.
-- We will simulate this by directly calling the UDF.
-- The UDF has internal NOTICE messages for these steps. We will expect to see them.
-- The UDF also has a placeholder for GetNextGroupId. We expect a WARNING for that.

-- For pg_regress, we can't interactively promote. The UDF will likely timeout on pg_is_in_recovery()
-- unless we can somehow mock/trick it.
-- For now, let's test the error path if the replica is not "promoted" (i.e., pg_is_in_recovery is true).
-- This requires the UDF to be able to connect to the replica node port.
-- In pg_regress, these ports are not actually running separate PostgreSQL instances.
-- So, the connection attempt itself will likely fail for node 5437.

-- Test Error Case: Replica node ID does not exist
SELECT pg_catalog.citus_promote_replica_and_rebalance(999);

-- Test Error Case: Node ID is not a replica
SELECT pg_catalog.citus_promote_replica_and_rebalance(2); -- Worker 1 is not a replica

-- Test Error Case: Replica's primary does not exist (hard to set up without direct DML on pg_dist_node)
\echo TODO: Test error case where replica primary node does not exist.

-- "Happy Path" test (Simulated Promotion & Rebalance)
-- Given the limitations of pg_regress for actual replication & promotion,
-- we focus on the metadata changes and rebalance logic *after* simulated promotion.
-- The UDF currently has a placeholder for GetNextGroupId and will emit a WARNING.
-- The UDF will also emit NOTICEs for WAL sync and promotion steps.
-- The actual connection attempts to the replica for pg_is_in_recovery will fail in CI.
-- So this call will likely error out when it tries to connect to the replica.
-- We will expect this error.
SELECT pg_catalog.citus_promote_replica_and_rebalance(5);


-- Verification after (attempted) promotion of node 5:
-- Since the above call is expected to fail when it tries to connect to the replica to check pg_is_in_recovery(),
-- we won't see the metadata changes or rebalancing yet.
-- A more advanced test setup would be needed to mock the replica's state.

-- For now, let's verify the state of the nodes as they were before the failing promotion call.
\echo Final state of pg_dist_node after tests:
SELECT nodeid, groupid, nodename, nodeport, noderole, nodeisreplica, nodeprimarynodeid, isactive FROM pg_catalog.pg_dist_node ORDER BY nodeid;

\echo Shard placements for dist_table (should be on group 1, original primary for promo test node has group 2):
SELECT p.shardid, n.nodename, n.nodeport, n.groupid
FROM pg_dist_placement p JOIN pg_dist_node n ON p.groupid = n.groupid
JOIN pg_dist_shard s ON s.shardid = p.shardid
WHERE s.logicalrelid = 'dist_table'::regclass
ORDER BY p.shardid, n.nodename, n.nodeport;

-- TODO: Add a test where promotion is "successful" by manually updating pg_dist_node
-- to simulate the replica being promoted (isActive=true, nodeisreplica=false etc BEFORE calling promote)
-- and then verify rebalancing. This would test the latter half of the UDF.
-- However, the UDF's internal checks for replica status might prevent this.
-- A dedicated "rebalance_from_former_primary" UDF might be easier to test in isolation.

\echo === Manual Simulation of Successful Promotion for Rebalance Test ===
-- Let's assume replica node 3 (replica of node 2) was externally promoted.
-- Update its metadata to reflect this state *before* calling a rebalance (if rebalance was separate).
-- Since citus_promote_replica_and_rebalance does it all, we can't easily inject this.

-- What we can do is:
-- 1. Add a replica.
-- 2. Manually update its pg_dist_node entry to look like it's been promoted (active, not replica, new groupid).
-- 3. Then call a *new, separate* UDF like `citus_rebalance_shards_from_group(source_group_id, target_group_id, ratio)`
-- This is beyond the current subtask of testing the existing UDF.

-- For the existing UDF, the most we can test in a simple regress is:
-- - Initial validations (non-existent replica, non-replica node)
-- - The NOTICE messages for WAL sync and promotion instructions.
-- - The error that occurs when it tries to connect to the replica to check pg_is_in_recovery.

-- Cleanup (optional, as regress usually runs in a temporary DB)
-- DROP TABLE dist_table;
-- DROP TABLE ref_table;
-- SELECT citus_remove_node('localhost', 5437);
-- SELECT citus_remove_node('localhost', 5436);
-- SELECT citus_remove_node('localhost', 5434);
-- SELECT citus_remove_node('localhost', 5433);
-- SELECT citus_remove_node('localhost', 5432); -- Might fail if coordinator

SELECT citus_version();
SELECT pg_extension_version('citus');

-- Show all notices/warnings that might have been suppressed during function calls
SET client_min_messages TO DEBUG1;
SHOW client_min_messages;
