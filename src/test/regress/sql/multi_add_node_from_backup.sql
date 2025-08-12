--
-- Test for adding a worker node from a backup
--

-- setup cluster
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- create a distributed table and load data
CREATE TABLE backup_test(id int, value text);
SELECT create_distributed_table('backup_test', 'id', 'hash');
INSERT INTO backup_test SELECT g, 'test' || g FROM generate_series(1, 10) g;

-- verify initial shard placement
SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

-- wait for the new node to be ready
SELECT pg_sleep(5);

-- register the new node as a clone
-- the function returns the new node id
SELECT citus_add_clone_node(:'clone_host', :worker_1_port, :'clone_host', :clone_port) AS clone_node_id \gset

-- promote the clone and rebalance the shards
SELECT citus_promote_clone_and_rebalance(:clone_node_id, 'test_campaign');

-- wait for rebalance to finish
SELECT pg_sleep(5);

-- verify that the new node is active
SELECT * FROM pg_dist_node WHERE nodeid = :clone_node_id;

-- verify the new shard placement
SELECT nodename, nodeport, count(shardid) FROM pg_dist_shard_placement GROUP BY nodename, nodeport ORDER BY nodename, nodeport;

-- verify data
SELECT count(*) FROM backup_test;
SELECT id, value FROM backup_test ORDER BY id;

-- cleanup
DROP TABLE backup_test;

-- stop the clone node
\! pg_ctl -D tmp_check/worker_clone/data stop

-- remove the clone node from the cluster
SELECT master_remove_node(:'clone_host', :clone_port);
