CREATE OR REPLACE FUNCTION pg_catalog.citus_finalize_replica_rebalance_metadata(
    original_primary_group_id INT,
    new_primary_group_id INT,
    original_primary_node_id INT,
    new_primary_node_id INT
)
RETURNS VOID
AS 'MODULE_PATHNAME', 'citus_finalize_replica_rebalance_metadata'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_catalog.citus_finalize_replica_rebalance_metadata(INT, INT, INT, INT)
IS 'Internal. Finalizes shard placement metadata after a replica promotion by calling the PL/pgSQL helper. This UDF is intended to be called as part of a broader promotion workflow, typically after the replica has been promoted and its pg_dist_node entry updated.';
