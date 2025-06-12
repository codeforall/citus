CREATE OR REPLACE FUNCTION pg_catalog.citus_remove_replica_node(
    nodename TEXT,
    nodeport INT
)
RETURNS VOID
AS 'MODULE_PATHNAME', 'citus_remove_replica_node'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_catalog.citus_remove_replica_node(TEXT, INT)
IS 'Removes an inactive streaming replica node from Citus metadata. Errors if the node is not found, not registered as a replica, or is currently marked active.';
