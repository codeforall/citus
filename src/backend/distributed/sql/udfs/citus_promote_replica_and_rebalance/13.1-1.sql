CREATE OR REPLACE FUNCTION pg_catalog.citus_promote_replica_and_rebalance(
    replica_nodeid integer
)
RETURNS VOID
AS 'MODULE_PATHNAME', $$citus_promote_replica_and_rebalance$$
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_catalog.citus_promote_replica_and_rebalance(integer) IS
'Promotes a registered replica node to a primary, performs necessary metadata updates, and rebalances a portion of shards from its original primary to the newly promoted node.';

REVOKE ALL ON FUNCTION pg_catalog.citus_promote_replica_and_rebalance(integer) FROM PUBLIC;
