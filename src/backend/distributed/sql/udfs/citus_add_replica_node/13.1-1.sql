CREATE OR REPLACE FUNCTION pg_catalog.citus_add_replica_node(
    replica_hostname TEXT,
    replica_port INT,
    primary_hostname TEXT,
    primary_port INT
)
RETURNS INT
AS 'MODULE_PATHNAME', 'citus_add_replica_node'
LANGUAGE C VOLATILE STRICT;

-- Grant execute to superusers or appropriate roles as needed.
-- By default, new functions in pg_catalog are executable by public.
-- If more restrictive permissions are desired, they should be set here.
-- For example:
-- REVOKE EXECUTE ON FUNCTION pg_catalog.citus_add_replica_node(TEXT, INT, TEXT, INT) FROM PUBLIC;
-- GRANT EXECUTE ON FUNCTION pg_catalog.citus_add_replica_node(TEXT, INT, TEXT, INT) TO superuser_role;

COMMENT ON FUNCTION pg_catalog.citus_add_replica_node(TEXT, INT, TEXT, INT) IS
'Adds a new node as a replica of an existing primary node. The replica is initially inactive. Returns the nodeid of the new replica node.';
