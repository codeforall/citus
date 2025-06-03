-- Add replica information columns to pg_dist_node
ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN nodeisreplica BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN nodeprimarynodeid INT4 NULL DEFAULT NULL;

-- It's good practice to bump the catalog version after schema changes
-- This command might vary depending on the project's specific catalog versioning mechanism
-- For Citus, this is typically handled by pg_extension_config_dump and CREATE EXTENSION ... FROM
-- For now, we'll assume a manual version bump or that it's handled by the extension upgrade script.
-- Example (conceptual, actual command might differ):
-- SELECT pg_catalog.pg_extension_config_dump('pg_dist_node', '');
-- UPDATE pg_catalog.pg_extension SET extversion = '12.0.X' WHERE extname = 'citus';

-- Add a comment to the table and columns for clarity in \d output
COMMENT ON COLUMN pg_catalog.pg_dist_node.nodeisreplica IS 'Indicates if this node is a replica of another node.';
COMMENT ON COLUMN pg_catalog.pg_dist_node.nodeprimarynodeid IS 'If nodeisreplica is true, this stores the nodeid of its primary node.';
