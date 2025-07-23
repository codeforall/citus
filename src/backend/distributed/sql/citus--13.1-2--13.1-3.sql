CREATE INDEX pg_class_relname_relnamespace_idx ON pg_catalog.pg_class (relname, relnamespace);
CREATE INDEX pg_dist_partition_colocationid_idx ON pg_dist_partition (colocationid);
