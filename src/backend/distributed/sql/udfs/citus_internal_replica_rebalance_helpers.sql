-- PL/pgSQL helper function for rebalancing shard placements after replica promotion

-- Helper to get a text representation of a shard for cleanup records.
-- This is a simplified version. Citus internals might have a more robust way.
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_get_qualified_shard_name(
    p_shard_id BIGINT
)
RETURNS TEXT AS $$
DECLARE
    shard_rel_oid REGCLASS;
    table_schema TEXT;
    table_name TEXT;
    shard_name_part TEXT;
BEGIN
    SELECT logicalrelid INTO shard_rel_oid FROM pg_catalog.pg_dist_shard WHERE shardid = p_shard_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Shard ID % not found in pg_dist_shard', p_shard_id;
    END IF;

    SELECT nc.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_class c JOIN pg_namespace nc ON c.relnamespace = nc.oid
    WHERE c.oid = shard_rel_oid;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Logical relation for shard ID % not found in pg_class', p_shard_id;
    END IF;

    -- Construct physical shard name part, e.g., _102045
    -- This is a common pattern, but internal Citus functions might do this differently.
    shard_name_part := '_' || p_shard_id::TEXT;

    RETURN quote_ident(table_schema) || '.' || quote_ident(table_name || shard_name_part);
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_rebalance_placements_for_promoted_node(
    p_original_group_id INT,
    p_new_group_id INT,
    p_original_primary_node_id INT, -- For cleanup context
    p_new_primary_node_id INT       -- For cleanup context
)
RETURNS VOID AS $$
DECLARE
    colocation_record RECORD;
    shard_info_record RECORD; -- To hold shardid
    shard_id_array BIGINT[];
    arr_idx INT;
    qualified_shard_name_val TEXT;
BEGIN
    -- Superuser check (belt-and-suspenders, as C UDF should check)
    IF NOT pg_has_role(current_user, 'pg_superuser', 'USAGE') AND
       NOT pg_has_role(current_user, 'azure_pg_superuser', 'USAGE') AND
       NOT pg_has_role(GetUserNameFromId(GetSessionUserId()), 'pg_superuser', 'USAGE') AND
       NOT pg_has_role(GetUserNameFromId(GetSessionUserId()), 'azure_pg_superuser', 'USAGE')
    THEN
        RAISE EXCEPTION 'citus_internal_rebalance_placements_for_promoted_node must be run by a superuser';
    END IF;

    RAISE NOTICE 'Starting rebalance from group % to group % (Original Primary NodeID: %, New Primary NodeID: %)',
                 p_original_group_id, p_new_group_id, p_original_primary_node_id, p_new_primary_node_id;

    FOR colocation_record IN
        SELECT DISTINCT dp.colocationid
        FROM pg_catalog.pg_dist_partition dp
        JOIN pg_catalog.pg_dist_shard s ON dp.relationid = s.logicalrelid
        JOIN pg_catalog.pg_dist_placement pl ON s.shardid = pl.shardid
        WHERE pl.groupid = p_original_group_id
    LOOP
        RAISE NOTICE 'Rebalancing colocation group: %', colocation_record.colocationid;

        SELECT array_agg(s.shardid ORDER BY s.shardid)
        INTO shard_id_array
        FROM pg_catalog.pg_dist_shard s
        JOIN pg_catalog.pg_dist_placement pl ON s.shardid = pl.shardid
        JOIN pg_catalog.pg_dist_partition dp ON s.logicalrelid = dp.relationid
        WHERE dp.colocationid = colocation_record.colocationid
          AND pl.groupid = p_original_group_id;

        IF shard_id_array IS NULL THEN
            RAISE NOTICE '  No shards found for colocation group % on original primary group %.',
                         colocation_record.colocationid, p_original_group_id;
            CONTINUE;
        END IF;

        RAISE NOTICE '  Found % shards for colocation group % on group % to rebalance.',
                     array_length(shard_id_array, 1), colocation_record.colocationid, p_original_group_id;

        arr_idx := 1;
        FOREACH shard_info_record.shardid IN ARRAY shard_id_array
        LOOP
            qualified_shard_name_val := pg_catalog.citus_internal_get_qualified_shard_name(shard_info_record.shardid);

            IF arr_idx % 2 = 1 THEN
                -- Assign to new primary: update placement, schedule cleanup on old primary
                RAISE NOTICE '  Assigning shard % (%) to new primary (group %)', shard_info_record.shardid, qualified_shard_name_val, p_new_group_id;

                UPDATE pg_catalog.pg_dist_placement
                SET groupid = p_new_group_id
                WHERE shardid = shard_info_record.shardid AND groupid = p_original_group_id;

                INSERT INTO pg_catalog.pg_dist_cleanup (object_type, object_name, group_id, cleanup_policy)
                VALUES ('shard_placement', qualified_shard_name_val, p_original_group_id, 'deferred_on_success')
                ON CONFLICT (object_type, object_name, group_id) DO NOTHING; -- Avoid duplicate cleanup tasks

            ELSE
                -- Assign to original primary: placement is fine, schedule cleanup on new primary
                RAISE NOTICE '  Shard % (%) remains on original primary (group %)', shard_info_record.shardid, qualified_shard_name_val, p_original_group_id;

                INSERT INTO pg_catalog.pg_dist_cleanup (object_type, object_name, group_id, cleanup_policy)
                VALUES ('shard_placement', qualified_shard_name_val, p_new_group_id, 'deferred_on_success')
                ON CONFLICT (object_type, object_name, group_id) DO NOTHING; -- Avoid duplicate cleanup tasks
            END IF;
            arr_idx := arr_idx + 1;
        END LOOP;
    END LOOP;

    -- The C code will call CitusInvalidateRelcacheByRelid after this function.
    RAISE NOTICE 'Metadata rebalancing updates complete.';
    RETURN;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_internal_rebalance_placements_for_promoted_node(INT, INT, INT, INT)
IS 'Internal helper to rebalance shard placements between an old and new primary group by updating pg_dist_placement and scheduling cleanup tasks. Called by citus_promote_replica_and_rebalance.';

COMMENT ON FUNCTION pg_catalog.citus_internal_get_qualified_shard_name(BIGINT)
IS 'Internal helper to construct a qualified text representation of a shard name for cleanup records.';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_get_next_group_id()
RETURNS INT AS $$
DECLARE
    v_next_group_id INT;
BEGIN
    -- Ensure this function is only callable by superusers
    -- Note: GetSessionUserId() is preferred over current_user for security definer like contexts
    IF NOT pg_has_role(GetUserNameFromId(GetSessionUserId()), 'pg_superuser', 'USAGE') AND
       NOT pg_has_role(GetUserNameFromId(GetSessionUserId()), 'azure_pg_superuser', 'USAGE') THEN
        RAISE EXCEPTION 'citus_internal_get_next_group_id must be run by a superuser';
    END IF;

    SELECT nextval('pg_catalog.pg_dist_groupid_seq'::regclass) INTO v_next_group_id;

    -- Basic sanity check: groupid 0 is reserved for the coordinator.
    -- nextval should not return 0 if sequence is set up correctly (e.g., minvalue 1, or if it cycles and 0 is not allowed).
    -- pg_dist_groupid_seq should be configured to not produce 0.
    IF v_next_group_id = 0 THEN
        RAISE EXCEPTION 'pg_dist_groupid_seq returned 0, which is reserved for the coordinator group. Sequence configuration error.';
    END IF;

    RETURN v_next_group_id;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION pg_catalog.citus_internal_get_next_group_id()
IS 'Internal helper to get the next available group ID from the pg_dist_groupid_seq sequence. Ensures non-zero ID. Called by Citus C functions.';
