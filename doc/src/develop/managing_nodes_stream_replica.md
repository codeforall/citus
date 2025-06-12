# Adding and Promoting Worker Nodes via Streaming Replication

## Overview

This feature allows you to expand your Citus cluster by adding a new worker node that starts its life as a PostgreSQL streaming replica of an existing Citus worker node. The process involves registering the replica, promoting it to a standalone PostgreSQL primary, updating Citus metadata to reflect this new primary, and then rebalancing shard placements from its original primary node to the new one.

This approach is useful for minimizing the load of initial data copying on the primary worker by leveraging PostgreSQL's built-in streaming replication.

The high-level workflow involves these stages:

1.  **Manual PostgreSQL Replica Setup:** The database administrator manually sets up a new PostgreSQL instance as a streaming replica of an active Citus worker node. This is done using standard PostgreSQL replication procedures.
2.  **Register Replica with Citus:** The administrator then registers this PostgreSQL replica with the Citus coordinator using the `citus_add_replica_node` UDF. This function now includes a verification step to confirm the replica is actively streaming from the specified primary.
3.  **Promote Replica and Update Node Metadata:** The administrator calls the `citus_promote_replica_and_rebalance` UDF. This UDF orchestrates:
    *   Waiting for the replica to synchronize WAL with its primary.
    *   Automatically promoting the PostgreSQL replica to a primary instance using `pg_promote()`.
    *   Updating the replica's entry in `pg_catalog.pg_dist_node` to mark it as an active, primary Citus worker with a new, unique group ID.
4.  **Finalize Shard Rebalance:** After the previous step is successful, the administrator calls the `citus_finalize_replica_rebalance_metadata` UDF. This function performs the metadata operations to reassign shard mastership, effectively rebalancing placements between the original primary and the newly promoted node.

## Prerequisites

*   A healthy, running Citus cluster.
*   Successful configuration of PostgreSQL streaming replication from an existing, active Citus worker node (the "primary") to a new PostgreSQL instance (the "replica").
    *   Refer to the official PostgreSQL documentation for setting up streaming replication.
*   The replica PostgreSQL instance must **not** yet be promoted to a primary when calling `citus_add_replica_node`. It should be a standby server, actively replicating.
*   **Highly Recommended for Verification:** For reliable verification by `citus_add_replica_node` and WAL lag monitoring by `citus_promote_replica_and_rebalance`, configure a unique `application_name` in the replica's `primary_conninfo` setting (in `postgresql.conf` or `standby.signal` / `recovery.conf` depending on your PostgreSQL version). It's good practice for this `application_name` to match the replica's hostname or be otherwise easily identifiable.
*   The PostgreSQL user connecting to the replica to run `pg_promote()` (as part of `citus_promote_replica_and_rebalance`) must have superuser privileges or sufficient permissions to execute `pg_promote()`.

## New User-Defined Functions (UDFs)

Three UDFs are involved in this process:

### `citus_add_replica_node`

*   **Syntax:**
    ```sql
    citus_add_replica_node(
        replica_hostname TEXT,
        replica_port INT,
        primary_hostname TEXT,
        primary_port INT
    )
    RETURNS INT
    ```
*   **Description:**
    Registers a PostgreSQL streaming replica as a potential Citus worker node within the Citus metadata. This function links the replica to its primary Citus worker and records its connection information. It includes a crucial verification step to ensure the replica is actively streaming from the specified primary before registration.
*   **Arguments:**
    *   `replica_hostname`: The hostname or IP address of the streaming replica instance.
    *   `replica_port`: The port number of the streaming replica instance.
    *   `primary_hostname`: The hostname or IP address of the existing Citus worker node from which the replica is streaming.
    *   `primary_port`: The port number of the existing Citus worker node.
*   **Return Value:**
    The `nodeid` of the newly registered replica node in `pg_catalog.pg_dist_node`.
*   **Details:**
    *   **Replica Verification:** Before registering the replica, `citus_add_replica_node` connects to the specified primary node (`primary_hostname`:`primary_port`) and queries the `pg_catalog.pg_stat_replication` view. It verifies that an entry exists corresponding to the replica (attempting to match by `application_name`, `client_addr`, or `client_hostname` using the provided `replica_hostname`) and that its replication `state` is 'streaming'. If no such active streaming replica entry is found, the function will raise an error, preventing the registration of a node that is not confirmed to be streaming from the specified primary.
    *   The replica is added to the Citus metadata as an inactive node (`isActive=false` and `shouldHaveShards=false` by default).
    *   It is explicitly marked as a replica (`nodeisreplica=true`) and associated with its primary via `nodeprimarynodeid`.
    *   This function must be called **before** the replica is promoted to a primary at the PostgreSQL level.
    *   The replica will typically share the same `groupid`, `noderole`, and `nodecluster` as its primary upon registration.
*   **Important Note on Verification:** For the most reliable replica verification, ensure the `application_name` in the replica's `primary_conninfo` (e.g., in `postgresql.conf` or the connection string used by the replica) is set to a unique identifier, ideally the `replica_hostname` itself. This allows Citus to accurately find its entry in `pg_stat_replication`.
*   **Example:**
    ```sql
    -- Assuming 'worker1.example.com:5432' is the primary Citus worker
    -- and 'replica1.example.com:5432' is its streaming replica,
    -- with application_name='replica1.example.com' set on the replica.
    SELECT citus_add_replica_node('replica1.example.com', 5432, 'worker1.example.com', 5432);
    -- Returns the new nodeid for replica1.example.com
    ```

### `citus_promote_replica_and_rebalance` (Now primarily `citus_promote_replica_node_metadata`)

*   **Syntax:**
    ```sql
    citus_promote_replica_and_rebalance(replica_nodeid INT)
    RETURNS VOID
    ```
    *(Note: The name `citus_promote_replica_and_rebalance` is kept for this step as per the subtask, but its role has focused. A rename to e.g., `citus_promote_replica_to_primary` might be clearer in the future.)*
*   **Description:**
    Orchestrates the promotion of a registered replica to an active Citus worker. This function handles waiting for WAL synchronization, attempts to automatically promote the PostgreSQL replica instance, and then updates its metadata in `pg_catalog.pg_dist_node` to reflect its new status as an active primary in a new group. **The actual shard placement rebalancing is now finalized by `citus_finalize_replica_rebalance_metadata`.**
*   **Arguments:**
    *   `replica_nodeid`: The `nodeid` (from `pg_catalog.pg_dist_node`) of the replica previously registered with `citus_add_replica_node`.
*   **Workflow Stages Orchestrated by this UDF:**
    1.  **Blocks Writes:** Temporarily blocks writes to shards on the original primary node.
    2.  **WAL Sync Wait:** Connects to the original primary node and monitors `pg_stat_replication` to ensure the specified replica has replayed all WAL records and is fully caught up. The UDF attempts to identify the replica using its `application_name` (ideally matching the replica's hostname, as registered) or its client address. This step has a timeout (default: 5 minutes).
    3.  **Automated PostgreSQL Promotion:** Connects to the replica node and executes `SELECT pg_promote(wait := true);` using SPI to promote the replica to a primary instance. The UDF then verifies the promotion by checking `pg_is_in_recovery()` on the replica. This step has a short timeout for verification.
    4.  **Metadata Update:** After successful PostgreSQL promotion, Citus updates the replica's metadata in `pg_catalog.pg_dist_node`: the node is marked as active (`isActive=true`), no longer a replica (`nodeisreplica=false`, `nodeprimarynodeid=0`), is marked as ready to receive shards (`shouldHaveShards=true`), and is assigned a new, unique `groupid` (obtained via `citus_internal_get_next_group_id()`).
*   **Important Notes:**
    *   This is a long-running UDF due to the waiting periods. It's recommended to run it in a session with an appropriate `statement_timeout`.
    *   If any step (WAL sync, `pg_promote` execution, promotion verification) times out or fails, the UDF will error out.
    *   After this UDF successfully completes, the replica is an active Citus primary worker, but shard placements have **not** yet been rebalanced. The `citus_finalize_replica_rebalance_metadata` function must be called next.
    *   The user must ensure that the PostgreSQL user that Citus uses to connect to the replica has permissions to execute `pg_promote()`.
*   **Example:**
    ```sql
    -- Assuming replica node with nodeid 3 was added via citus_add_replica_node
    SELECT citus_promote_replica_and_rebalance(3);
    -- On success, node 3 is now an active primary in a new group.
    -- Writes on the original primary are unblocked upon this UDF's transaction completion.
    ```

### `citus_finalize_replica_rebalance_metadata`

*   **Syntax:**
    ```sql
    citus_finalize_replica_rebalance_metadata(
        original_primary_group_id INT,
        new_primary_group_id INT,
        original_primary_node_id INT,
        new_primary_node_id INT
    )
    RETURNS VOID
    ```
*   **Description:**
    Performs the metadata adjustments for shard placements after a replica has been promoted to a new primary worker by `citus_promote_replica_and_rebalance`. It distributes shard mastership (by count) between the original primary and the new primary for each colocation group.
*   **Arguments:**
    *   `original_primary_group_id`: The `groupid` of the worker node that was the original primary for the replica (before promotion).
    *   `new_primary_group_id`: The new `groupid` assigned to the promoted replica by `citus_promote_replica_and_rebalance`.
    *   `original_primary_node_id`: The `nodeid` of the original primary worker.
    *   `new_primary_node_id`: The `nodeid` of the promoted replica (now the new primary).
*   **Details:**
    *   This function should be called **after** `citus_promote_replica_and_rebalance` has successfully run and the new primary node is active with its new group ID.
    *   It calls an internal PL/pgSQL helper (`citus_internal_rebalance_placements_for_promoted_node`) which iterates through all colocation groups that had placements on the `original_primary_group_id`.
    *   For each colocation group, it assigns approximately half the shards (by count, alternating) to remain with the `original_primary_group_id` and the other half to be mastered by the `new_primary_group_id`.
    *   It updates `pg_catalog.pg_dist_placement` to reflect these new group assignments for shards.
    *   It schedules the cleanup of physical shard data from the node that is no longer the master for a given shard by inserting records into `pg_catalog.pg_dist_cleanup`. For example, for a shard moved to the new primary, its data on the old primary is scheduled for cleanup. For a shard that remains on the old primary, its (replicated) data on the new primary is scheduled for cleanup.
*   **Example:**
    ```sql
    -- After citus_promote_replica_and_rebalance(3) has successfully run:
    -- Assume original primary was nodeid 2 in groupid 1.
    -- Assume promoted replica (nodeid 3) was assigned new groupid 4.
    SELECT pg_catalog.citus_finalize_replica_rebalance_metadata(
        original_primary_group_id := 1,
        new_primary_group_id := 4,
        original_primary_node_id := 2,
        new_primary_node_id := 3
    );
    ```

## Example Workflow Summary

1.  **DBA:** Set up a new PostgreSQL instance (`replica-new.example.com:5432`) as a streaming replica of an existing Citus worker (`worker-old.example.com:5432`). Ensure `primary_conninfo` on the replica includes an `application_name` (e.g., `replica-new.example.com`).
2.  **DBA (on Citus coordinator):** Register the replica with Citus:
    ```sql
    SELECT citus_add_replica_node('replica-new.example.com', 5432, 'worker-old.example.com', 5432) AS replica_nodeid;
    -- Let's say this returns replica_nodeid = 5.
    ```
3.  **DBA (on Citus coordinator):** Promote the replica and update its Citus node metadata:
    ```sql
    SELECT citus_promote_replica_and_rebalance(5);
    -- This function will block writes, wait for WAL sync, execute pg_promote() on replica 5,
    -- verify promotion, then update pg_dist_node for node 5 (isactive=true, new groupid, etc.).
    -- Note the NOTICE messages for progress.
    ```
4.  **DBA (on Citus coordinator):** After the previous command succeeds, gather necessary IDs. You'll need the original primary's group ID and node ID, and the newly promoted node's new group ID and its node ID.
    ```sql
    -- Example: Find original primary (worker-old) info
    SELECT nodeid AS original_node_id, groupid AS original_group_id
    FROM pg_catalog.pg_dist_node
    WHERE nodename = 'worker-old.example.com' AND nodeport = 5432;
    -- Let's say this returns original_node_id=2, original_group_id=1.

    -- Example: Find promoted replica (replica-new, now a primary) info
    SELECT nodeid AS promoted_node_id, groupid AS new_group_id
    FROM pg_catalog.pg_dist_node
    WHERE nodeid = 5;
    -- Let's say this returns promoted_node_id=5, new_group_id=4 (assigned by previous UDF).
    ```
5.  **DBA (on Citus coordinator):** Finalize rebalancing by updating shard placement metadata:
    ```sql
    SELECT citus_finalize_replica_rebalance_metadata(
        original_primary_group_id := 1,
        new_primary_group_id := 4,
        original_primary_node_id := 2,
        new_primary_node_id := 5
    );
    -- This updates pg_dist_placement and schedules cleanups.
    ```
6.  **Result:** `replica-new.example.com` is now an active Citus worker, sharing load approximately 50/50 (by shard count per colocation group) with `worker-old.example.com`. Data cleanup tasks are scheduled.

## Considerations and Limitations

*   **Two-Stage Process:** The promotion and rebalancing is now a two-stage process requiring calls to two UDFs: `citus_promote_replica_and_rebalance` followed by `citus_finalize_replica_rebalance_metadata`. Ensure both are run in the correct order and with correct parameters derived from `pg_dist_node` after the first UDF completes.
*   **Replica Identification for Verification & WAL Sync:** The reliability of detecting the replica in `pg_stat_replication` (for verification in `citus_add_replica_node` and WAL lag checking within `citus_promote_replica_and_rebalance`) is highest when `application_name` is consistently set in the replica's configuration.
*   **Permissions for `pg_promote`:** The database user that Citus uses to connect to the replica node (during `citus_promote_replica_and_rebalance`) must have permissions to execute `pg_promote()`. This typically means superuser.
*   **Atomicity and Failure Recovery:**
    *   Operations on the Citus coordinator are generally transactional.
    *   The `pg_promote()` call is external to the coordinator's transaction. If `citus_promote_replica_and_rebalance` fails after `pg_promote()` has been executed but before Citus metadata is updated, the replica will be a standalone PostgreSQL primary but not yet a fully integrated Citus primary worker. Manual metadata adjustments or re-running parts of the process might be needed.
    *   `citus_finalize_replica_rebalance_metadata` performs metadata changes. If it fails, placements might be in an intermediate state.
*   **Shard Rebalancing Strategy:** The current rebalancing strategy in `citus_finalize_replica_rebalance_metadata` distributes shards by count (alternating) within each colocation group. It does not consider shard size or load. For more sophisticated rebalancing, consider using `rebalance_table_shards()` or other manual shard movement commands after the new node is fully integrated.
*   **Network and Node Availability:** The process relies on network connectivity between the coordinator and both the original primary and replica nodes at various stages.
*   **Monitoring:** It is crucial to monitor PostgreSQL logs on all involved nodes (coordinator, original primary, replica) and Citus coordinator logs during these operations.
*   **`pg_dist_cleanup` Table:** The rebalancing process schedules cleanup tasks by inserting into `pg_catalog.pg_dist_cleanup`. Ensure that the Citus maintenance daemon or a similar mechanism is active to process these cleanup records.
*   **Group ID Assignment:** The `citus_promote_replica_and_rebalance` UDF assigns a new, unique group ID to the promoted replica using an internal mechanism based on the `pg_dist_groupid_seq` sequence.
