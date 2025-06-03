# Adding and Promoting Worker Nodes via Streaming Replication

## Overview

This feature allows you to expand your Citus cluster by adding a new worker node that starts its life as a PostgreSQL streaming replica of an existing Citus worker node. Once the replica is synchronized and promoted to a standalone primary, Citus can integrate it into the cluster and rebalance shard placements to it from its original primary.

This process is useful for scenarios where you want to offload the initial data copying from the primary worker by leveraging PostgreSQL's built-in streaming replication.

The high-level workflow involves these stages:

1.  **Manual PostgreSQL Replica Setup:** The database administrator manually sets up a new PostgreSQL instance as a streaming replica of an active Citus worker node. This is done using standard PostgreSQL replication procedures.
2.  **Register Replica with Citus:** The administrator then registers this PostgreSQL replica with the Citus coordinator using the `citus_add_replica_node` UDF. This makes Citus aware of the replica and its relationship to the primary worker.
3.  **Initiate Promotion and Rebalance:** When ready, the administrator calls the `citus_promote_replica_and_rebalance` UDF.
4.  **Automated Orchestration by Citus:**
    *   Citus temporarily blocks writes to the shards on the original primary node.
    *   It monitors WAL (Write-Ahead Log) synchronization to ensure the replica is fully caught up with its primary.
    *   Once synchronized, Citus prompts the administrator to perform the actual PostgreSQL promotion of the replica instance (e.g., using `pg_promote()`).
    *   Citus waits for the replica's promotion to complete by monitoring its recovery state.
    *   After the replica is promoted, Citus updates its metadata to reflect the new node as an active, primary worker with a new unique group ID.
    *   Finally, Citus automatically rebalances approximately half of the shard placements from the original primary worker node to the newly promoted worker. Writes are then unblocked.

## Prerequisites

*   A healthy, running Citus cluster.
*   Successful configuration of PostgreSQL streaming replication from an existing, active Citus worker node (the "primary") to a new PostgreSQL instance (the "replica").
    *   Refer to the official PostgreSQL documentation for setting up streaming replication.
*   The replica PostgreSQL instance must **not** yet be promoted to a primary when calling `citus_add_replica_node`. It should be a standby server, actively replicating.
*   **Highly Recommended:** For reliable WAL lag monitoring by `citus_promote_replica_and_rebalance`, configure a unique `application_name` in the replica's `primary_conninfo` setting (in `postgresql.conf` or `recovery.conf` / `standby.signal` depending on your PostgreSQL version). It's good practice for this `application_name` to match the replica's hostname or be otherwise easily identifiable.

## New User-Defined Functions (UDFs)

Two new UDFs are introduced to manage this process:

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
    Registers a PostgreSQL streaming replica as a potential Citus worker node within the Citus metadata. This function links the replica to its primary Citus worker and records its connection information.
*   **Arguments:**
    *   `replica_hostname`: The hostname or IP address of the streaming replica instance.
    *   `replica_port`: The port number of the streaming replica instance.
    *   `primary_hostname`: The hostname or IP address of the existing Citus worker node from which the replica is streaming.
    *   `primary_port`: The port number of the existing Citus worker node.
*   **Return Value:**
    The `nodeid` of the newly registered replica node in `pg_catalog.pg_dist_node`.
*   **Details:**
    *   The replica is added to the Citus metadata as an inactive node (`isActive=false` and `shouldHaveShards=false` by default).
    *   It is explicitly marked as a replica (`nodeisreplica=true`) and associated with its primary via `nodeprimarynodeid`.
    *   This function must be called **before** the replica is promoted to a primary at the PostgreSQL level.
    *   The replica will typically share the same `groupid`, `noderole`, and `nodecluster` as its primary upon registration.
*   **Example:**
    ```sql
    -- Assuming 'worker1.example.com:5432' is the primary Citus worker
    -- and 'replica1.example.com:5432' is its streaming replica.
    SELECT citus_add_replica_node('replica1.example.com', 5432, 'worker1.example.com', 5432);
    -- Returns the new nodeid for replica1.example.com
    ```

### `citus_promote_replica_and_rebalance`

*   **Syntax:**
    ```sql
    citus_promote_replica_and_rebalance(replica_nodeid INT)
    RETURNS VOID
    ```
*   **Description:**
    Orchestrates the promotion of a previously registered replica to become an active Citus worker. This includes waiting for WAL synchronization, prompting for PostgreSQL-level promotion, updating Citus metadata, and rebalancing a portion of shards from the original primary to the newly promoted node.
*   **Arguments:**
    *   `replica_nodeid`: The `nodeid` (from `pg_catalog.pg_dist_node`) of the replica that was registered using `citus_add_replica_node`.
*   **Workflow Stages Orchestrated by the UDF:**
    1.  **Block Writes:** Temporarily blocks writes to shards located on the original primary node (the one the replica was replicating from) to ensure data consistency during the final sync and switchover.
    2.  **WAL Sync Wait:** Connects to the original primary node and monitors `pg_stat_replication` to ensure the specified replica has replayed all WAL records and is fully caught up. The UDF attempts to identify the replica using its `application_name` (ideally matching the replica's hostname, as registered) or its client address. This stage has a configurable timeout (default: 5 minutes).
    3.  **User Promotion Prompt & Wait:** Once the replica is caught up, the UDF issues a `NOTICE` message, prompting the database administrator to promote the PostgreSQL replica instance to a primary server (e.g., by running `pg_promote()` on the replica or using other promotion methods). The UDF then pauses and waits, periodically attempting to connect to the replica and checking if `pg_is_in_recovery()` returns `false`. This stage also has a configurable timeout (default: 5 minutes).
    4.  **Metadata Update:** Upon successful detection of the replica's promotion (i.e., it's no longer in recovery), Citus updates its internal metadata. The replica node is marked as active (`isActive=true`), is no longer considered a replica (`nodeisreplica=false`, `nodeprimarynodeid=0`), is marked as ready to receive shards (`shouldHaveShards=true`), and is assigned a new, unique `groupid`.
    5.  **Shard Rebalance:** Citus then rebalances a portion of the shard placements. For each colocation group that had shards on the original primary, approximately half of these shards are moved to the newly promoted worker.
    6.  **Unblock Writes:** Write locks on the original primary's shards are released (typically at the end of the UDF's transaction).
*   **Important Notes:**
    *   This is a long-running UDF due to the built-in waiting periods for WAL synchronization and replica promotion. It's recommended to run it in a session with an appropriate `statement_timeout`.
    *   The actual promotion of the PostgreSQL replica (e.g., using `pg_promote()`) is an **external action** that the user must perform when prompted by the UDF. The UDF orchestrates around this external step but does not perform the PostgreSQL-level promotion itself.
    *   If any step (WAL sync wait, promotion wait) times out, or if other errors occur (e.g., nodes becoming unavailable), the UDF will error out.
    *   Due to the multi-stage nature of the process and its reliance on external actions and distributed state, the entire operation is not fully atomic in the traditional sense across the whole system. Metadata changes on the coordinator are transactional. If the UDF errors out after the user has promoted the PostgreSQL replica but before Citus metadata is updated, manual steps might be needed to register the new primary or clean up.
*   **Example:**
    ```sql
    -- Assuming replica node with nodeid 3 was added via citus_add_replica_node
    SELECT citus_promote_replica_and_rebalance(3);
    ```

## Example Workflow Summary

1.  **DBA:** Set up a new PostgreSQL instance (`replica-new.example.com:5432`) as a streaming replica of an existing Citus worker (`worker-old.example.com:5432`). Ensure `primary_conninfo` on the replica includes an `application_name` (e.g., `replica-new.example.com`).
2.  **DBA (on Citus coordinator):** Register the replica with Citus:
    ```sql
    SELECT citus_add_replica_node('replica-new.example.com', 5432, 'worker-old.example.com', 5432);
    -- Note the returned replica_nodeid, let's say it's 5.
    ```
3.  **DBA (on Citus coordinator):** Initiate the promotion and rebalance process:
    ```sql
    SELECT citus_promote_replica_and_rebalance(5);
    ```
4.  **UDF:** Blocks writes on `worker-old`.
5.  **UDF:** Waits for `replica-new` to catch up with WAL from `worker-old`.
6.  **UDF:** Displays `NOTICE: Replica replica-new.example.com:5432 is caught up. Please promote it to a primary PostgreSQL instance NOW. ...`
7.  **DBA:** Connects to `replica-new.example.com` and promotes it (e.g., `pg_ctl promote` or `pg_promote()`).
8.  **UDF:** Detects that `replica-new` is no longer in recovery.
9.  **UDF:** Updates Citus metadata: `replica-new` becomes an active primary in a new group.
10. **UDF:** Moves approximately half the shards from `worker-old`'s group to `replica-new`'s new group.
11. **UDF:** Completes, writes are unblocked.
12. **Result:** `replica-new.example.com` is now an active Citus worker, sharing load with `worker-old.example.com`.

## Considerations and Limitations

*   **Concurrency for Group ID:** The mechanism for assigning a new `groupid` to the promoted replica should ensure uniqueness even with concurrent operations. The current implementation uses a temporary, non-concurrency-safe method (`max(groupid) + 1`) as a placeholder due to internal access constraints for the preferred sequence-based `GetNextGroupId()` function. This aspect is marked for improvement.
*   **Replica Identification:** The reliability of detecting the replica in `pg_stat_replication` (for WAL lag checking) is highest when `application_name` is consistently set in the replica's configuration and matches what the UDF expects (e.g., the replica's hostname).
*   **Atomicity and Failure Recovery:** While operations on the Citus coordinator are transactional, the end-to-end process involves external systems (PostgreSQL replication and promotion) and distributed operations (shard movement).
    *   If the UDF fails before the user promotes the replica, the situation is generally recoverable by addressing the issue and re-running the UDF, or by manually cleaning up the registered replica entry using `citus_remove_node()`.
    *   If the UDF fails *after* the user has promoted the replica but *before* Citus metadata is fully updated and shards are rebalanced, the newly promoted PostgreSQL instance will be a standalone primary. Manual intervention might be needed to either add it as a new standard worker node to Citus (using `citus_add_node`) or to troubleshoot the failed `citus_promote_replica_and_rebalance` call. The original primary will still have all its shards and writes would have been blocked by the UDF; these locks are released if the UDF transaction aborts.
*   **Shard Rebalancing Strategy:** The current rebalancing strategy moves approximately 50% of the shard load (per colocation group) from the original primary to the new one. This is a fixed strategy. For more fine-grained control, subsequent manual shard rebalancer commands can be used.
*   **Network and Node Availability:** The process relies on network connectivity between the coordinator and both the primary and replica nodes at various stages. Ensure nodes are accessible and PostgreSQL instances are running.
*   **Monitoring:** It is crucial to monitor the PostgreSQL logs on both the original primary and the replica, as well as the Citus coordinator logs, during this operation.
