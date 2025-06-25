#include "postgres.h"
#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"

#include "distributed/remote_commands.h"
#include "distributed/metadata_cache.h"


static int64 GetReplicationLag(WorkerNode *primaryWorkerNode, WorkerNode *replicaWorkerNode);
static void BlockAllWritesToWorkerNode(WorkerNode *workerNode);
static bool GetNodeIsInRecoveryStatus(WorkerNode *workerNode);
static void PromoteReplicaNode(WorkerNode *replicaWorkerNode);


PG_FUNCTION_INFO_V1(citus_promote_replica_and_rebalance);

Datum
citus_promote_replica_and_rebalance(PG_FUNCTION_ARGS)
{
	// Ensure superuser and coordinator
	EnsureSuperUser();
	EnsureCoordinator();

	// Get replica_nodeid argument
	int32 replicaNodeIdArg = PG_GETARG_INT32(0);

	WorkerNode *replicaNode = NULL;
	WorkerNode *primaryNode = NULL;

	// Lock pg_dist_node to prevent concurrent modifications during this operation
	LockRelationOid(DistNodeRelationId(), RowExclusiveLock);

	replicaNode = FindNodeAnyClusterByNodeId(replicaNodeIdArg);
	if (replicaNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica node with ID %d not found.", replicaNodeIdArg)));
	}

	if (!replicaNode->nodeisreplica || replicaNode->nodeprimarynodeid == 0) // Assuming 0 is InvalidNodeId for nodeprimarynodeid
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Node %s:%d (ID %d) is not a valid replica or its primary node ID is not set.",
							   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	}

	if (replicaNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica node %s:%d (ID %d) is already active and cannot be promoted.",
							   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	}

	primaryNode = FindNodeAnyClusterByNodeId(replicaNode->nodeprimarynodeid);
	if (primaryNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node with ID %d (for replica %s:%d) not found.",
							   replicaNode->nodeprimarynodeid, replicaNode->workerName, replicaNode->workerPort)));
	}

	if (primaryNode->nodeisreplica)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is itself a replica.",
							   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));
	}

	if (!primaryNode->isActive)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Primary node %s:%d (ID %d) is not active.",
							   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));
	}

	ereport(NOTICE, (errmsg("Starting promotion process for replica node %s:%d (ID %d), original primary %s:%d (ID %d)",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId,
						   primaryNode->workerName, primaryNode->workerPort, primaryNode->nodeId)));

	// Step 1: Block Writes on Original Primary's Shards
	ereport(NOTICE, (errmsg("Step 1: Blocking writes on shards of original primary node %s:%d (group %d)",
						   primaryNode->workerName, primaryNode->workerPort, primaryNode->groupId)));

	BlockAllWritesToWorkerNode(primaryNode);

	// Step 2: Wait for Replica to Catch Up
	ereport(NOTICE, (errmsg("Step 2: Waiting for replica %s:%d to catch up with primary %s:%d",
						   replicaNode->workerName, replicaNode->workerPort,
						   primaryNode->workerName, primaryNode->workerPort)));

	bool caughtUp = false;
	const int catchUpTimeoutSeconds = 300; // 5 minutes, TODO: Make GUC
	const int sleepIntervalSeconds = 5;
	int elapsedTimeSeconds = 0;

	while (elapsedTimeSeconds < catchUpTimeoutSeconds)
	{
		uint64 repLag = GetReplicationLag(primaryNode, replicaNode);
		if (repLag <= 0)
		{
			caughtUp = true;
			break;
		}
		pg_usleep(sleepIntervalSeconds * 1000000L);
		elapsedTimeSeconds += sleepIntervalSeconds;
	}

	if (!caughtUp)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica %s:%d failed to catch up with primary %s:%d within %d seconds.",
							   replicaNode->workerName, replicaNode->workerPort,
							   primaryNode->workerName, primaryNode->workerPort,
							   catchUpTimeoutSeconds)));
	}

	ereport(NOTICE, (errmsg("Replica %s:%d is now caught up with primary %s:%d.",
						   replicaNode->workerName, replicaNode->workerPort,
						   primaryNode->workerName, primaryNode->workerPort)));
						


	// Step 3: Automate PostgreSQL Replica Promotion
	ereport(NOTICE, (errmsg("Step 3: Attempting to promote replica %s:%d via pg_promote().",
						   replicaNode->workerName, replicaNode->workerPort)));
	PromoteReplicaNode(replicaNode);

	// TODO: Rebalance shards here


	// Step 4: Update Replica Metadata in pg_dist_node on Coordinator
	ereport(NOTICE, (errmsg("Step 4: Updating metadata for promoted replica %s:%d (ID %d)",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));
	ActivateReplicaNodeAsPrimary(replicaNode);

	TransactionModifiedNodeMetadata = true; // Inform Citus about metadata change
	TriggerNodeMetadataSyncOnCommit();      // Ensure changes are propagated



	ereport(NOTICE, (errmsg("Replica node %s:%d (ID %d) metadata updated. It is now a primary",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));

	#ifdef NO_USE

	// TODO: Step 5: Rebalance Shards
	ereport(NOTICE, (errmsg("Step 5: Rebalancing shards from %s:%d (group %d) to %s:%d (new group %d)",
						   primaryNode->workerName, primaryNode->workerPort, primaryNode->groupId,
						   replicaNode->workerName, replicaNode->workerPort, newGroupId)));

	List *placementsOnOldPrimaryGroup = AllShardPlacementsOnNodeGroup(primaryNode->groupId);
	HTAB *shardsByColocationId = NULL;
	HASHCTL ctl;
	MemoryContext oldContext;
	MemoryContext rebalanceContext;

	if (list_length(placementsOnOldPrimaryGroup) > 0)
	{
		// Create a memory context for this step
		rebalanceContext = AllocSetContextCreate(CurrentMemoryContext,
												 "RebalanceContext",
												 ALLOCSET_DEFAULT_SIZES);
		oldContext = MemoryContextSwitchTo(rebalanceContext);

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(List *); // Store a List* of ShardInterval*
		ctl.hcxt = rebalanceContext;
		shardsByColocationId = hash_create("Shards By ColocationId",
										   list_length(placementsOnOldPrimaryGroup), // Estimate
										   &ctl,
										   HASH_ELEM | HASH_KEY | HASH_CONTEXT);

		ListCell *plCell = NULL;
		foreach(plCell, placementsOnOldPrimaryGroup)
		{
			GroupShardPlacement *placement = (GroupShardPlacement *) lfirst(plCell);
			uint64 shardId = placement->shardId;
			uint32 colocationId = GetColocationIdForShard(shardId);
			bool found = false;
			List **shardListPtr = (List **) hash_search(shardsByColocationId,
														&colocationId,
														HASH_ENTER, &found);
			if (!found)
			{
				*shardListPtr = NIL;
			}
			// We need ShardInterval, not GroupShardPlacement for TransferShards (or just shardId)
			// For sorting and selecting half, having ShardIntervals might be useful.
			// For now, let's just use shardId. TransferShards will resolve it.
			*shardListPtr = lappend_int64(*shardListPtr, shardId);
		}

		MemoryContextSwitchTo(oldContext); // Switch back before iterating and calling TransferShards

		HASH_SEQ_STATUS status;
		List *shardIdList = NIL;
		uint32 *currentColocationIdPtr = NULL;

		hash_seq_init(&status, shardsByColocationId);

		while ((shardIdList = *(List **) hash_seq_search(&status)) != NULL)
		{
			currentColocationIdPtr = (uint32 *) hash_seq_get_key(&status);
			uint32 currentColocationId = *currentColocationIdPtr;

			// Sort the shardIdList to make the selection deterministic
			// list_sort might not work directly on int64 list, needs custom sort or convert to List of Datums/Structs
			// For simplicity, we'll proceed without sorting for now, or assume TransferShards can take any.
			// A proper implementation would sort this list.
			// list_sort(shardIdList, compare_int64_list_nodes); // Placeholder for sort

			int numToMove = list_length(shardIdList) / 2;
			int movedCount = 0;
			ListCell *sCell = NULL;

			ereport(NOTICE, (errmsg("For colocation group %u, found %d shards on old primary. Attempting to move %d.",
								   currentColocationId, list_length(shardIdList), numToMove)));

			foreach(sCell, shardIdList)
			{
				if (movedCount >= numToMove)
				{
					break;
				}

				uint64 shardToMove = (uint64) lfirst_int(sCell); // Assuming direct int64 storage

				ereport(NOTICE, (errmsg("Moving shard %lld (colocation %u) from %s:%d to %s:%d",
									   shardToMove, currentColocationId,
									   primaryNode->workerName, primaryNode->workerPort,
									   replicaNode->workerName, replicaNode->workerPort)));

				TransferShards(shardToMove,
							   primaryNode->workerName, primaryNode->workerPort,
							   replicaNode->workerName, replicaNode->workerPort,
							   TRANSFER_MODE_BLOCK_WRITES, // Writes already blocked broadly
							   SHARD_TRANSFER_MOVE);
				movedCount++;
			}
		}
		MemoryContextDelete(rebalanceContext); // Clean up
	}
	else
	{
		ereport(NOTICE, (errmsg("No shard placements found on the original primary group %d to rebalance.", primaryNode->groupId)));
	}
	list_free_deep(placementsOnOldPrimaryGroup);


	// TODO: Step 6: Unblock Writes (often handled by transaction commit)
	ereport(NOTICE, (errmsg("TODO: Step 6: Unblock Writes")));


	ereport(NOTICE, (errmsg("citus_promote_replica_and_rebalance for node ID %d - IMPLEMENTATION IN PROGRESS", replicaNodeIdArg)));
#endif // NO_USE
	PG_RETURN_VOID();
}



static int64
GetReplicationLag(WorkerNode *primaryWorkerNode, WorkerNode *replicaWorkerNode)
{

#if PG_VERSION_NUM >= 100000
    const char *primary_lsn_query = "SELECT pg_current_wal_lsn()";
    const char *replica_lsn_query = "SELECT pg_last_wal_replay_lsn()";
#else
    const char *primary_lsn_query = "SELECT pg_current_xlog_location()";
    const char *replica_lsn_query = "SELECT pg_last_xlog_replay_location()";
#endif

	int connectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(connectionFlag,
													primaryWorkerNode->workerName,
													primaryWorkerNode->workerPort);
	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to fetch replication status",
							   primaryWorkerNode->workerName, primaryWorkerNode->workerPort)));
	}
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag,
													replicaWorkerNode->workerName,
													replicaWorkerNode->workerPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to fetch replication status",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort)));
	}

	int primaryResultCode = SendRemoteCommand(primaryConnection, primary_lsn_query);
	if (primaryResultCode == 0)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *primaryResult = GetRemoteCommandResult(primaryConnection, true);
	if (!IsResponseOK(primaryResult))
	{
		ReportResultError(primaryConnection, primaryResult, ERROR);
	}

	int replicaResultCode = SendRemoteCommand(replicaConnection, replica_lsn_query);
	if (replicaResultCode == 0)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}
	PGresult *replicaResult = GetRemoteCommandResult(replicaConnection, true);
	if (!IsResponseOK(replicaResult))
	{
		ReportResultError(replicaConnection, replicaResult, ERROR);
	}


	List *primaryLsnList = ReadFirstColumnAsText(primaryResult);
	if (list_length(primaryLsnList) != 1)
	{
		PQclear(primaryResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse get primary LSN result from %s:%d",
								  primaryWorkerNode->workerName,
								  primaryWorkerNode->workerPort)));

	}
	StringInfo primaryLsnQueryResInfo = (StringInfo) linitial(primaryLsnList);
	char *primary_lsn_str = primaryLsnQueryResInfo->data;

	List *replicaLsnList = ReadFirstColumnAsText(replicaResult);
	if (list_length(replicaLsnList) != 1)
	{
		PQclear(replicaResult);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse get replica LSN result from %s:%d",
								  replicaWorkerNode->workerName,
								  replicaWorkerNode->workerPort)));

	}
	StringInfo replicaLsnQueryResInfo = (StringInfo) linitial(replicaLsnList);
	char *replica_lsn_str = replicaLsnQueryResInfo->data;

    if (!primary_lsn_str || !replica_lsn_str)
        return -1;

    /* Use PostgreSQL function pg_wal_lsn_diff as C code (returns double, but it's always integer in bytes) */
    int64 primary_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(primary_lsn_str)));
    int64 replica_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(replica_lsn_str)));

	int64 lag_bytes = primary_lsn - replica_lsn;

	PQclear(primaryResult);
	ForgetResults(primaryConnection);
	CloseConnection(primaryConnection);

	PQclear(replicaResult);
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);

	ereport(NOTICE, (errmsg("replication lag between %s:%d and %s:%d is %ld bytes",
							primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
							replicaWorkerNode->workerName, replicaWorkerNode->workerPort,
							lag_bytes)));
    return lag_bytes;
}

static void
PromoteReplicaNode(WorkerNode *replicaWorkerNode)
{
	int connectionFlag = 0;
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag,
													replicaWorkerNode->workerName,
													replicaWorkerNode->workerPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to promote replica",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort)));
	}

	const char *promoteQuery = "SELECT pg_promote(wait := true);";
	int resultCode = SendRemoteCommand(replicaConnection, promoteQuery);
	if (resultCode == 0)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);
	/* connect again and verify the replica is promoted */
	if ( GetNodeIsInRecoveryStatus(replicaWorkerNode) )
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Failed to promote replica %s:%d (ID %d). It is still in recovery.",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort, replicaWorkerNode->nodeId)));
	}
	else
	{
		ereport(NOTICE, (errmsg("Replica node %s:%d (ID %d) has been successfully promoted.",
							   replicaWorkerNode->workerName, replicaWorkerNode->workerPort, replicaWorkerNode->nodeId)));
	}
}

static void
BlockAllWritesToWorkerNode(WorkerNode *workerNode)
{
	ereport(NOTICE, (errmsg("Blocking all writes to worker node %s:%d (ID %d)",
						   workerNode->workerName, workerNode->workerPort, workerNode->nodeId)));
	// List *placementsOnOldPrimaryGroup = AllShardPlacementsOnNodeGroup(workerNode->groupId);

	LockShardsInWorkerPlacementList(workerNode, AccessExclusiveLock);
}

bool
GetNodeIsInRecoveryStatus(WorkerNode *workerNode)
{
	int connectionFlag = 0;
	MultiConnection *nodeConnection = GetNodeConnection(connectionFlag,
													workerNode->workerName,
													workerNode->workerPort);

	if (PQstatus(nodeConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errmsg("cannot connect to %s:%d to check recovery status",
							   workerNode->workerName, workerNode->workerPort)));
	}

	const char *recoveryQuery = "SELECT pg_is_in_recovery();";
	int resultCode = SendRemoteCommand(nodeConnection, recoveryQuery);
	if (resultCode == 0)
	{
		ReportConnectionError(nodeConnection, ERROR);
	}

	PGresult *result = GetRemoteCommandResult(nodeConnection, true);
	if (!IsResponseOK(result))
	{
		ReportResultError(nodeConnection, result, ERROR);
	}

	List *recoveryStatusList = ReadFirstColumnAsText(result);
	if (list_length(recoveryStatusList) != 1)
	{
		PQclear(result);
		ClearResults(nodeConnection, true);
		CloseConnection(nodeConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse recovery status result from %s:%d",
								  workerNode->workerName,
								  workerNode->workerPort)));
	}

	StringInfo recoveryStatusInfo = (StringInfo) linitial(recoveryStatusList);
	bool isInRecovery = (strcmp(recoveryStatusInfo->data, "t") == 0) || (strcmp(recoveryStatusInfo->data, "true") == 0);

	PQclear(result);
	ForgetResults(nodeConnection);
	CloseConnection(nodeConnection);

	return isInRecovery;
}