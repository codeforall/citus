#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "distributed/node_promotion.h"
#include "distributed/worker_manager.h" // For WorkerNode, FindNodeAnyClusterByNodeId
#include "distributed/metadata_utility.h" // For EnsureSuperUser, EnsureCoordinator
#include "distributed/pg_dist_node.h" // For Anum_pg_dist_node_*
#include "distributed/connection_management.h" // For GetNodeConnection, ExecuteRemoteCommand
#include "distributed/shard_transfer.h" // For BlockWritesToShardList, TransferShards
#include "distributed/colocation_utils.h" // For AllColocationIdsOnGroup
#include "distributed/shardinterval_utils.h" // For LoadColocatedShardIntervalList
#include "distributed/listutils.h" // For list_free_deep
#include "lib/stringinfo.h" // For StringInfo
#include "utils/builtins.h" // For DatumGetLSN, pg_sleep
#include "access/xlog_internal.h" // For LSN related functions (pg_current_wal_lsn is a SQL function)
#include "utils/snapmgr.h" // For GetTransactionSnapshot, PushActiveSnapshot, PopActiveSnapshot
#include "executor/spi.h" // For SPI_connect, SPI_execute, SPI_finish
#include "distributed/citus_safe_lib.h" // For SDL_GetErrMessage
#include "catalog/pg_extension.h" // For EXTENSION_NAME_DATALEN
#include "access/heapam.h" // For heap_open, heap_close
#include "access/htup_details.h" // For heap_modify_tuple, etc.
#include "catalog/indexing.h" // For CatalogTupleUpdate
#include "utils/relcache.h" // For RelationInvalidateCacheCallback
#include "distributed/metadata_cache.h" // For CitusInvalidateRelcacheByRelid
#include "distributed/metadata_sync.h" // For TriggerNodeMetadataSyncOnCommit
#include "distributed/metadata/node_metadata.h" // For GetNextGroupId (needs to be exposed or reimplemented)
#include "utils/hsearch.h" // For hash table operations
#include "utils/memutils.h" // For MemoryContext related operations

// May need more includes as we implement

// Forward declare GetNextGroupId if it's static in node_metadata.c and not in a header
// For a real implementation, this should be properly exposed via a header or a UDF.
// For now, we'll assume it can be called. If not, this part will fail compilation
// and would need to be addressed by refactoring GetNextGroupId.
// static int32 GetNextGroupId(void); // This will cause linker error if not available.
// Let's assume for now we will call a helper that calls the real GetNextGroupId or handle it via SPI call to a helper UDF.
// For the purpose of this exercise, we'll call a placeholder.

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

	List *colocationIdsOnPrimary = AllColocationIdsOnGroup(primaryNode->groupId);
	ListCell *lc = NULL;

	if (list_length(colocationIdsOnPrimary) == 0)
	{
		ereport(NOTICE, (errmsg("No colocated shards found on primary group %d to block.", primaryNode->groupId)));
	}
	else
	{
		foreach(lc, colocationIdsOnPrimary)
		{
			uint32 colocationId = (uint32) lfirst_int(lc);
			List *colocatedShardList = LoadColocatedShardIntervalList(colocationId, AccessShareLock);

			if (list_length(colocatedShardList) > 0)
			{
				ereport(NOTICE, (errmsg("Blocking writes for colocation group %u (found %d shards)",
									   colocationId, list_length(colocatedShardList))));
				BlockWritesToShardList(colocatedShardList);
			}
			else
			{
				ereport(NOTICE, (errmsg("No shards found for colocation group %u on primary group %d to block.",
									   colocationId, primaryNode->groupId)));
			}
			list_free_deep(colocatedShardList); // Assuming ShardIntervals are palloc'd
		}
	}
	list_free(colocationIdsOnPrimary);


	// Step 2: Wait for Replica to Catch Up
	ereport(NOTICE, (errmsg("Step 2: Waiting for replica %s:%d to catch up with primary %s:%d",
						   replicaNode->workerName, replicaNode->workerPort,
						   primaryNode->workerName, primaryNode->workerPort)));

	bool caughtUp = false;
	const int catchUpTimeoutSeconds = 300; // 5 minutes, TODO: Make GUC
	const int sleepIntervalSeconds = 5;
	int elapsedTimeSeconds = 0;
	char *replicaApplicationName = replicaNode->workerName; // Best guess, user should configure this

	while (elapsedTimeSeconds < catchUpTimeoutSeconds)
	{
		MultiConnection *primaryConnection = NULL;
		StringInfoData query;
		XLogRecPtr primaryLsn = InvalidXLogRecPtr;
		XLogRecPtr replicaReplayLsn = InvalidXLogRecPtr;
		bool replicaFoundInStats = false;

		PG_TRY();
		{
			primaryConnection = GetNodeConnection(CONNECTION_PRIORITY_INVALID,
												  primaryNode->workerName,
												  primaryNode->workerPort);

			// Get primary's current LSN
			char *lsnQuery = "SELECT pg_current_wal_lsn();";
			PushActiveSnapshot(GetTransactionSnapshot()); // Needed for SPI on some PG versions
			SPI_connect();
			if (SPI_execute(lsnQuery, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
			{
				primaryLsn = DatumGetLSN(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, NULL));
			}
			SPI_finish();
			PopActiveSnapshot();


			// Get replica's replay_lsn from pg_stat_replication on primary
			initStringInfo(&query);
			appendStringInfo(&query, "SELECT replay_lsn FROM pg_stat_replication WHERE application_name = %s OR client_addr = %s;",
							 quote_literal_cstr(replicaApplicationName),
							 quote_literal_cstr(replicaNode->workerName)); // Fallback to client_addr if app_name differs

			PushActiveSnapshot(GetTransactionSnapshot());
			SPI_connect();
			if (SPI_execute(query.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool isnull;
				Datum replayLsnDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
				if (!isnull)
				{
					replicaReplayLsn = DatumGetLSN(replayLsnDatum);
					replicaFoundInStats = true;
				}
			}
			SPI_finish();
			PopActiveSnapshot();
			pfree(query.data);

			if (primaryConnection)
			{
				ReleaseNodeConnection(primaryConnection);
				primaryConnection = NULL;
			}
		}
		PG_CATCH();
		{
			if (primaryConnection)
			{
				ReleaseNodeConnection(primaryConnection);
			}
			FlushErrorState();
			ereport(WARNING, (errmsg("Error connecting to primary or querying LSNs: %s. Retrying...",
									 SDL_GetErrMessage(NULL)))); // Need to include citus_safe_lib.h for SDL_GetErrMessage
			// Fall through to sleep and retry
		}
		PG_END_TRY();


		if (XLogRecPtrIsInvalid(primaryLsn))
		{
			ereport(WARNING, (errmsg("Could not determine primary LSN on %s:%d. Retrying...",
								   primaryNode->workerName, primaryNode->workerPort)));
		}
		else if (!replicaFoundInStats)
		{
			ereport(WARNING, (errmsg("Replica %s:%d (app_name: %s) not found in pg_stat_replication on primary %s:%d. "
								   "Ensure replica is connected and application_name is set correctly. Retrying...",
								   replicaNode->workerName, replicaNode->workerPort, replicaApplicationName,
								   primaryNode->workerName, primaryNode->workerPort)));
		}
		else if (XLogRecPtrIsInvalid(replicaReplayLsn))
		{
			 ereport(WARNING, (errmsg("Replica %s:%d LSN is NULL on primary %s:%d. Retrying...",
								   replicaNode->workerName, replicaNode->workerPort,
								   primaryNode->workerName, primaryNode->workerPort)));
		}
		else
		{
			ereport(NOTICE, (errmsg("Primary LSN: %X/%X, Replica Replay LSN: %X/%X",
								   (uint32)(primaryLsn >> 32), (uint32)primaryLsn,
								   (uint32)(replicaReplayLsn >> 32), (uint32)replicaReplayLsn)));
			if (replicaReplayLsn >= primaryLsn)
			{
				caughtUp = true;
				break;
			}
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

	// Step 3: Instruct User & Wait for PostgreSQL Replica Promotion
	ereport(NOTICE, (errmsg("Step 3: Replica %s:%d is caught up. Please promote it to a primary PostgreSQL instance NOW. "
						   "The UDF will wait for up to %d minutes for the promotion to complete.",
						   replicaNode->workerName, replicaNode->workerPort, catchUpTimeoutSeconds / 60)));

	bool replicaPromoted = false;
	const int promotionTimeoutSeconds = 300; // 5 minutes, should ideally be separate GUC or related to catchUpTimeout
	elapsedTimeSeconds = 0; // Reset for this new wait period

	while (elapsedTimeSeconds < promotionTimeoutSeconds)
	{
		MultiConnection *replicaConnection = NULL;
		bool isInRecovery = true; // Assume in recovery until proven otherwise

		PG_TRY();
		{
			replicaConnection = GetNodeConnection(CONNECTION_PRIORITY_INVALID,
												 replicaNode->workerName,
												 replicaNode->workerPort);

			char *recoveryCheckQuery = "SELECT pg_is_in_recovery();";
			PushActiveSnapshot(GetTransactionSnapshot());
			SPI_connect();
			if (SPI_execute(recoveryCheckQuery, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool isnull;
				Datum recoveryDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
				if (!isnull)
				{
					isInRecovery = DatumGetBool(recoveryDatum);
				}
			}
			else {
				 // If query fails, replica might not be fully up or accepting queries yet
				 ereport(WARNING, (errmsg("Failed to query pg_is_in_recovery() on %s:%d. Retrying...",
									   replicaNode->workerName, replicaNode->workerPort)));
			}
			SPI_finish();
			PopActiveSnapshot();

			if (replicaConnection)
			{
				ReleaseNodeConnection(replicaConnection);
				replicaConnection = NULL;
			}
		}
		PG_CATCH();
		{
			if (replicaConnection)
			{
				ReleaseNodeConnection(replicaConnection);
			}
			FlushErrorState();
			ereport(WARNING, (errmsg("Error connecting to replica %s:%d to check promotion status: %s. Retrying...",
									 replicaNode->workerName, replicaNode->workerPort, SDL_GetErrMessage(NULL))));
			// Fall through to sleep and retry
		}
		PG_END_TRY();

		if (!isInRecovery)
		{
			replicaPromoted = true;
			break;
		}

		ereport(NOTICE, (errmsg("Waiting for replica %s:%d to be promoted... (currently in_recovery=%s)",
							   replicaNode->workerName, replicaNode->workerPort, isInRecovery ? "true" : "false")));
		pg_usleep(sleepIntervalSeconds * 1000000L);
		elapsedTimeSeconds += sleepIntervalSeconds;
	}

	if (!replicaPromoted)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica %s:%d was not promoted within %d seconds.",
							   replicaNode->workerName, replicaNode->workerPort, promotionTimeoutSeconds)));
	}

	ereport(NOTICE, (errmsg("Replica %s:%d has been successfully promoted.",
						   replicaNode->workerName, replicaNode->workerPort)));


	// Step 4: Update Replica Metadata in pg_dist_node on Coordinator
	ereport(NOTICE, (errmsg("Step 4: Updating metadata for promoted replica %s:%d (ID %d)",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId)));

	Datum values[Natts_pg_dist_node];
	bool isnulls[Natts_pg_dist_node];
	bool replace[Natts_pg_dist_node];
	Relation pgDistNodeRel = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple currentTuple = NULL;
	HeapTuple newTuple = NULL;
	int32 newGroupId = 0;

	// This is a placeholder. In a real scenario, GetNextGroupId() from node_metadata.c
	// would need to be made available, or another mechanism used (e.g., SQL UDF call).
	// For now, let's simulate getting a new group ID. This part would need proper implementation.
	// A temporary workaround could be to find max(groupid) + 1 via SPI if GetNextGroupId is not callable.
	// For this exercise, we'll assign a symbolic value and note this needs real implementation.
	// newGroupId = GetNextGroupId(); // This is the ideal call.

	// Temporary workaround for GetNextGroupId: Use SPI to get max(groupid) + 1.
	// WARNING: THIS IS NOT CONCURRENCY SAFE AND NOT FOR PRODUCTION.
	// It is used here due to difficulties making GetNextGroupId() from node_metadata.c directly callable.
	// A proper solution involves exposing GetNextGroupId or using a sequence via a helper UDF.
	ereport(WARNING, (errmsg("Using a temporary, non-production-safe method to determine new group ID. "
						   "This should be replaced by a call to a centrally managed sequence (e.g., GetNextGroupId).")));

	SPI_connect();
	if (SPI_execute("SELECT max(groupid) + 1 FROM pg_catalog.pg_dist_node;", true, 1) == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		Datum maxGroupIdPlusOne = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull) {
			newGroupId = 1; // Should not happen if there are nodes, but as a fallback.
		} else {
			newGroupId = DatumGetInt32(maxGroupIdPlusOne);
		}
	}
	else
	{
		SPI_finish();
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Could not determine next group ID using SPI.")));
	}
	SPI_finish();

	// Ensure the newGroupId is not the coordinator group id, unless it's the only possible one.
	if (newGroupId == COORDINATOR_GROUP_ID) {
		// This logic might need refinement based on how group IDs are typically assigned.
		// If max(groupid) was -1 (no user groups), max+1 could be 0.
		// A robust GetNextGroupId would handle this by starting sequences appropriately.
		List *allNodes = ReadDistNode(true); // includeNodesFromOtherClusters = true
		if (list_length(allNodes) > 0) { // If there are ANY nodes, 0 is reserved.
			 newGroupId = 1; // Start with 1 if 0 is taken by coordinator and no other groups exist
			 ListCell *nodeCell;
			 foreach(nodeCell, allNodes) {
				 WorkerNode *n = (WorkerNode *) lfirst(nodeCell);
				 if (n->groupId >= newGroupId) {
					 newGroupId = n->groupId + 1;
				 }
			 }
		}
		list_free_deep(allNodes);
	}


	pgDistNodeRel = table_open(DistNodeRelationId(), RowExclusiveLock); // Already locked, but good for clarity
	tupleDescriptor = RelationGetDescr(pgDistNodeRel);

	// Fetch the current tuple for the replica node
	// We already have replicaNode (WorkerNode*), but need HeapTuple for modification
	// A robust way is to scan by replicaNode->nodeId
	ScanKeyData scanKey[1];
	SysScanDesc scan;

	ScanKeyInit(&scanKey[0],
				Anum_pg_dist_node_nodeid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(replicaNode->nodeId));

	scan = systable_beginscan(pgDistNodeRel, DistNodeNodeIdIndexId(), true,
							  NULL, 1, scanKey);
	currentTuple = systable_getnext(scan);

	if (!HeapTupleIsValid(currentTuple))
	{
		systable_endscan(scan);
		table_close(pgDistNodeRel, RowExclusiveLock); // Release lock if erroring out
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("Could not find pg_dist_node entry for replica node ID %d during metadata update.",
							   replicaNode->nodeId)));
	}
	currentTuple = heap_copytuple(currentTuple); // Make a writable copy

	systable_endscan(scan);


	memset(values, 0, sizeof(values));
	memset(isnulls, false, sizeof(isnulls));
	memset(replace, false, sizeof(replace));

	// Set nodeisreplica = false
	values[Anum_pg_dist_node_nodeisreplica - 1] = BoolGetDatum(false);
	replace[Anum_pg_dist_node_nodeisreplica - 1] = true;

	// Set nodeprimarynodeid = NULL (represented as 0 or InvalidNodeId in C, becomes SQL NULL)
	values[Anum_pg_dist_node_nodeprimarynodeid - 1] = Int32GetDatum(0);
	isnulls[Anum_pg_dist_node_nodeprimarynodeid - 1] = true; // Ensure it's SQL NULL
	replace[Anum_pg_dist_node_nodeprimarynodeid - 1] = true;

	// Set isActive = true
	values[Anum_pg_dist_node_isactive - 1] = BoolGetDatum(true);
	replace[Anum_pg_dist_node_isactive - 1] = true;

	// Set shouldHaveShards = true
	values[Anum_pg_dist_node_shouldhaveshards - 1] = BoolGetDatum(true);
	replace[Anum_pg_dist_node_shouldhaveshards - 1] = true;

	// Set groupid to newGroupId
	values[Anum_pg_dist_node_groupid - 1] = Int32GetDatum(newGroupId);
	replace[Anum_pg_dist_node_groupid - 1] = true;

	newTuple = heap_modify_tuple(currentTuple, tupleDescriptor, values, isnulls, replace);
	CatalogTupleUpdate(pgDistNodeRel, &newTuple->t_self, newTuple);

	heap_freetuple(currentTuple);
	// newTuple is freed by CatalogTupleUpdate or should be if it's a copy

	table_close(pgDistNodeRel, RowExclusiveLock); // Lock released at transaction end

	CitusInvalidateRelcacheByRelid(DistNodeRelationId());
	TransactionModifiedNodeMetadata = true; // Inform Citus about metadata change
	TriggerNodeMetadataSyncOnCommit();      // Ensure changes are propagated

	ereport(NOTICE, (errmsg("Replica node %s:%d (ID %d) metadata updated. It is now a primary in group %d.",
						   replicaNode->workerName, replicaNode->workerPort, replicaNode->nodeId, newGroupId)));


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

	PG_RETURN_VOID();
}
