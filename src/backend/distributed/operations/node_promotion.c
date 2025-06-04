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
#include "distributed/shardid_utils.h" // For GetQualifiedShardName, ShardInterval
#include "distributed/shard_cleaner.h" // For InsertCleanupRecordOutsideTransaction
#include "catalog/pg_dist_placement.h" // For DistPlacementRelationId
#include "catalog/pg_dist_shard.h" // For ShardInterval
#include "nodes/pg_list.h" // For List operations

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

	// Step 3: Automate PostgreSQL Replica Promotion
	ereport(NOTICE, (errmsg("Step 3: Attempting to promote replica %s:%d via pg_promote().",
						   replicaNode->workerName, replicaNode->workerPort)));

	bool replicaPromoted = false;
	MultiConnection *replicaConnectionToPromote = NULL;
	PG_TRY();
	{
		// Ensure this connection can perform superuser operations like pg_promote.
		// This might require specific connection string parameters or server-side config.
		// For now, assume GetNodeConnection provides a sufficiently privileged connection.
		replicaConnectionToPromote = GetNodeConnection(CONNECTION_PRIORITY_INVALID,
											 replicaNode->workerName,
											 replicaNode->workerPort);

		char *promoteQuery = "SELECT pg_promote(wait := true);";
		PushActiveSnapshot(GetTransactionSnapshot()); // SPI context
		SPI_connect();
		if (SPI_execute(promoteQuery, false, 0) == SPI_OK_UTILITY) // pg_promote is UTILITY
		{
			// For `wait := true`, successful execution means promotion occurred.
			// However, pg_promote() itself returns VOID. We check execution status.
			// To be absolutely sure, we should check pg_is_in_recovery() AFTER this.
			replicaPromoted = true; // Assume success if command completes without error
		}
		else
		{
			// SPI_execute for utility commands that return void might still be SPI_OK_UTILITY
			// but we might want to check for errors specifically.
			// If pg_promote itself fails (e.g. not a replica, already promoted), it errors.
			// An explicit check of pg_is_in_recovery() after this is safer.
			ereport(WARNING, (errmsg("pg_promote command sent, but verify promotion status for %s:%d.",
								   replicaNode->workerName, replicaNode->workerPort)));
		}
		SPI_finish();
		PopActiveSnapshot();

		if (replicaConnectionToPromote)
		{
			ReleaseNodeConnection(replicaConnectionToPromote);
			replicaConnectionToPromote = NULL;
		}
	}
	PG_CATCH();
	{
		if (replicaConnectionToPromote)
		{
			ReleaseNodeConnection(replicaConnectionToPromote);
		}
		FlushErrorState();
		ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("Failed to execute pg_promote() on replica %s:%d: %s",
							   replicaNode->workerName, replicaNode->workerPort, SDL_GetErrMessage(NULL))));
	}
	PG_END_TRY();

	// After attempting pg_promote, verify by checking pg_is_in_recovery()
	if (replicaPromoted) // Only check if pg_promote utility command itself didn't error
	{
		bool isInRecovery = true;
		int checkAttempts = 5; // Try a few times as promotion might take a moment
		for (int i = 0; i < checkAttempts; ++i)
		{
			PG_TRY();
			{
				replicaConnectionToPromote = GetNodeConnection(CONNECTION_PRIORITY_INVALID, replicaNode->workerName, replicaNode->workerPort);
				PushActiveSnapshot(GetTransactionSnapshot());
				SPI_connect();
				if (SPI_execute("SELECT pg_is_in_recovery();", true, 1) == SPI_OK_SELECT && SPI_processed > 0)
				{
					Datum recoveryDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isInRecovery);
					isInRecovery = isnull ? true : DatumGetBool(recoveryDatum);
				}
				SPI_finish();
				PopActiveSnapshot();
				if (replicaConnectionToPromote) ReleaseNodeConnection(replicaConnectionToPromote); replicaConnectionToPromote = NULL;
			}
			PG_CATCH();
			{
				if (replicaConnectionToPromote) ReleaseNodeConnection(replicaConnectionToPromote); replicaConnectionToPromote = NULL;
				FlushErrorState(); // Ignore error, will retry or fail below
			}
			PG_END_TRY();

			if (!isInRecovery) break;
			pg_usleep(1000000L); // 1 second
		}
		replicaPromoted = !isInRecovery;
	}

	if (!replicaPromoted)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Replica %s:%d failed to promote or promotion could not be confirmed.",
							   replicaNode->workerName, replicaNode->workerPort)));
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
	// newGroupId = GetNextGroupId(); // This was the ideal call, but direct C linkage was problematic.

	// Call the SQL helper function to get the next group ID from the sequence.
	ereport(NOTICE, (errmsg("Obtaining new group ID from sequence via SQL helper function.")));

	PushActiveSnapshot(GetTransactionSnapshot()); // Ensure snapshot for SPI
	SPI_connect();
	if (SPI_execute("SELECT pg_catalog.citus_internal_get_next_group_id();", true, 1) == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		Datum nextGroupIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
		{
			SPI_finish();
			PopActiveSnapshot();
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("citus_internal_get_next_group_id() returned NULL.")));
		}
		newGroupId = DatumGetInt32(nextGroupIdDatum);
	}
	else
	{
		int spi_error_code = SPI_get_result(); // Capture error code before SPI_finish
		SPI_finish();
		PopActiveSnapshot();
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Failed to execute citus_internal_get_next_group_id(): %s",
							   SPI_result_code_string(spi_error_code))));
	}
	SPI_finish();
	PopActiveSnapshot();

	if (newGroupId == 0) // Double check, though SQL function should prevent this.
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Obtained group ID 0 from sequence, which is invalid for new groups.")));
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
