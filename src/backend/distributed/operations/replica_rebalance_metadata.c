#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"      // For PG_GETARG_INT32, etc.
#include "executor/spi.h" // For SPI_connect, SPI_execute, SPI_finish
#include "lib/stringinfo.h" // For StringInfo
#include "utils/builtins.h" // For ereport, etc.

// Assuming these are the correct headers for these functions/constants
#include "distributed/metadata_utility.h" // For EnsureSuperUser, EnsureCoordinator
#include "distributed/metadata_cache.h"   // For CitusInvalidateRelcacheByRelid
#include "catalog/pg_dist_placement.h"    // For DistPlacementRelationId (actually Oid)
										  // Might need pg_dist_placement_d.h for Anum_* if directly manipulating, but PL/pgSQL handles it.

// If this UDF is in a new .c file, it needs its own PG_MODULE_MAGIC if compiled as a separate module part,
// or ensure it's part of the main citus module. Assuming part of main citus.c linkage.
// For Citus, PG_MODULE_MAGIC is usually in a central file like citus.c or shared_library_init.c
// and not needed in every .c file that has UDFs, as they are linked into a single citus.so.

PG_FUNCTION_INFO_V1(citus_finalize_replica_rebalance_metadata);

Datum
citus_finalize_replica_rebalance_metadata(PG_FUNCTION_ARGS)
{
	int32 original_primary_group_id;
	int32 new_primary_group_id;
	int32 original_primary_node_id;
	int32 new_primary_node_id;
	StringInfoData spi_sql;
	int spi_ret;

	// It's good practice for such functions to ensure they run on the coordinator
	// and by a superuser, as they make significant metadata changes.
	EnsureCoordinator();
	EnsureSuperUser();

	original_primary_group_id = PG_GETARG_INT32(0);
	new_primary_group_id = PG_GETARG_INT32(1);
	original_primary_node_id = PG_GETARG_INT32(2);
	new_primary_node_id = PG_GETARG_INT32(3);

	ereport(NOTICE, (errmsg("Calling PL/pgSQL helper to finalize replica rebalance: original_group_id=%d, new_group_id=%d, old_node_id=%d, new_node_id=%d",
						   original_primary_group_id, new_primary_group_id, original_primary_node_id, new_primary_node_id)));

	PushActiveSnapshot(GetTransactionSnapshot()); // Required for SPI calls that execute SQL
	SPI_connect();

	initStringInfo(&spi_sql);
	appendStringInfo(&spi_sql, "SELECT pg_catalog.citus_internal_rebalance_placements_for_promoted_node(%d, %d, %d, %d);",
					 original_primary_group_id,
					 new_primary_group_id,
					 original_primary_node_id,
					 new_primary_node_id);

	spi_ret = SPI_execute(spi_sql.data, false, 0); // read_only = false

	// For a SELECT statement that calls a VOLATILE function which returns void,
	// SPI_OK_SELINTO is a common successful return code.
	// SPI_OK_UTILITY is for utility commands. SPI_OK_COMMAND for certain DML.
	if (spi_ret != SPI_OK_SELINTO && spi_ret != SPI_OK_UTILITY)
	{
		char *err_detail = SPI_result_code_string(spi_ret);
		SPI_finish();
		PopActiveSnapshot();
		pfree(spi_sql.data);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), // Or a more specific SQL execution error code
						errmsg("Failed to execute internal rebalancing PL/pgSQL function: %s",
							   err_detail)));
	}

	SPI_finish();
	PopActiveSnapshot();
	pfree(spi_sql.data);

	// After the PL/pgSQL function (which modifies pg_dist_placement) completes:
	CitusInvalidateRelcacheByRelid(DistPlacementRelationId());

	// Metadata sync for pg_dist_placement changes is usually handled by
	// TriggerNodeMetadataSyncOnCommit(), which should have been called when pg_dist_node
	// was updated in the (hypothetical, now separate) promotion step.
	// If this UDF is called in a separate transaction or if more immediate sync is desired,
	// TriggerNodeMetadataSyncOnCommit(); could be called here too.
	// For now, relying on prior sync trigger and just invalidating local cache.

	ereport(NOTICE, (errmsg("Metadata rebalancing via PL/pgSQL helper complete. "
						   "Ensure metadata sync is triggered if this is the final step in a multi-UDF process.")));

	PG_RETURN_VOID();
}
