/*-------------------------------------------------------------------------
 *
 * shared_library_init.h
 *	  Functionality related to the initialization of the Citus extension.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARED_LIBRARY_INIT_H
#define SHARED_LIBRARY_INIT_H

// Forward declare Oid if not already available through other includes (it usually is)
// typedef unsigned int Oid;

#define GUC_STANDARD 0
#define MAX_SHARD_COUNT 64000
#define MAX_SHARD_REPLICATION_FACTOR 100

// Function pointer types defined by Citus for calling into Columnar
typedef bool (*CitusColumnarSupportsIndexAM_type)(char *indexAmName);
typedef bool (*CitusIsColumnarTableAmTable_type)(Oid relationId);
// We are intentionally omitting CitusCompressionTypeStr_type and CitusReadColumnarOptions_type
// for now to simplify decoupling. If direct C-level calls are needed for these,
// they will be identified during compilation of other files and addressed then,
// possibly by using opaque structs or more specialized interface functions.

extern PGDLLEXPORT bool IsColumnarModuleAvailable;

extern PGDLLEXPORT CitusColumnarSupportsIndexAM_type extern_ColumnarSupportsIndexAM;
extern PGDLLEXPORT CitusIsColumnarTableAmTable_type extern_IsColumnarTableAmTable;
// extern PGDLLEXPORT CitusCompressionTypeStr_type extern_CompressionTypeStr;
// extern PGDLLEXPORT CitusReadColumnarOptions_type extern_ReadColumnarOptions;


extern void StartupCitusBackend(void);
extern const char * GetClientMinMessageLevelNameForValue(int minMessageLevel);

#endif /* SHARED_LIBRARY_INIT_H */
