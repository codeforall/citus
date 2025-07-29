/*-------------------------------------------------------------------------
 *
 * jsonb.h
 *	  Declarations for jsonb utility functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_JSONB_H
#define CITUS_JSONB_H

#include "postgres.h"
#include "utils/jsonb.h"

#include "distributed/metadata_utility.h"

/*
 * JobConfigOption is a struct that represents a single configuration
 * option for a background job.
 */
typedef struct JobConfigOption
{
	char *name;
	char *value;
	bool is_local;
} JobConfigOption;

extern Jsonb * JobConfigOptionArrayToJsonb(JobConfigOption *options, int count);

#endif /* CITUS_JSONB_H */
