/*-------------------------------------------------------------------------
 *
 * jsonb.c
 *	  Utility functions for jsonb.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "distributed/jsonb.h"


/*
 * JobConfigOptionArrayToJsonb converts an array of JobConfigOption structs
 * to a jsonb object.
 */
Jsonb *
JobConfigOptionArrayToJsonb(JobConfigOption *options, int count)
{
	JsonbParseState *state = NULL;
	JsonbValue *res = NULL;

	(void) pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);

	for (int i = 0; i < count; i++)
	{
		(void) pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		JsonbValue key;
		key.type = jbvString;

		key.val.string.val = "name";
		key.val.string.len = 4;
		(void) pushJsonbValue(&state, WJB_KEY, &key);

		JsonbValue value;
		value.type = jbvString;
		value.val.string.val = options[i].name;
		value.val.string.len = strlen(options[i].name);
		(void) pushJsonbValue(&state, WJB_VALUE, &value);

		key.val.string.val = "value";
		key.val.string.len = 5;
		(void) pushJsonbValue(&state, WJB_KEY, &key);

		value.val.string.val = options[i].value;
		value.val.string.len = strlen(options[i].value);
		(void) pushJsonbValue(&state, WJB_VALUE, &value);

		key.val.string.val = "is_local";
		key.val.string.len = 8;
		(void) pushJsonbValue(&state, WJB_KEY, &key);

		value.type = jbvBool;
		value.val.boolean = options[i].is_local;
		(void) pushJsonbValue(&state, WJB_VALUE, &value);

		(void) pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}

	(void) pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	res = pushJsonbValue(&state, WJB_END_ARRAY, NULL);

	return JsonbValueToJsonb(res);
}
