/*-------------------------------------------------------------------------
 *
 * pg_tracing_utils.c
 *		pg_tracing utilities.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_tracing.h"

/*
 * Add a new string to the provided stringinfo
 */
int
add_str_to_trace_buffer(StringInfo str_info, const char *str, int str_len)
{
	int			position = str_info->cursor;

	Assert(str_len > 0);

	appendBinaryStringInfo(str_info, str, str_len);
	appendStringInfoChar(str_info, '\0');
	str_info->cursor = str_info->len;
	return position;
}

/*
 * Add the worker name to the provided stringinfo
 */
int
add_worker_name_to_trace_buffer(StringInfo str_info, int parallel_worker_number)
{
	int			position = str_info->cursor;

	appendStringInfo(str_info, "Worker %d", parallel_worker_number);
	appendStringInfoChar(str_info, '\0');
	str_info->cursor = str_info->len;
	return position;
}
