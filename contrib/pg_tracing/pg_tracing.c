/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *
 *
 * Copyright (c) 2008-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *
 * dddbs='postgres.db',traceparent='00-00000000000000007ed39623a1d7b2d2-7ed39623a1d7b2d2-00' SELECT count(*) from pgbench_accounts
 * 00 -> version
 * 00000000000000007ed39623a1d7b2d2 -> traceid
 * 7ed39623a1d7b2d2 -> sampleid
 * 00 -> sampled
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "funcapi.h"
#include "access/parallel.h"
#include "nodes/params.h"
#include "utils/guc.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"

PG_MODULE_MAGIC;

static int	pg_tracing_log_level = LOG;
static const struct config_enum_entry loglevel_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Global statistics for pgtracing
 */
typedef struct pgTracingGlobalStats
{
	int64		traces;
	int64		spans;
	TimestampTz stats_reset;	/* timestamp with all stats reset */
} pgTracingGlobalStats;

typedef enum resourceType
{
	EXECUTOR_START,
	EXECUTOR_RUN,
} resourceType;

typedef struct span
{
	int64 trace_id;
	int64 span_id;
	int64 parent_id;
	resourceType resource_type;
	TimestampTz start;
	TimestampTz end;
} span;

static char *resource_type_to_str(resourceType r) {
	switch (r) {
		case EXECUTOR_START: return "ExecutorStart";
		case EXECUTOR_RUN: return "ExecutorRun";
		default: return "Unknown";
	}
}

/*
 * Global shared state
 */
typedef struct pgTracingSharedStats
{
	slock_t		mutex;				/* protects following fields only: */
	pgTracingGlobalStats stats;		/* global statistics for pgtracing */
	int start_span;
	int end_span;
	span spans[FLEXIBLE_ARRAY_MEMBER];
} pgTracingSharedStats;

static int64 current_trace_id;
static int64 current_parent_id;
static int current_sampled;
static pgTracingSharedStats *pgtracing = NULL;

static void tracing_shmem_startup(void);

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static void pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static void pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count, bool execute_once);
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);

/* Number of output arguments (columns) for pg_stat_statements_info */
#define PG_TRACING_INFO_COLS	3
#define PG_TRACING_TRACES_COLS	6


static int pg_tracing_max = 5000;

/*
 * Module load callback
 */
void
_PG_init(void)
{

	DefineCustomEnumVariable("pg_tracing.log_level",
							 "Log level for the plan.",
							 NULL,
							 &pg_tracing_log_level,
							 LOG,
							 loglevel_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	/* Install hooks. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = tracing_shmem_startup;

	prev_planner_hook = planner_hook;
	/* planner_hook = pgss_planner; */

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_tracing_post_parse_analyze;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_tracing_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_tracing_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_tracing_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_tracing_ExecutorEnd;
}

static void
tracing_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgtracing = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgtracing = ShmemInitStruct("pg_tracing",
						   sizeof(pgTracingSharedStats) + pg_tracing_max * sizeof(span),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgtracing->start_span = 0;
		pgtracing->end_span = 0;

		pgtracing->stats.traces = 0;
		pgtracing->stats.stats_reset = GetCurrentTimestamp();
	}

	LWLockRelease(AddinShmemInitLock);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;
}

static void
add_span(span *new_span)
{

	{
		volatile pgTracingSharedStats *s = (volatile pgTracingSharedStats *) pgtracing;
		SpinLockAcquire(&s->mutex);
		s->spans[s->end_span] = *new_span;
		s->end_span++;
        if(s->end_span >= pg_tracing_max)
            s->end_span = 0;
		s->stats.spans++;
		SpinLockRelease(&s->mutex);
	}
}

/*
 * Post-parse-analysis hook: mark query with a queryId
 */
static void
pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	const char *query_str;
	const char *expected_start="/*";
	const char *traceparent;
	char *end_search;
	char *end_comment;
	current_trace_id = 0;
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* Safety check... */
	if (!pgtracing)
		return;

	query_str = pstate->p_sourcetext;
	// 00 -> version
	// 00000000000000007ed39623a1d7b2d2 -> traceid
	// 7ed39623a1d7b2d2 -> spanid parent
	// 01 -> sampled
    // /*dddbs='postgres.db',traceparent='00-00000000000000007ed39623a1d7b2d2-7ed39623a1d7b2d2-01'*/ SELECT count(*) from pgbench_accounts
	for (size_t i = 0; i < strlen(expected_start); i++) {
		if (query_str[i] != expected_start[i]) {
			return;
		}
	}

	end_comment = strstr(query_str, "*/");
	if (end_comment == NULL) {
		return;
	}

	traceparent = strstr(query_str, "traceparent='");
	if (traceparent == NULL || traceparent > end_comment || end_comment - traceparent < 55 + 13) {
		return;
	}
	traceparent = traceparent + 13;

	if (traceparent[2] != '-' || traceparent[35] != '-' || traceparent[52] != '-') {
		return;
	}

	current_trace_id = strtol(&traceparent[3], &end_search, 16);
	current_parent_id = strtol(&traceparent[36], NULL, 16);
	current_sampled = strtol(&traceparent[53], NULL, 16);
	ereport(pg_tracing_log_level,
		 (errmsg("Found trace id"),
		 errdetail("trace_id: %ld, parent_id: %ld, sampled: %d", current_trace_id,
			 current_parent_id, current_sampled)));
}


/*
 * ExecutorStart hook: start up logging if needed
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	span executor_start_span;
	if (current_trace_id == 0) {
		if (prev_ExecutorStart)
			prev_ExecutorStart(queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
		return;
	}
	executor_start_span.trace_id = current_trace_id;
	executor_start_span.parent_id = current_trace_id;
	// TODO generate span id
	executor_start_span.span_id = 0;
	executor_start_span.start = GetCurrentTimestamp();
	executor_start_span.resource_type = EXECUTOR_START;
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
	executor_start_span.end = GetCurrentTimestamp();
	add_span(&executor_start_span);
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count, bool execute_once)
{
	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}


static void add_result_span(ReturnSetInfo *rsinfo, span *span) {
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};

	int i = 0;

	values[i++] = Int64GetDatum(span->trace_id);
	values[i++] = Int64GetDatum(span->parent_id);
	values[i++] = Int64GetDatum(span->span_id);
	values[i++] = CStringGetTextDatum(resource_type_to_str(span->resource_type));
	values[i++] = Int64GetDatum(span->start);
	values[i++] = Int64GetDatum(span->end);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * Return spans
 */
Datum
pg_tracing_spans(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	span span;
    int span_limit;

	if (!pgtracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	InitMaterializedSRF(fcinfo, 0);

	{
		volatile pgTracingSharedStats *s = (volatile pgTracingSharedStats *) pgtracing;
		SpinLockAcquire(&s->mutex);
        span_limit = s->stats.spans > pg_tracing_max ? pg_tracing_max : s->stats.spans;
		for (int i = s->start_span; i < span_limit; i++) {
			span = s->spans[i];
			add_result_span(rsinfo, &span);
		}
		SpinLockRelease(&s->mutex);
	}
	return (Datum) 0;
}

/*
 * Return statistics of pg_tracing.
 */
Datum
pg_tracing_info(PG_FUNCTION_ARGS)
{
	pgTracingGlobalStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};

	if (!pgtracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Read global statistics for pg_stat_statements */
	{
		volatile pgTracingSharedStats *s = (volatile pgTracingSharedStats *) pgtracing;

		SpinLockAcquire(&s->mutex);
		stats = s->stats;
		SpinLockRelease(&s->mutex);
	}

	values[0] = Int64GetDatum(stats.traces);
	values[1] = Int64GetDatum(stats.spans);
	values[2] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Reset pg_tracing statistics.
 */
Datum
pg_tracing_reset(PG_FUNCTION_ARGS)
{
	/*
	 * Reset global statistics for pg_tracing since all entries are
	 * removed.
	 */
	{
		volatile pgTracingSharedStats *s = (volatile pgTracingSharedStats *) pgtracing;
		TimestampTz stats_reset = GetCurrentTimestamp();

		SpinLockAcquire(&s->mutex);
		s->stats.traces = 0;
		s->stats.stats_reset = stats_reset;
		SpinLockRelease(&s->mutex);
	}

	PG_RETURN_VOID();
}
