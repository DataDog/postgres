/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *		Generate spans for distributed tracing from SQL query
 *
 * Spans will only be generated for sampled queries. A query is sampled if:
 * - It has a tracecontext propagated throught SQLCommenter and it passes the caller_sample_rate.
 * - It has no SQLCommenter but the query randomly passes the global sample_rate
 *
 * A query with SQLcommenter will look like: /\*dddbs='postgres.db',traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/ select 1;
 * The traceparent fields are detailed in https://www.w3.org/TR/trace-context/#traceparent-header-field-values
 * 00000000000000000000000000000009: trace id
 * 0000000000000005: parent id
 * 01: trace flags (01 == sampled)
 *
 * If sampled, pg_tracing will generate spans for the current statement.
 * A span represents an operation with a start time, an end time and metadatas (block stats, wal stats...).
 * We will track the following operations:
 * - Parse: We estimate a Parse span begining at the start of the statement and the end of post parse.
 * - Top Span: The top span for a statement. They are created after extracting the traceid from traceparent or to represent a nested query.
 * - Planner: We track the time spent in the planner and report the planner counters.
 * - Node Span: Created from planstate. The name is extracted from the node type (IndexScan, SeqScan) and we rely on query instrumentation
 *   to get their durations.
 * - Executor: We only track steps of the Executor that could contain nested queries: Run and Finish
 *
 * A typical traced query will generate the following spans:
 * +-----------------++-----------------------------------------------------------------------------------------+
 * | Type: Parse     || Type: Select (top span)                                                                 |
 * | Operation: Parse|| Operation: Select * pgbench_accounts WHERE aid=$1;                                      |
 * +-----------------++------------------------+---+---+------------------------------------------------------+-+
 *                    | Type: Planner          |   |Type: Executor                                        |
 *                    | Operation: Planner     |   |Operation: Run                                        |
 *                    +------------------------+   +--+--------------------------------------------------++
 *                                                    | Type: IndexScan                                  |
 *                                                    | Operation: IndexScan using pgbench_accounts_pkey |
 *                                                    |            on pgbench_accounts                   |
 *                                                    +--------------------------------------------------+
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *-------------------------------------------------------------------------
 */
#include "pg_tracing.h"

#include "access/xact.h"
#include "common/pg_prng.h"
#include "commands/async.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

typedef enum
{
	PG_TRACING_TRACK_NONE,		/* track no statements */
	PG_TRACING_TRACK_TOP,		/* only top level statements */
	PG_TRACING_TRACK_ALL		/* all statements, including nested ones */
}			pgTracingTrackLevel;

typedef enum
{
	PG_TRACING_KEEP_ON_FULL,	/* Keep existing buffers when full */
	PG_TRACING_DROP_ON_FULL		/* Drop current buffers when full */
}			pgTracingBufferMode;

/*
 * Structure to store flexible array of spans
 */
typedef struct pgTracingSpans
{
	int			end;			/* Index of last element */
	int			max;			/* Maximum number of element */
	Span		spans[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingSpans;

/*
 * Structure to store per exec level informations
 */
typedef struct pgTracingPerLevelBuffer
{
	Span		top_span;		/* current top span for this level */
	Span		parse_span;		/* parse span for this level */
	Span		executor_run;	/* executor run span for this level. Executor
								 * run is used as parent for spans generated
								 * from planstate */
	uint64		query_id;		/* Query id by for this level when available */
}			pgTracingPerLevelBuffer;

/*
 * Structure to store Query id filtering array of query id used for filtering
 */
typedef struct pgTracingQueryIdFilter
{
	int			num_query_id;	/* number of query ids */
	uint64		query_ids[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingQueryIdFilter;

/* GUC variables */
static int	pg_tracing_max_span;	/* Maximum number of spans to store */
static int	pg_tracing_max_parameter_str;	/* Maximum number of spans to
											 * store */
static bool pg_tracing_trace_parallel_workers = true;	/* True to generate
														 * spans from parallel
														 * workers */
static bool pg_tracing_instrument_buffers;	/* Enable buffers instrumentation */
static bool pg_tracing_instrument_wal;	/* Enable WAL instrumentation */
static double pg_tracing_sample_rate = 0;	/* Sample rate applied to queries
											 * without SQLCommenter */
static double pg_tracing_caller_sample_rate = 1;	/* Sample rate applied to
													 * queries with
													 * SQLCommenter */
static bool pg_tracing_export_parameters = true;	/* Export query's
													 * parameters as span
													 * metadata */
static bool pg_tracing_deparse_plan = true; /* Deparse plan to generate more
											 * detailed spans */
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;	/* tracking level */
static bool pg_tracing_track_utility = true;	/* whether to track utility
												 * commands */
static int	pg_tracing_buffer_mode = PG_TRACING_KEEP_ON_FULL;	/* behaviour on full
																 * buffer */
static char *pg_tracing_notify_channel = NULL;	/* Name of the channel to
												 * notify when span buffer
												 * exceeds a provided
												 * threshold */
static char *pg_tracing_filter_query_ids = NULL;	/* only sample query
													 * matching query ids */
static double pg_tracing_notify_threshold = 0.8;	/* threshold for span
													 * buffer usage
													 * notification */

static const struct config_enum_entry track_options[] =
{
	{"none", PG_TRACING_TRACK_NONE, false},
	{"top", PG_TRACING_TRACK_TOP, false},
	{"all", PG_TRACING_TRACK_ALL, false},
	{NULL, 0, false}
};

static const struct config_enum_entry buffer_mode_options[] =
{
	{"keep_on_full", PG_TRACING_KEEP_ON_FULL, false},
	{"drop_on_full", PG_TRACING_DROP_ON_FULL, false},
	{NULL, 0, false}
};

#define pg_tracking_level(level) \
	((pg_tracing_track == PG_TRACING_TRACK_ALL || \
	(pg_tracing_track == PG_TRACING_TRACK_TOP && (level) == 0)))

#define pg_tracing_enabled(trace_context, level) \
	(trace_context->sampled && pg_tracking_level(level))

#define US_IN_S INT64CONST(1000000)
#define INT64_HEX_FORMAT "%016" INT64_MODIFIER "x"

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Global variables
 */

/* Memory context for pg_tracing. */
static MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static struct pgTracingTraceContext root_trace_context;

/* trace context used in nested levels or within executor hooks */
static struct pgTracingTraceContext current_trace_context;

/* Latest trace id observed */
static TraceId latest_trace_id;

/* Latest local transaction id traced */
static LocalTransactionId latest_lxid = InvalidLocalTransactionId;

/* Shared state with stats and file external state */
static pgTracingSharedState * pg_tracing = NULL;

/*
 * Shared buffer storing spans. Query with sampled flag will add new spans to
 * the shared state at the end of the traced query. Those spans will be consumed during calls to
 * pg_tracing_consume_spans.
 */
static pgTracingSpans * shared_spans = NULL;

/*
 * Store spans for the current trace.
 * They will be added to shared_spans at the end of the query tracing.
 */
static pgTracingSpans * current_trace_spans;

/*
* Text for spans are buffered in this stringinfo and written at
* the end of the query tracing in a single write.
*/
static StringInfo current_trace_text;

/*
 * Maximum nested level for a query to know how many top spans we need to
 * copy in shared_spans.
 */
static int	max_nested_level = -1;

/* Current nesting depth of planner calls */
static int	plan_nested_level = 0;

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
static int	exec_nested_level = 0;

/* Previous nested level observed during post parse */
static int	previous_parse_nested_level = 0;

/* Number of allocated levels. */
static int	allocated_nested_level = 0;

/* Possible start for a nested span */
static TimestampTz nested_query_start_time = 0;

/* Timestamp of the latest statement checked for sampling. */
static TimestampTz last_statement_check_for_sampling = 0;

/* Number of spans allocated at the start of a trace at the same time. */
static int	pg_tracing_initial_allocated_spans = 25;

static pgTracingPerLevelBuffer * per_level_buffers = NULL;
static pgTracingQueryIdFilter * query_id_filter = NULL;

static void pg_tracing_shmem_request(void);
static void pg_tracing_shmem_startup(void);

/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static PlannedStmt *pg_tracing_planner_hook(Query *parse,
											const char *query_string,
											int cursorOptions,
											ParamListInfo params);
static void pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count, bool execute_once);
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);
static void pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context, ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest, QueryCompletion *qc);

static TimestampTz generate_member_nodes(PlanState **planstates, int nplans, planstateTraceContext * planstateTraceContext,
										 uint64 parent_id, TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end);
static TimestampTz generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext,
												uint64 parent_id, TimestampTz root_end, TimestampTz *latest_end);
static pgTracingStats get_empty_pg_tracing_stats(void);

static bool check_filter_query_ids(char **newval, void **extra, GucSource source);
static void assign_filter_query_ids(const char *newval, void *extra);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_tracing.max_span",
							"Maximum number of spans stored in shared memory.",
							NULL,
							&pg_tracing_max_span,
							5000,
							0,
							500000,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.max_parameter_size",
							"Maximum size of parameters. -1 to disable parameter in top span.",
							NULL,
							&pg_tracing_max_parameter_str,
							1024,
							0,
							10000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_tracing.instrument_buffers",
							 "Instrument and add buffers usage in spans.",
							 NULL,
							 &pg_tracing_instrument_buffers,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.instrument_wal",
							 "Instrument and add WAL usage in spans.",
							 NULL,
							 &pg_tracing_instrument_wal,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.trace_parallel_workers",
							 "Whether to generate samples from parallel workers.",
							 NULL,
							 &pg_tracing_trace_parallel_workers,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_tracing.track",
							 "Selects which statements are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track,
							 PG_TRACING_TRACK_ALL,
							 track_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.track_utility",
							 "Selects whether utility commands are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track_utility,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_tracing.buffer_mode",
							 "Controls behaviour on full buffer.",
							 NULL,
							 &pg_tracing_buffer_mode,
							 PG_TRACING_KEEP_ON_FULL,
							 buffer_mode_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.sample_rate",
							 "Fraction of queries without sampled flag or tracecontext to process.",
							 NULL,
							 &pg_tracing_sample_rate,
							 0.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.caller_sample_rate",
							 "Fraction of queries having a tracecontext with sampled flag to process.",
							 NULL,
							 &pg_tracing_caller_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("pg_tracing.filter_query_ids",
							   "Limiting sampling to the provided query ids.",
							   NULL,
							   &pg_tracing_filter_query_ids,
							   "",
							   PGC_USERSET,
							   GUC_LIST_INPUT,
							   check_filter_query_ids,
							   assign_filter_query_ids,
							   NULL);

	DefineCustomBoolVariable("pg_tracing.export_parameters",
							 "Export query's parameters as span metadata.",
							 NULL,
							 &pg_tracing_export_parameters,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.deparse_plan",
							 "Deparse query plan to generate details more details on a plan node.",
							 NULL,
							 &pg_tracing_deparse_plan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("pg_tracing.notify_channel",
							   "Name of the channel to notify when span buffer reaches a provided threshold.",
							   NULL,
							   &pg_tracing_notify_channel,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomRealVariable("pg_tracing.notify_threshold",
							 "When span buffer exceeds this threshold, a notification will be sent.",
							 NULL,
							 &pg_tracing_notify_threshold,
							 0.8,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_tracing");

	/* For jumble state */
	EnableQueryId();

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_tracing_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_tracing_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_tracing_post_parse_analyze;

	prev_planner_hook = planner_hook;
	planner_hook = pg_tracing_planner_hook;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_tracing_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_tracing_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_tracing_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_tracing_ExecutorEnd;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pg_tracing_ProcessUtility;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pg_tracing_memsize(void)
{
	Size		size;

	/* pg_tracing shared state */
	size = sizeof(pgTracingSharedState);
	/* span struct */
	size = add_size(size, sizeof(pgTracingSpans));
	/* the span variable array */
	size = add_size(size, mul_size(pg_tracing_max_span, sizeof(Span)));
	/* and the parallel workers context  */
	size = add_size(size, mul_size(max_parallel_workers, sizeof(pgTracingParallelContext)));

	return size;
}

/*
 * Parse query id for sampling filter
 */
static bool
check_filter_query_ids(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *queryidlist;
	ListCell   *l;
	pgTracingQueryIdFilter *result;
	pgTracingQueryIdFilter *query_ids;
	int			num_query_ids = 0;
	size_t		size_query_id_filter;

	if (strcmp(*newval, "") == 0)
	{
		*extra = NULL;
		return true;
	}

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	if (!SplitIdentifierString(rawstring, ',', &queryidlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(queryidlist);
		return false;
	}

	size_query_id_filter = sizeof(pgTracingQueryIdFilter) + list_length(queryidlist) * sizeof(uint64);
	/* Work on a palloced buffer */
	query_ids = (pgTracingQueryIdFilter *) palloc(size_query_id_filter);

	foreach(l, queryidlist)
	{
		char	   *query_id_str = (char *) lfirst(l);
		int64		query_id = strtoi64(query_id_str, NULL, 10);

		if (errno == EINVAL || errno == ERANGE)
		{
			GUC_check_errdetail("Query id is not a valid int64: \"%s\".", query_id_str);
			pfree(rawstring);
			list_free(queryidlist);
			return false;
		}
		query_ids->query_ids[num_query_ids++] = query_id;
	}
	query_ids->num_query_id = num_query_ids;

	pfree(rawstring);
	list_free(queryidlist);

	/* Copy query id filter to a guc malloced result */
	result = (pgTracingQueryIdFilter *) guc_malloc(LOG, size_query_id_filter);
	if (result == NULL)
		return false;
	memcpy(result, query_ids, size_query_id_filter);

	*extra = result;
	return true;
}

/*
 * Assign parsed query id filter
 */
static void
assign_filter_query_ids(const char *newval, void *extra)
{
	query_id_filter = (pgTracingQueryIdFilter *) extra;
}

/*
 * shmem_request hook: request additional shared resources.  We'll allocate
 * or attach to the shared resources in pgss_shmem_startup().
 */
static void
pg_tracing_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(pg_tracing_memsize());
}

/*
 * Store span in the current_trace_spans buffer
 */
static void
store_span(const Span * span)
{
	int			index;

	Assert(span->ended);
	Assert(span->span_id > 0);

	if (current_trace_spans->end >= current_trace_spans->max)
	{
		MemoryContext oldcxt;

		/* Need to extend. */
		/* TODO: Have a configurable limit? */
		current_trace_spans->max *= 2;
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		current_trace_spans = repalloc(current_trace_spans, sizeof(pgTracingSpans) + current_trace_spans->max * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
	}
	index = current_trace_spans->end++;
	current_trace_spans->spans[index] = *span;
}

/*
 * Get the span index of the top span for a specific nested level
 */
static Span *
get_top_span_for_nested_level(int nested_level)
{
	Assert(nested_level <= max_nested_level);
	Assert(nested_level >= 0);
	return &per_level_buffers[nested_level].top_span;
}

/*
 * End top span for the current nested level
 */
static void
end_top_span(const TimestampTz *end_time, const Span * previous_top_span)
{
	Span	   *top_span;

	/* Check if the level was allocated */
	if (exec_nested_level > max_nested_level)
		return;

	top_span = get_top_span_for_nested_level(exec_nested_level);
	if (!top_span->ended)
		end_span(top_span, end_time);

	/* Restore previous top span if provided */
	if (previous_top_span != NULL)
	{
		/* Store latest top span */
		store_span(top_span);
		/* Restore previous top span */
		*top_span = *previous_top_span;
	}
}

/*
 * Add span to the shared memory. Mutex must be acquired beforehand.
 */
static void
add_span_to_shared_buffer_locked(const Span * span)
{
	/* Spans must be ended before adding them to the shared buffer */
	Assert(span->ended);
	if (shared_spans->end + 1 >= shared_spans->max)
		pg_tracing->stats.dropped_spans++;
	else
	{
		pg_tracing->stats.spans++;
		shared_spans->spans[shared_spans->end++] = *span;
	}
}

/*
 * Check if we still have available space in the shared spans.
 *
 * Between the moment we check and the moment we insert spans, the buffer
 * may be full but we will redo a check before. This check is done
 * when starting a top span to bail out early if the buffer is
 * already full.
 */
static bool
check_full_shared_spans()
{
	if (shared_spans->end + 1 >= shared_spans->max)
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;
		bool		full_buffer = false;

		SpinLockAcquire(&s->mutex);
		if (pg_tracing_buffer_mode == PG_TRACING_DROP_ON_FULL)
		{
			s->stats.dropped_spans += shared_spans->end;
			shared_spans->end = 0;
			s->extent = 0;
		}
		else
		{
			full_buffer = true;
			s->stats.dropped_spans++;
		}
		SpinLockRelease(&s->mutex);
		return full_buffer;
	}
	return false;
}

/*
 * Reset trace_context fields
 */
static void
reset_trace_context(pgTracingTraceContext * trace_context)
{
	trace_context->sampled = 0;
	trace_context->trace_id.traceid_right = 0;
	trace_context->trace_id.traceid_left = 0;
	trace_context->parent_id = 0;
	trace_context->root_span.span_id = 0;
	trace_context->in_aborted_transaction = false;
}

/*
 * shmem_startup hook: allocate or attach to shared memory, Also create and
 * load the query-texts file, which is expected to exist (even if empty)
 * while the module is enabled.
 */
static void
pg_tracing_shmem_startup(void)
{
	bool		found_pg_tracing;
	bool		found_shared_spans;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pg_tracing = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	pg_tracing = ShmemInitStruct("PgTracing Shared", sizeof(pgTracingSharedState), &found_pg_tracing);
	shared_spans = ShmemInitStruct("PgTracing Spans",
								   sizeof(pgTracingSpans) + pg_tracing_max_span * sizeof(Span),
								   &found_shared_spans);

	/* Initialize pg_tracing memory context */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/* Initialize shmem for trace propagation to parallel workers */
	pg_tracing_shmem_parallel_startup();

	/* First time, let's init shared state */
	if (!found_pg_tracing)
	{
		pg_tracing->stats = get_empty_pg_tracing_stats();
		SpinLockInit(&pg_tracing->mutex);
	}
	if (!found_shared_spans)
	{
		shared_spans->end = 0;
		shared_spans->max = pg_tracing_max_span;
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters and generate span nodes from queryDesc planstate
 */
static void
process_query_desc(pgTracingTraceContext * trace_context, QueryDesc *queryDesc, int sql_error_code, TimestampTz *end_time)
{
	NodeCounters *node_counters = &get_top_span_for_nested_level(exec_nested_level)->node_counters;

	/* Process total counters */
	if (queryDesc->totaltime)
	{
		InstrEndLoop(queryDesc->totaltime);
		node_counters->buffer_usage = queryDesc->totaltime->bufusage;
		node_counters->wal_usage = queryDesc->totaltime->walusage;
	}
	/* Process JIT counter */
	if (queryDesc->estate->es_jit)
		node_counters->jit_usage = queryDesc->estate->es_jit->instr;
	node_counters->rows = queryDesc->estate->es_total_processed;

	/* Process planstate */
	if (queryDesc->planstate && queryDesc->planstate->instrument != NULL && queryDesc->planstate->instrument->firsttime != 0)
	{
		Bitmapset  *rels_used = NULL;
		planstateTraceContext planstateTraceContext;
		TimestampTz latest_end = 0;
		Span	   *parent_span = &per_level_buffers[exec_nested_level].executor_run;

		planstateTraceContext.rtable_names = select_rtable_names_for_explain(queryDesc->plannedstmt->rtable, rels_used);
		planstateTraceContext.trace_id = trace_context->trace_id;
		planstateTraceContext.ancestors = NULL;
		planstateTraceContext.sql_error_code = sql_error_code;
		/* Prepare the planstate context for deparsing */
		planstateTraceContext.deparse_ctx = NULL;
		if (pg_tracing_deparse_plan)
			planstateTraceContext.deparse_ctx = deparse_context_for_plan_tree(queryDesc->plannedstmt, planstateTraceContext.rtable_names);

		if (parent_span->end == 0)
		{
			Assert(end_time != NULL);
			parent_span->end = *end_time;
		}
		generate_span_from_planstate(queryDesc->planstate, &planstateTraceContext, parent_span->span_id, parent_span->end, &latest_end);
	}
}

/*
 * Create a trace id for the trace context if there's none.
 * If trace was started from the global sample rate without
 * a parent trace, we need to generate a random trace id.
 */
static void
set_trace_id(struct pgTracingTraceContext *trace_context)
{
	if (!traceid_zero(trace_context->trace_id))
	{
		/* Update last lxid seen */
		latest_lxid = MyProc->lxid;
		return;
	}

	/*
	 * We want to keep the same trace id for all statements within the same
	 * transaction. For that, we check if we're in the same local xid.
	 */
	if (MyProc->lxid == latest_lxid)
	{
		trace_context->trace_id = latest_trace_id;
		return;
	}

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace.
	 */
	Assert(trace_context->parent_id == 0);

	trace_context->trace_id.traceid_left = pg_prng_int64(&pg_global_prng_state);
	trace_context->trace_id.traceid_right = pg_prng_int64(&pg_global_prng_state);
	latest_trace_id = trace_context->trace_id;
	latest_lxid = MyProc->lxid;
}

/*
 * Check whether trace context is filtered based on query id
 */
static bool
is_query_id_filtered(const struct pgTracingTraceContext *trace_context)
{
	/* No query id filter defined */
	if (query_id_filter == NULL)
		return false;
	for (int i = 0; i < query_id_filter->num_query_id; i++)
		if (query_id_filter->query_ids[i] == trace_context->query_id)
			return false;
	return true;
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * flag and the provided sample rate configurations
 */
static bool
is_query_sampled(const struct pgTracingTraceContext *trace_context)
{
	double		rand;
	bool		sampled;

	/* Everything is sampled */
	if (pg_tracing_sample_rate >= 1.0)
		return true;

	/* No SQLCommenter sampled and no global sample rate */
	if (!trace_context->sampled && pg_tracing_sample_rate == 0.0)
		return false;

	/* SQLCommenter sampled and caller sample is on */
	if (trace_context->sampled && pg_tracing_caller_sample_rate >= 1.0)
		return true;

	/*
	 * We have a non zero global sample rate or a sampled flag. Either way, we
	 * need a rand value
	 */
	rand = pg_prng_double(&pg_global_prng_state);
	if (trace_context->sampled)
		/* Sampled flag case */
		sampled = (rand < pg_tracing_caller_sample_rate);
	else
		/* Global rate case */
		sampled = (rand < pg_tracing_sample_rate);
	return sampled;
}

/*
 * If the query was started as a prepared statement, we won't be able to
 * extract traceparent during query parsing since parsing was skipped.
 *
 * We assume that SQLCommenter content can be passed as a text in the
 * first parameter and try to extract the trace context from this first parameter.
 */
static void
extract_trace_context_from_parameter(struct pgTracingTraceContext *trace_context, ParamListInfo params)
{
	Oid			typoutput;
	bool		typisvarlena;
	char	   *pstring;
	ParamExternData param;

	if (params == NULL || params->numParams == 0)
		return;

	param = params->params[0];
	/* We only check the first parameter and if it's a text parameter */
	if (param.ptype != TEXTOID)
		return;

	/* Get the text of the first string parameter */
	getTypeOutputInfo(param.ptype, &typoutput, &typisvarlena);
	pstring = OidOutputFunctionCall(typoutput, param.value);
	extract_trace_context_from_query(trace_context, pstring, true);
}

/*
 * Extract trace_context from either the query text or parameters.
 * Sampling rate will be applied to the trace_context.
 */
static void
extract_trace_context(struct pgTracingTraceContext *trace_context, ParseState *pstate, const ParamListInfo params, uint64 query_id)
{
	TimestampTz statement_start_ts;

	/* Safety check... */
	if (!pg_tracing || !pg_tracking_level(exec_nested_level))
		return;

	/* sampling already started */
	if (trace_context->sampled)
		return;

	/* Don't start tracing if we're not at the top */
	if (exec_nested_level > 0)
		return;

	/* Both sampling rate are set to 0, no tracing will happen */
	if (pg_tracing_sample_rate == 0 && pg_tracing_caller_sample_rate == 0)
		return;

	/*
	 * In a parallel worker, check the parallel context shared buffer to see
	 * if the leader left a trace context
	 */
	if (IsParallelWorker())
	{
		if (pg_tracing_trace_parallel_workers)
			fetch_parallel_context(trace_context);
		return;
	}

	Assert(trace_context->root_span.span_id == 0);
	Assert(traceid_zero(trace_context->trace_id));

	if (params != NULL)
		extract_trace_context_from_parameter(trace_context, params);
	else if (pstate != NULL)
		extract_trace_context_from_query(trace_context, pstate->p_sourcetext, false);

	/*
	 * A statement can go through this function 3 times: post parsing, planner
	 * and executor run. If no sampling flag was extracted from SQLCommenter
	 * and we've already seen this statement (using statement_start ts), don't
	 * try to apply sample rate and exit.
	 */
	statement_start_ts = GetCurrentStatementStartTimestamp();
	if (trace_context->sampled == 0 && last_statement_check_for_sampling == statement_start_ts)
		return;
	/* First time we see this statement, save the time */
	last_statement_check_for_sampling = statement_start_ts;

	trace_context->query_id = query_id;
	if (is_query_id_filtered(trace_context))
		/* Query id filter is not matching, disable sampling */
		trace_context->sampled = 0;
	else
		trace_context->sampled = is_query_sampled(trace_context);

	if (trace_context->sampled && check_full_shared_spans())
		/* Buffer is full, abort sampling */
		trace_context->sampled = 0;

	if (trace_context->sampled)
		set_trace_id(trace_context);
	else
		/* No sampling, reset the context */
		reset_trace_context(trace_context);
}

/*
 * Reset pg_tracing memory context and global state.
 */
static void
cleanup_tracing(void)
{
	if (!root_trace_context.sampled && !current_trace_context.sampled)
		/* No need for cleaning */
		return;
	if (pg_tracing_trace_parallel_workers)
		remove_parallel_context();
	MemoryContextReset(pg_tracing_mem_ctx);
	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	max_nested_level = -1;
	previous_parse_nested_level = 0;
	nested_query_start_time = 0;
	current_trace_spans = NULL;
	per_level_buffers = NULL;
}

/*
 * End the query tracing and dump all spans in the shared buffer.
 * This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(pgTracingTraceContext * trace_context)
{
	Size		file_position = 0;
	int			end_spans_number;

	/* We're still a nested query, tracing is not finished */
	if (exec_nested_level + plan_nested_level > 0)
		return;

	/* Dump all buffered texts in file */
	text_store_file(pg_tracing, current_trace_text->data, current_trace_text->len, &file_position);

	/* Store top spans */
	for (int i = 0; i <= max_nested_level; i++)
	{
		Span	   *top_span = &per_level_buffers[i].top_span;

		if (!traceid_zero(top_span->trace_id))
			store_span(top_span);
	}

	/* Store parse spans */
	for (int i = 0; i <= max_nested_level; i++)
	{
		Span	   *parse_span = &per_level_buffers[i].parse_span;

		if (!traceid_zero(parse_span->trace_id))
			store_span(parse_span);
	}

	for (int i = 0; i < current_trace_spans->end; i++)
		adjust_file_offset(current_trace_spans->spans + i, file_position);

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		/* We're at the end, add all spans to the shared memory */
		for (int i = 0; i < current_trace_spans->end; i++)
			add_span_to_shared_buffer_locked(&current_trace_spans->spans[i]);
		end_spans_number = shared_spans->end;
		SpinLockRelease(&s->mutex);
	}

	if (pg_tracing_notify_channel != NULL && !IsParallelWorker())
	{
		int			span_threshold = pg_tracing_max_span * pg_tracing_notify_threshold;

		/*
		 * We can't send notification within an aborted transaction as async
		 * already went through the AtAbort_Notify. Queuing a notification
		 * after could lead to a possible segfault.
		 */
		if (end_spans_number >= span_threshold && !trace_context->in_aborted_transaction)
			/* We've crossed the threshold, send a notification */
			Async_Notify(pg_tracing_notify_channel, NULL);
	}

	/* We can reset the memory context here */
	cleanup_tracing();
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean
 * our memory context.
 *
 * We provide the ongoing span where the error was caught to attach the
 * sql error code to it.
 */
static void
handle_pg_error(pgTracingTraceContext * trace_context,
				Span * ongoing_span, QueryDesc *queryDesc,
				TimestampTz *span_end_input)
{
	Span	   *top_span;
	int			sql_error_code;
	TimestampTz span_end_time;

	/* If we're not sampling the query, bail out */
	if (!pg_tracing_enabled(trace_context, exec_nested_level))
		return;
	sql_error_code = geterrcode();

	if (span_end_input != NULL)
		span_end_time = *span_end_input;
	else
		span_end_time = GetCurrentTimestamp();

	if (queryDesc != NULL)
		process_query_desc(trace_context, queryDesc, sql_error_code, &span_end_time);

	/* Assign the error code to the latest span */
	if (ongoing_span != NULL)
	{
		ongoing_span->sql_error_code = sql_error_code;
		end_span(ongoing_span, &span_end_time);
		store_span(ongoing_span);
	}

	top_span = get_top_span_for_nested_level(exec_nested_level);
	if (exec_nested_level == 0)
	{
		/*
		 * On a planner error, top_span is only stored in the trace_context.
		 * We need to copy it to the per_level_buffers
		 */
		if (traceid_zero(top_span->trace_id))
			*top_span = trace_context->root_span;
	}

	/* Assign the error code to the latest top span */
	top_span->sql_error_code = sql_error_code;

	/* End all ongoing top spans */
	for (int i = 0; i <= max_nested_level; i++)
	{
		Span	   *span = &per_level_buffers[i].top_span;

		if (!span->ended)
			end_span(span, &span_end_time);
	}

	end_tracing(trace_context);
}

/*
 * Get the parent id for the given nested level
 *
 * nested_level can be negative when creating the root top span.
 * In this case, we use the parent id from the propagated trace context.
 */
static uint64
get_parent_id(pgTracingTraceContext * trace_context, int nested_level)
{
	Span	   *top_span;

	if (nested_level < 0)
		return trace_context->parent_id;
	Assert(nested_level <= allocated_nested_level);
	top_span = get_top_span_for_nested_level(nested_level);
	Assert(top_span->span_id > 0);
	return top_span->span_id;
}

/*
 * If we enter a new nested level, initialize all necessary buffers
 * and start_trace timer.
 *
 * The start of a top span can vary: prepared statement will skip parsing, the use of cached
 * plans will skip the planner hook.
 * Thus, a top span can start in post parse, planner hook or executor run.
 */
static bool
initialize_trace_level(void)
{
	Assert(exec_nested_level >= 0);

	/* Check if we've already created a top span for this nested level */
	if (exec_nested_level <= max_nested_level)
		return false;

	/* First time */
	if (max_nested_level == -1)
	{
		MemoryContext oldcxt;

		Assert(pg_tracing_mem_ctx->isReset);

		/*
		 * We need to be able to pass information that depends on the nested
		 * level.
		 *
		 * executor span ids: Since an executor run becomes the parent span,
		 * we need subsequent created node spans to have the correct parent.
		 *
		 * query ids: used to propagate queryId to all spans in the same level
		 *
		 * top spans: We create one top span per nested level and those are
		 * only inserted in the shared buffer at the end
		 */
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		/* initial allocation */
		allocated_nested_level = 1;
		per_level_buffers = palloc0(allocated_nested_level * sizeof(pgTracingPerLevelBuffer));
		current_trace_spans = palloc0(sizeof(pgTracingSpans) + pg_tracing_initial_allocated_spans * sizeof(Span));
		current_trace_spans->max = pg_tracing_initial_allocated_spans;
		current_trace_text = makeStringInfo();

		MemoryContextSwitchTo(oldcxt);
		{
			/* Start of a new trace */
			volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

			SpinLockAcquire(&s->mutex);
			s->stats.traces++;
			SpinLockRelease(&s->mutex);
		}

	}
	else if (exec_nested_level >= allocated_nested_level)
	{
		/* New nested level, allocate more memory */
		MemoryContext oldcxt;
		int			old_allocated_nested_level = allocated_nested_level;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		allocated_nested_level++;
		per_level_buffers = repalloc0(per_level_buffers, old_allocated_nested_level * sizeof(pgTracingPerLevelBuffer),
									  allocated_nested_level * sizeof(pgTracingPerLevelBuffer));
		MemoryContextSwitchTo(oldcxt);
	}

	max_nested_level = exec_nested_level;
	return true;
}

/*
 * Start a new top span if we've entered a new nested level
 * or if the previous span at the same level ended.
 */
static void
begin_top_span(pgTracingTraceContext * trace_context,
			   Span * top_span, CmdType commandType,
			   const Query *query, const JumbleState *jstate,
			   const PlannedStmt *pstmt,
			   const char *query_text, TimestampTz start_time)
{
	int			query_len;
	const char *normalised_query;

	/* in case of a cached plan, query might be unavailable */
	if (query != NULL)
		per_level_buffers[exec_nested_level].query_id = query->queryId;
	else if (trace_context->query_id > 0)
		per_level_buffers[exec_nested_level].query_id = trace_context->query_id;

	/*
	 * With multi-statement queries, we can have multiple top spans in an exec
	 * level. Check if we have an previous top span that was ended and store
	 * it.
	 */
	if (top_span->ended == true)
		store_span(top_span);

	begin_span(trace_context->trace_id, top_span, command_type_to_span_type(commandType),
			   NULL, get_parent_id(trace_context, exec_nested_level - 1),
			   per_level_buffers[exec_nested_level].query_id, &start_time);

	if (IsParallelWorker())
	{
		/*
		 * In a parallel worker, we use the worker name as the span's
		 * operation
		 */
		top_span->operation_name_offset = add_worker_name_to_trace_buffer(current_trace_text, ParallelWorkerNumber);
		return;
	}

	if (jstate && jstate->clocations_count > 0 && query != NULL)
	{
		/* jstate is available, normalise query and extract parameters' values */
		char	   *param_str;
		int			param_len;

		query_len = query->stmt_len;
		normalised_query = normalise_query_parameters(jstate, query_text,
													  query->stmt_location,
													  &query_len, &param_str, &param_len);
		Assert(param_len > 0);
		if (pg_tracing_export_parameters)
			top_span->parameter_offset = add_str_to_trace_buffer(current_trace_text,
																 param_str, param_len);
	}
	else
	{
		/*
		 * No jstate available, normalise query but we won't be able to
		 * extract parameters
		 */
		int			stmt_location;

		if (query != NULL && query->stmt_len > 0)
		{
			query_len = query->stmt_len;
			stmt_location = query->stmt_location;
		}
		else if (pstmt != NULL && pstmt->stmt_location != -1 && pstmt->stmt_len > 0)
		{
			query_len = pstmt->stmt_len;
			stmt_location = pstmt->stmt_location;
		}
		else
		{
			query_len = strlen(query_text);
			stmt_location = 0;
		}
		normalised_query = normalise_query(query_text, stmt_location, &query_len);
	}
	top_span->operation_name_offset = add_str_to_trace_buffer(current_trace_text, normalised_query, query_len);
}

/*
 * Initialise buffers if we are in a new nested level and start associated top span.
 * If the top span already exists for the current nested level, this has no effect.
 *
 * This needs to be called every time a top span could be started: post parse, planner, executor start
 * and process utility
 */
static Span *
initialize_trace_level_and_top_span(pgTracingTraceContext * trace_context, CmdType commandType,
									Query *query, JumbleState *jstate, const PlannedStmt *pstmt,
									const char *query_text, TimestampTz start_time, bool in_parse_plan)
{
	bool		new_allocated_level;
	Span	   *top_span;

	new_allocated_level = initialize_trace_level();
	if (in_parse_plan && exec_nested_level == 0)
	{
		/*
		 * A the root post parse, we want to use trace_context's root_span as
		 * the top span in per_level_buffers might still be ongoing.
		 */
		top_span = &trace_context->root_span;
	}
	else
	{
		top_span = get_top_span_for_nested_level(exec_nested_level);
		if (exec_nested_level == 0 && top_span->span_id != trace_context->root_span.span_id)
			/* At root level, we need to copy the root span content */
			*top_span = trace_context->root_span;
	}

	/*
	 * It's possible to have multiple top spans within a nested query. If the
	 * previous top span was closed, we need to start a new one. If it's still
	 * ongoing, use it.
	 */
	if (!new_allocated_level && top_span->span_id > 0 && top_span->ended == false)
		return top_span;

	/* current_trace_spans buffer should have been allocated */
	Assert(current_trace_spans != NULL);

	/*
	 * Either this is the first top span for this level or the previous one
	 * was finished, start the new top span
	 */
	begin_top_span(trace_context, top_span, commandType, query, jstate, pstmt, query_text, start_time);
	return top_span;
}

/*
 * Post-parse-analyze hook
 *
 * Tracing can be started here if:
 * - The query has a SQLCommenter with traceparent parameter with sampled flag on and passes the caller_sample_rate
 * - The query passes the sample_rate
 *
 * If query is sampled, create a start the top span and possibly create a parse span.
 */
static void
pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	TimestampTz start_top_span;
	Span	   *top_span;
	pgTracingTraceContext *trace_context = &root_trace_context;
	bool		new_lxid = MyProc->lxid != latest_lxid;

	if (!pg_tracing_mem_ctx->isReset && new_lxid && exec_nested_level + plan_nested_level == 0)

		/*
		 * Some errors can happen outside of our PG_TRY (incorrect number of
		 * bind parameters for example) which will leave a dirty trace
		 * context. We can also have sampled individual Parse command through
		 * extended protocol. In this case, we just drop the spans and reset
		 * memory context
		 */
		cleanup_tracing();

	if (exec_nested_level + plan_nested_level == 0)

		/*
		 * At the root level, clean any leftover state of previous trace
		 * context
		 */
		reset_trace_context(&root_trace_context);
	else
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
		return;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, pstate, NULL, query->queryId);

	if (!trace_context->sampled)
		/* Query is not sampled, nothing to do */
		return;

	/*
	 * We want to avoid calling get_ns at the start of post parse as it will
	 * impact all queries and we will only use it when the query is sampled.
	 */
	start_top_span = GetCurrentTimestamp();

	/*
	 * Either we're inside a nested sampled query or we've parsed a query with
	 * the sampled flag, start a new level with a top span
	 */
	top_span = initialize_trace_level_and_top_span(trace_context, query->commandType,
												   query, jstate, NULL,
												   pstate->p_sourcetext, start_top_span,
												   true);

	/*
	 * We only create parse span when we have a good idea of when parsing
	 * started. Since multiple queries can be parsed at once when entering a
	 * new nested level, we only create a parse span when we detect a change
	 * in parse nested level.
	 */
	if ((exec_nested_level + plan_nested_level == 0) ||
		(nested_query_start_time > 0 && previous_parse_nested_level != exec_nested_level))
	{
		TimestampTz start_parse_span;
		Span	   *parse_span = &per_level_buffers[exec_nested_level].parse_span;

		/*
		 * We don't have a precise time for parse end, estimate it
		 *
		 * TODO: Can we get a more precise start of parse?
		 */
		TimestampTz end_parse = start_top_span;

		if (!traceid_zero(per_level_buffers[exec_nested_level].parse_span.trace_id))
		{
			/* We have a previous parse span existing, store it */
			store_span(&per_level_buffers[exec_nested_level].parse_span);
		}

		if (nested_query_start_time > 0 && (exec_nested_level + plan_nested_level) > 0)
			start_parse_span = nested_query_start_time;
		else
			start_parse_span = GetCurrentStatementStartTimestamp();

		/*
		 * Parse should be at the same level as the top_span so we need to use
		 * top_span's parent
		 */
		begin_span(trace_context->trace_id, parse_span, SPAN_PARSE, NULL, top_span->parent_id,
				   per_level_buffers[exec_nested_level].query_id,
				   &start_parse_span);
		end_span(parse_span, &end_parse);
	}

	/* keep track of the previous parse level */
	previous_parse_nested_level = exec_nested_level;
}

/*
 * Planner hook.
 * Tracing can start here if the query skipped parsing (prepared statement) and passes the random sample_rate.
 * If query is sampled, create a plan span and start the top span if not already started.
 */
static PlannedStmt *
pg_tracing_planner_hook(Query *query,
						const char *query_string,
						int cursorOptions,
						ParamListInfo params)
{
	PlannedStmt *result;
	Span	   *top_span;
	Span		span_planner;
	pgTracingTraceContext *trace_context = &root_trace_context;
	TimestampTz start_span_time;

	if (exec_nested_level > 0)
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, params, query->queryId);

	if (!pg_tracing_enabled(trace_context, plan_nested_level + exec_nested_level))
	{
		/* No sampling */
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions,
									   params);
		else
			result = standard_planner(query, query_string, cursorOptions,
									  params);
		return result;
	}

	start_span_time = GetCurrentTimestamp();

	/*
	 * We may have skipped parsing if statement was prepared, create a new top
	 * span in this case.
	 */
	top_span = initialize_trace_level_and_top_span(trace_context, query->commandType, query,
												   NULL, NULL, query_string, start_span_time, true);

	/* Start planner span */
	begin_span(trace_context->trace_id, &span_planner, SPAN_PLANNER,
			   NULL, top_span->span_id,
			   per_level_buffers[exec_nested_level].query_id, &start_span_time);

	plan_nested_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions,
									   params);
		else
			result = standard_planner(query, query_string, cursorOptions,
									  params);
	}
	PG_CATCH();
	{
		plan_nested_level--;
		handle_pg_error(trace_context, &span_planner, NULL, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	plan_nested_level--;

	/* End planner span */
	end_span(&span_planner, NULL);
	store_span(&span_planner);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (params != NULL && pg_tracing_export_parameters)
	{
		char	   *paramStr = BuildParamLogString(params,
												   NULL, pg_tracing_max_parameter_str);

		if (paramStr != NULL)
			top_span->parameter_offset = add_str_to_trace_buffer(current_trace_text, paramStr, strlen(paramStr));
	}
	return result;
}

/*
 * ExecutorStart hook: Activate query instrumentation if query is sampled
 * Tracing can be started here if the query used a cached plan and passes the random sample_rate.
 * If query is sampled, start the top span if it doesn't already exist.
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			instrument_options;
	pgTracingTraceContext *trace_context = &current_trace_context;
	bool		is_lazy_function;
	bool		query_sampled = false;

	if (exec_nested_level + plan_nested_level == 0)
	{
		/* We're at the root level, copy trace context from parsing/planning */
		*trace_context = root_trace_context;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, queryDesc->params, queryDesc->plannedstmt->queryId);

	/*
	 * We can detect the presence of lazy function through the node tag and
	 * the executor flags. Lazy function will go through an ExecutorRun with
	 * every call, possibly generating thousands of spans (ex: lazy call of
	 * generate_series) which is not manageable and not very useful. If lazy
	 * functions are detected, we don't instrument this node and no spans will
	 * be generated.
	 */
	is_lazy_function = nodeTag(queryDesc->plannedstmt->planTree) == T_FunctionScan && eflags == EXEC_FLAG_SKIP_TRIGGERS;
	query_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && !is_lazy_function;

	if (query_sampled)
	{
		TimestampTz start_span_time = GetCurrentTimestamp();

		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook. Parallel workers will also start tracing from
		 * there. Create the top case in this case.
		 */
		initialize_trace_level_and_top_span(trace_context, queryDesc->operation,
											NULL, NULL, NULL,
											queryDesc->sourceText, start_span_time, false);

		/*
		 * Activate query instrumentation. Timer and rows instrumentation are
		 * mandatory.
		 */
		instrument_options = INSTRUMENT_TIMER | INSTRUMENT_ROWS;
		if (pg_tracing_instrument_buffers)
			instrument_options |= INSTRUMENT_BUFFERS;
		if (pg_tracing_instrument_wal)
			instrument_options |= INSTRUMENT_WAL;
		queryDesc->instrument_options = instrument_options;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/* Allocate totaltime instrumentation in the per-query context */
	if (query_sampled && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, instrument_options, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ExecutorRun hook: track nesting depth and create executor run span.
 * If the plan needs to create parallel workers, push the trace context in the parallel shared buffer.
 */
static void
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					   uint64 count, bool execute_once)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	bool		query_sampled = pg_tracing_enabled(trace_context, exec_nested_level);
	bool		executor_sampled = query_sampled && queryDesc->instrument_options > 0;
	TimestampTz span_start_time;
	TimestampTz span_end_time;

	if (query_sampled)
	{
		span_start_time = GetCurrentTimestamp();
		nested_query_start_time = span_start_time;
	}

	if (executor_sampled)
	{
		/*
		 * Executor run is used as the parent's span for nodes, store its' id
		 * in the per level buffer.
		 */
		Span	   *executor_run_span = &per_level_buffers[exec_nested_level].executor_run;

		/* Start executor run span */
		begin_span(trace_context->trace_id, executor_run_span, SPAN_EXECUTOR_RUN,
				   NULL, get_parent_id(trace_context, exec_nested_level),
				   per_level_buffers[exec_nested_level].query_id,
				   &span_start_time);

		/*
		 * If this query starts parallel worker, push the trace context for
		 * the child processes
		 */
		if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
			add_parallel_context(trace_context, executor_run_span->span_id,
								 per_level_buffers[exec_nested_level].query_id);
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_CATCH();
	{
		span_end_time = GetCurrentTimestamp();
		end_top_span(&span_end_time, NULL);
		exec_nested_level--;

		/*
		 * When a query is executing the pg_tracing_{peek,consume}_spans
		 * function, sampling will be disabled while we have an ongoing trace
		 * and current_trace_spans will be NULL.
		 */
		if (current_trace_spans != NULL && executor_sampled)
			handle_pg_error(trace_context, &per_level_buffers[exec_nested_level].executor_run, queryDesc, &span_end_time);
		PG_RE_THROW();
	}
	PG_END_TRY();
	span_end_time = GetCurrentTimestamp();
	end_top_span(&span_end_time, NULL);
	exec_nested_level--;

	/*
	 * Same as above, tracing could have been aborted, check for
	 * current_trace_spans
	 */
	if (current_trace_spans != NULL && executor_sampled)
	{
		Span	   *executor_run_span = &per_level_buffers[exec_nested_level].executor_run;

		end_span(executor_run_span, &span_end_time);
		store_span(executor_run_span);
	}
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth.
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span.
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	Span		previous_top_span;
	Span	   *top_span;
	TimestampTz span_end_time;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->instrument_options > 0;

	if (executor_sampled)
	{
		TimestampTz span_start_time = GetCurrentTimestamp();

		/* Save previous state of the top span */
		previous_top_span = per_level_buffers[exec_nested_level].top_span;

		begin_span(trace_context->trace_id, &per_level_buffers[exec_nested_level].top_span, SPAN_EXECUTOR_FINISH,
				   NULL, previous_top_span.span_id,
				   per_level_buffers[exec_nested_level].query_id, &span_start_time);
		/* Set a possible start for nested query */
		nested_query_start_time = span_start_time;
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_CATCH();
	{
		span_end_time = GetCurrentTimestamp();
		end_top_span(&span_end_time, NULL);
		exec_nested_level--;

		if (current_trace_spans != NULL && executor_sampled)
			handle_pg_error(trace_context, &previous_top_span, queryDesc, &span_end_time);
		PG_RE_THROW();
	}
	PG_END_TRY();
	span_end_time = GetCurrentTimestamp();
	end_top_span(&span_end_time, NULL);
	exec_nested_level--;

	if (current_trace_spans == NULL || !executor_sampled)
		return;

	top_span = get_top_span_for_nested_level(exec_nested_level);

	/*
	 * We only trace executorFinish when it has a nested query, check if we
	 * have a possible child
	 */
	if (max_nested_level > exec_nested_level)
	{
		Span	   *nested_span = get_top_span_for_nested_level(exec_nested_level + 1);

		/* Check if the child matches ExecutorFinish's id */
		if (nested_span->parent_id == top_span->span_id)
		{
			/* ExecutorFinish has a child span, end and store it */
			end_top_span(NULL, &previous_top_span);
			return;
		}
	}
	/* Restore previous top span */
	*top_span = previous_top_span;
}

/*
 * ExecutorEnd hook will:
 * - process queryDesc to generate span from planstate
 * - end top span for the current nested level
 * - end tracing if we're at the root nested level
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	bool		tracing_enabled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->instrument_options > 0;

	if (tracing_enabled)
		process_query_desc(trace_context, queryDesc, 0, NULL);

	exec_nested_level++;
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
	exec_nested_level--;

	if (tracing_enabled)
	{
		TimestampTz span_end_time = GetCurrentTimestamp();
		bool		overlapping_trace_context;
		Span	   *top_span = get_top_span_for_nested_level(exec_nested_level);

		/*
		 * We may go through multiple statement nested in a single
		 * ExecutorFinish (multiple triggers) so we need to update the
		 * nested_start.
		 */
		nested_query_start_time = span_end_time;

		/* End top span */
		end_span(top_span, &span_end_time);

		/*
		 * if root trace context has a different root span index, it means
		 * that we may have started to trace the next statement while still
		 * processing the current one. This may happen with a transaction
		 * block using extended protocol, the parsing of the next statement
		 * happens before the ExecutorEnd of the current statement. In this
		 * case, we can't end tracing as we still have unfinished spans.
		 */
		overlapping_trace_context = exec_nested_level == 0 &&
			current_trace_context.root_span.span_id != root_trace_context.root_span.span_id && root_trace_context.sampled;
		if (overlapping_trace_context)
			store_span(top_span);
		else
			end_tracing(trace_context);
	}
}

/*
 * ProcessUtility hook
 *
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 * Process utility may create nested queries (for example function CALL) so we need
 * to set the ProcessUtility span as the top span before going through the standard
 * codepath.
 */
static void
pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	Span	   *process_utility_span;
	Span	   *top_span;
	Span		previous_top_span;
	TimestampTz span_end_time;
	TimestampTz span_start_time;

	/*
	 * Save track utility value since this value could be modified by a SET
	 * command
	 */
	bool		track_utility = pg_tracing_track_utility;
	pgTracingTraceContext *trace_context = &current_trace_context;

	if (exec_nested_level + plan_nested_level == 0)
	{
		/* We're at root level, copy the root trace_context */
		*trace_context = root_trace_context;
	}

	/*
	 * Save aborted state at the before executing utility. If utility executed
	 * is a rollback, aborted state will be reset. However, we can't send
	 * async notification since we've started in an aborted state.
	 */
	trace_context->in_aborted_transaction = IsAbortedTransactionBlockState();

	if (!track_utility || !pg_tracing_enabled(trace_context, exec_nested_level))
	{
		/*
		 * When utility tracking is disabled, we want to avoid tracing nested
		 * queries created by the utility statement. Incrementing the nested
		 * level will avoid starting a new nested trace while the root utility
		 * wasn't traced
		 */
		if (!track_utility)
			exec_nested_level++;

		/* No sampling, just go through the standard process utility */
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
		if (!track_utility)
			exec_nested_level--;

		/*
		 * Tracing may have been started within the utility call without going
		 * through ExecutorEnd (ex: prepare statement). Check and end tracing
		 * here in this case.
		 */
		if (!pg_tracing_mem_ctx->isReset && exec_nested_level == 0)
		{
			Assert(current_trace_context.sampled || root_trace_context.sampled);
			Assert(exec_nested_level == 0);
			end_tracing(trace_context);
		}
		return;
	}

	/* Statement is sampled */
	span_start_time = GetCurrentTimestamp();

	/*
	 * Set a possible start for nested query. ProcessUtility may start a
	 * nested query and we can't use the statement start like we usually do.
	 * We artificially start nested 1us later to avoid overlapping spans.
	 */
	nested_query_start_time = span_start_time;

	top_span = initialize_trace_level_and_top_span(trace_context, pstmt->commandType,
												   NULL, NULL, pstmt, queryString, span_start_time, false);

	/*
	 * We need to set the process utility as the top span, save the previous
	 * top span
	 */
	previous_top_span = *top_span;

	Assert(!traceid_zero(previous_top_span.trace_id));
	process_utility_span = get_top_span_for_nested_level(exec_nested_level);

	/* Build the process utility span. */
	begin_span(trace_context->trace_id, process_utility_span, SPAN_PROCESS_UTILITY,
			   NULL, get_parent_id(trace_context, exec_nested_level),
			   per_level_buffers[exec_nested_level].query_id,
			   &span_start_time);
	Assert(previous_top_span.start <= process_utility_span->start);

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
	}
	PG_CATCH();
	{
		/*
		 * Tracing may have been reset within processUtility, check for
		 * current_trace_spans
		 */
		if (current_trace_spans != NULL)
		{
			span_end_time = GetCurrentTimestamp();
			end_top_span(&span_end_time, NULL);
			exec_nested_level--;
			handle_pg_error(trace_context, &previous_top_span, NULL, &span_end_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Same as above, abort if tracing was disabled within process utility */
	if (current_trace_spans == NULL)
	{
		exec_nested_level--;
		return;
	}
	span_end_time = GetCurrentTimestamp();
	end_top_span(&span_end_time, NULL);
	exec_nested_level--;

	/* buffer may have been repalloced, grab a fresh pointer */
	process_utility_span = get_top_span_for_nested_level(exec_nested_level);
	if (qc != NULL)
		process_utility_span->node_counters.rows = qc->nprocessed;

	/* End process utility span */
	end_top_span(&span_end_time, &previous_top_span);
	/* End top span that was restored */
	end_top_span(&span_end_time, NULL);

	end_tracing(trace_context);

	/*
	 * We may iterate through a list of nested ProcessUtility calls, update
	 * the nested query start.
	 */
	nested_query_start_time = span_end_time;
}

/*
 * Iterate over a list of planstate to generate span node
 */
static TimestampTz
generate_member_nodes(PlanState **planstates, int nplans, planstateTraceContext * planstateTraceContext, uint64 parent_id,
					  TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end)
{
	int			j;
	TimestampTz child_end;

	for (j = 0; j < nplans; j++)
		child_end = generate_span_from_planstate(planstates[j], planstateTraceContext, parent_id, root_end, latest_end);
	return child_end;
}

/*
 * Iterate over custom scan planstates to generate span node
 */
static TimestampTz
generate_span_from_custom_scan(CustomScanState *css, planstateTraceContext * planstateTraceContext, uint64 parent_id,
							   TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end)
{
	ListCell   *cell;
	TimestampTz last_end;

	foreach(cell, css->custom_ps)
		last_end = generate_span_from_planstate((PlanState *) lfirst(cell), planstateTraceContext, parent_id, root_end, latest_end);
	return last_end;
}

/*
 * With nested queries, the post parse hook will be called and we will create the top span before we know
 * it's parent. We will only know and the parent when we generate span nodes from planstate.
 *
 * For all nodes that could generate nested queries, we check if there's a nested top span that could have been
 * created by this node and define it as the top span's parent.
 */
static void
attach_top_span_to_new_parent(uint64 parent_id, TimestampTz parent_start, TimestampTz parent_end)
{
	Span	   *child_top_span;

	if (exec_nested_level + 1 > max_nested_level)
		/* No child to attach */
		return;

	child_top_span = &per_level_buffers[exec_nested_level + 1].top_span;
	/* If child start after parent's end, don't link it */
	if (parent_end < child_top_span->start)
		return;
	/* If child ends before parent's start, don't link it */
	if (child_top_span->end < parent_start)
		return;

	/* Link this child to the new parent */
	child_top_span->parent_id = parent_id;
	/* Readjust top's end */
	child_top_span->end = parent_end;
}

/*
 * Attach an already existing parse span to a new parent.
 *
 * Parse span are created in the post_parse hook but we don't know
 * its parent nor its exact start, we only have a good idea of its end.
 *
 * When processing the queryDesc, we can find possible parent candidate and
 * attach the parse span. We need to adjust the parse span start to match
 * the new parent.
 *
 * The parent node may have another child node (ProjectSet will have a Result
 * outerplan node). The parse span will need to start after this child node.
 */
static void
attach_parse_span_to_new_parent(uint64 parent_id, TimestampTz node_start,
								TimestampTz node_end, TimestampTz new_start_time)
{
	Span	   *parse_span;

	Assert(new_start_time <= node_end);
	if (exec_nested_level + 1 > max_nested_level)
		/* No child to attach */
		return;

	/* Get possible child parse span */
	parse_span = &per_level_buffers[exec_nested_level + 1].parse_span;

	if (traceid_zero(parse_span->trace_id))
		return;

	/* Is parse's end within the node's timeframe? */
	if (parse_span->end < node_start)
		return;
	if (parse_span->end > node_end)
	{
		/*
		 * No match. However, parse span should not overlap with running node
		 * so we can use the node's end as a better estimation of parse's
		 * start.
		 */
		if (parse_span->start < node_end)
			parse_span->start = node_end;
		return;
	}

	/*
	 * Adjust span start. Start time may have already been adjusted by
	 * previous overlapping Result/ProjectSet nodes.
	 */
	if (new_start_time > parse_span->start)
	{
		parse_span->start = new_start_time;
		Assert(parse_span->start < parse_span->end);
	}

	/* Child span should already have ended */
	Assert(parse_span->ended);
	/* Link parse to the new parent */
	parse_span->parent_id = parent_id;
}

/*
 * Get end time for a span node from the provided planstate.
 */
static TimestampTz
get_span_end_from_planstate(PlanState *planstate, TimestampTz root_end)
{
	TimestampTz span_start;
	TimestampTz span_end_time;

	span_start = planstate->instrument->firsttime;

	if (!INSTR_TIME_IS_ZERO(planstate->instrument->starttime) && root_end > 0)

		/*
		 * Node was ongoing but aborted due to an error, use root end as the
		 * end
		 */
		span_end_time = root_end;
	else if (planstate->instrument->total == 0)
	{
		span_end_time = GetCurrentTimestamp();
	}
	else
	{
		span_end_time = span_start + planstate->instrument->total * US_IN_S;

		/*
		 * Since we convert from double seconds to microseconds again, we can
		 * have a span_end_time greater to the root due to the loss of
		 * precision for long durations. Fallback to the root end in this
		 * case.
		 */
		if (span_end_time > root_end)
			span_end_time = root_end;
	}
	return span_end_time;
}

/*
 * Create span node for the provided planstate
 */
static Span
create_span_node(PlanState *planstate, const planstateTraceContext * planstateTraceContext,
				 uint64 *span_id, uint64 parent_id, SpanType span_type,
				 char *subplan_name, TimestampTz span_end_time)
{
	Span		span;
	Plan const *plan = planstate->plan;

	/*
	 * Make sure stats accumulation is done. Function is a no-op if if was
	 * already done.
	 */
	InstrEndLoop(planstate->instrument);

	/* We only create span node on node that were executed */
	Assert(planstate->instrument->firsttime != 0);

	begin_span(planstateTraceContext->trace_id, &span, span_type, span_id, parent_id,
			   per_level_buffers[exec_nested_level].query_id,
			   &planstate->instrument->firsttime);

	/* first tuple time */
	span.startup = planstate->instrument->startup * NS_PER_S;

	if (span_type == SPAN_NODE)
	{
		/* Generate node specific variable strings and store them */
		const char *node_type;
		const char *deparse_info;
		char const *operation_name;
		int			deparse_info_len;

		node_type = plan_to_node_type(plan);
		span.node_type_offset = add_str_to_trace_buffer(current_trace_text, node_type, strlen(node_type));

		operation_name = plan_to_operation(planstateTraceContext, planstate, node_type);
		span.operation_name_offset = add_str_to_trace_buffer(current_trace_text, operation_name, strlen(operation_name));

		/* deparse_ctx us NULL if deparsing was disabled */
		if (planstateTraceContext->deparse_ctx != NULL)
		{
			deparse_info = plan_to_deparse_info(planstateTraceContext, planstate);
			deparse_info_len = strlen(deparse_info);
			if (deparse_info_len > 0)
				span.deparse_info_offset = add_str_to_trace_buffer(current_trace_text, deparse_info, deparse_info_len);
		}
	}
	else if (subplan_name != NULL)
		span.operation_name_offset = add_str_to_trace_buffer(current_trace_text, subplan_name, strlen(subplan_name));

	span.node_counters.rows = (int64) planstate->instrument->ntuples / planstate->instrument->nloops;
	span.node_counters.nloops = (int64) planstate->instrument->nloops;
	span.node_counters.buffer_usage = planstate->instrument->bufusage;
	span.node_counters.wal_usage = planstate->instrument->walusage;

	span.plan_counters.startup_cost = plan->startup_cost;
	span.plan_counters.total_cost = plan->total_cost;
	span.plan_counters.plan_rows = plan->plan_rows;
	span.plan_counters.plan_width = plan->plan_width;

	if (!planstate->state->es_finished)
	{
		/*
		 * If the query is in an unfinished state, it means that we're in an
		 * error handler. Stop the node instrumentation to get the latest
		 * known state.
		 */
		if (!INSTR_TIME_IS_ZERO(planstate->instrument->starttime))
			/* Don't stop the node if it wasn't started */
			InstrStopNode(planstate->instrument, planstate->state->es_processed);
		InstrEndLoop(planstate->instrument);
		span.sql_error_code = planstateTraceContext->sql_error_code;
	}
	Assert(planstate->instrument->total > 0);
	end_span(&span, &span_end_time);

	return span;
}

/*
 * Walk through the planstate tree generating a node span for each node.
 */
static TimestampTz
generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext,
							 uint64 parent_id, TimestampTz root_end, TimestampTz *latest_end)
{
	ListCell   *l;
	Plan const *plan = planstate->plan;
	uint64		span_id;
	Span		span_node;
	TimestampTz span_start;
	TimestampTz span_end_time;
	TimestampTz outer_span_end = 0;

	/* The node was never executed, skip it */
	if (planstate->instrument == NULL || planstate->instrument->firsttime == 0)
		return 0;

	/*
	 * Make sure stats accumulation is done. Function is a no-op if if was
	 * already done.
	 */
	InstrEndLoop(planstate->instrument);

	span_id = pg_prng_uint64(&pg_global_prng_state);
	span_start = planstate->instrument->firsttime;
	span_end_time = get_span_end_from_planstate(planstate, root_end);

	/* Keep track of the last child end */
	if (*latest_end < span_end_time)
		*latest_end = span_end_time;

	planstateTraceContext->ancestors = lcons(planstate->plan, planstateTraceContext->ancestors);

	/* Walk the outerplan */
	if (outerPlanState(planstate))
		outer_span_end = generate_span_from_planstate(outerPlanState(planstate), planstateTraceContext, span_id, root_end, latest_end);
	/* Walk the innerplan */
	if (innerPlanState(planstate))
		generate_span_from_planstate(innerPlanState(planstate), planstateTraceContext, span_id, root_end, latest_end);

	/* Handle init plans */
	foreach(l, planstate->initPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		Span		init_span;
		TimestampTz initplan_span_end;

		if (splan->instrument->firsttime == 0)
			continue;
		initplan_span_end = get_span_end_from_planstate(planstate, root_end);
		init_span = create_span_node(splan, planstateTraceContext, NULL, parent_id, SPAN_NODE_INIT_PLAN, sstate->subplan->plan_name, initplan_span_end);
		store_span(&init_span);
		generate_span_from_planstate(splan, planstateTraceContext, init_span.span_id, root_end, latest_end);
	}

	/* Handle sub plans */
	foreach(l, planstate->subPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		Span		subplan_span;
		TimestampTz subplan_span_end;

		if (splan->instrument->firsttime == 0)
			continue;
		subplan_span_end = get_span_end_from_planstate(planstate, root_end);
		subplan_span = create_span_node(splan, planstateTraceContext, NULL, parent_id, SPAN_NODE_SUBPLAN, sstate->subplan->plan_name, subplan_span_end);
		store_span(&subplan_span);
		generate_span_from_planstate(splan, planstateTraceContext, subplan_span.span_id, root_end, latest_end);
	}

	/* Handle special nodes with children nodes */
	switch (nodeTag(plan))
	{
		case T_Append:
			generate_member_nodes(((AppendState *) planstate)->appendplans,
								  ((AppendState *) planstate)->as_nplans, planstateTraceContext, span_id, span_start, root_end, latest_end);
			break;
		case T_MergeAppend:
			generate_member_nodes(((MergeAppendState *) planstate)->mergeplans,
								  ((MergeAppendState *) planstate)->ms_nplans, planstateTraceContext, span_id, span_start, root_end, latest_end);
			break;
		case T_BitmapAnd:
			generate_member_nodes(((BitmapAndState *) planstate)->bitmapplans,
								  ((BitmapAndState *) planstate)->nplans, planstateTraceContext, span_id, span_start, root_end, latest_end);
			break;
		case T_BitmapOr:
			generate_member_nodes(((BitmapOrState *) planstate)->bitmapplans,
								  ((BitmapOrState *) planstate)->nplans, planstateTraceContext, span_id, span_start, root_end, latest_end);
			break;
		case T_SubqueryScan:
			generate_span_from_planstate(((SubqueryScanState *) planstate)->subplan, planstateTraceContext, span_id, root_end, latest_end);
			break;
		case T_CustomScan:
			generate_span_from_custom_scan((CustomScanState *) planstate, planstateTraceContext, span_id, span_start, root_end, latest_end);
			break;
		default:
			break;
	}
	planstateTraceContext->ancestors = list_delete_first(planstateTraceContext->ancestors);

	/* If node had no duration, use the latest end of its child */
	if (planstate->instrument->total == 0)
		span_end_time = *latest_end;

	/*
	 * If we have a Result or ProjectSet node, make it the span parent of the
	 * child query span if we have any
	 *
	 * TODO: Do we have other node that could be the base for nested queries?
	 */
	if (nodeTag(plan) == T_Result || nodeTag(plan) == T_ProjectSet)
	{
		attach_top_span_to_new_parent(span_id, span_start, span_end_time);
		if (nodeTag(plan) == T_ProjectSet)
		{
			/* We should have an outer span with a ProjectSet */
			Assert(outer_span_end > 0);

			/*
			 * A parse may happen within a projectset during function exec.
			 * Try to attach existing child parse to the project set node.
			 */
			attach_parse_span_to_new_parent(span_id, span_start, span_end_time, outer_span_end);
		}
		else
			attach_parse_span_to_new_parent(span_id, span_start, span_end_time, span_start);
	}

	/*
	 * With a before trigger, the node (update/insert/delete/truncate) should
	 * become the parent for the nested parse and top span.
	 */
	if (nodeTag(plan) == T_ModifyTable)
	{
		attach_top_span_to_new_parent(span_id, span_start, span_end_time);
		attach_parse_span_to_new_parent(span_id, span_start, span_end_time, span_start);
	}

	span_node = create_span_node(planstate, planstateTraceContext, &span_id, parent_id, SPAN_NODE, NULL, span_end_time);
	store_span(&span_node);
	return span_end_time;
}

/*
 * Add plan counters to the Datum output
 */
static int
add_plan_counters(const PlanCounters * plan_counters, int i, Datum *values)
{
	values[i++] = Float8GetDatumFast(plan_counters->startup_cost);
	values[i++] = Float8GetDatumFast(plan_counters->total_cost);
	values[i++] = Float8GetDatumFast(plan_counters->plan_rows);
	values[i++] = Int32GetDatum(plan_counters->plan_width);
	return i;
}

/*
 * Add node counters to the Datum output
 */
static int
add_node_counters(const NodeCounters * node_counters, int i, Datum *values)
{
	Datum		wal_bytes;
	char		buf[256];
	double		blk_read_time,
				blk_write_time,
				temp_blk_read_time,
				temp_blk_write_time;
	double		generation_counter,
				inlining_counter,
				optimization_counter,
				emission_counter;
	int64		jit_created_functions;

	values[i++] = Int64GetDatumFast(node_counters->rows);
	values[i++] = Int64GetDatumFast(node_counters->nloops);

	/* Buffer usage */
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_written);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_written);

	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_write_time);
	temp_blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time);
	temp_blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time);

	values[i++] = Float8GetDatumFast(blk_read_time);
	values[i++] = Float8GetDatumFast(blk_write_time);
	values[i++] = Float8GetDatumFast(temp_blk_read_time);
	values[i++] = Float8GetDatumFast(temp_blk_write_time);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_written);

	/* WAL usage */
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_records);
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_fpi);
	snprintf(buf, sizeof buf, UINT64_FORMAT, node_counters->wal_usage.wal_bytes);

	/* Convert to numeric. */
	wal_bytes = DirectFunctionCall3(numeric_in,
									CStringGetDatum(buf),
									ObjectIdGetDatum(0),
									Int32GetDatum(-1));
	values[i++] = wal_bytes;

	/* JIT usage */
	generation_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.generation_counter);
	inlining_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.inlining_counter);
	optimization_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.optimization_counter);
	emission_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.emission_counter);
	jit_created_functions = node_counters->jit_usage.created_functions;

	values[i++] = Int64GetDatumFast(jit_created_functions);
	values[i++] = Float8GetDatumFast(generation_counter);
	values[i++] = Float8GetDatumFast(inlining_counter);
	values[i++] = Float8GetDatumFast(optimization_counter);
	values[i++] = Float8GetDatumFast(emission_counter);

	return i;
}

/*
 * Build the tuple for a Span and add it to the output
 */
static void
add_result_span(ReturnSetInfo *rsinfo, Span * span,
				const char *qbuffer, Size qbuffer_size)
{
#define PG_TRACING_TRACES_COLS	44
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};
	const char *span_type;
	const char *operation_name;
	const char *sql_error_code;
	int			i = 0;
	char		trace_id[33];
	char		parent_id[17];
	char		span_id[17];

	span_type = get_span_type(span, qbuffer, qbuffer_size);
	operation_name = get_operation_name(span, qbuffer, qbuffer_size);
	sql_error_code = unpack_sql_state(span->sql_error_code);

	pg_snprintf(trace_id, 33, INT64_HEX_FORMAT INT64_HEX_FORMAT,
				span->trace_id.traceid_left,
				span->trace_id.traceid_right);
	pg_snprintf(parent_id, 17, INT64_HEX_FORMAT, span->parent_id);
	pg_snprintf(span_id, 17, INT64_HEX_FORMAT, span->span_id);

	Assert(span_type != NULL);
	Assert(operation_name != NULL);
	Assert(sql_error_code != NULL);

	values[i++] = CStringGetTextDatum(trace_id);
	values[i++] = CStringGetTextDatum(parent_id);
	values[i++] = CStringGetTextDatum(span_id);
	values[i++] = UInt64GetDatum(span->query_id);
	values[i++] = CStringGetTextDatum(span_type);
	values[i++] = CStringGetTextDatum(operation_name);
	values[i++] = Int64GetDatumFast(span->start);
	values[i++] = Int64GetDatumFast(span->end);

	values[i++] = CStringGetTextDatum(sql_error_code);
	values[i++] = UInt32GetDatum(span->be_pid);
	values[i++] = ObjectIdGetDatum(span->user_id);
	values[i++] = ObjectIdGetDatum(span->database_id);
	values[i++] = UInt8GetDatum(span->subxact_count);

	/* Only node and top spans have counters */
	if ((span->type >= SPAN_NODE && span->type <= SPAN_TOP_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		i = add_plan_counters(&span->plan_counters, i, values);
		i = add_node_counters(&span->node_counters, i, values);

		values[i++] = Int64GetDatumFast(span->startup);
		if (span->parameter_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->parameter_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->parameter_offset);
		else
			nulls[i++] = 1;
		if (span->deparse_info_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->deparse_info_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->deparse_info_offset);
	}

	for (int j = i; j < PG_TRACING_TRACES_COLS; j++)
		nulls[j] = 1;

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * Return spans as a result set.
 *
 * Accept a consume parameter. When consume is set,
 * we empty the shared buffer and truncate query text.
 */
Datum
pg_tracing_spans(PG_FUNCTION_ARGS)
{
	bool		consume;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Span	   *span;
	const char *qbuffer;
	Size		qbuffer_size = 0;

	consume = PG_GETARG_BOOL(0);
	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));
	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If this query was sampled and we're consuming tracing_spans buffer, the
	 * spans will target a query string that doesn't exist anymore in the
	 * query file. Better abort the sampling, clean ongoing traces and remove
	 * any possible parallel context pushed. Since this will be called within
	 * an ExecutorRun, we will need to check for current_trace_spans at the
	 * end of the ExecutorRun hook.
	 */
	cleanup_tracing();

	qbuffer = qtext_load_file(&qbuffer_size);
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		for (int i = 0; i < shared_spans->end; i++)
		{
			span = shared_spans->spans + i;
			add_result_span(rsinfo, span, qbuffer, qbuffer_size);
		}

		/* Consume is set, remove spans from the shared buffer */
		if (consume)
		{
			shared_spans->end = 0;
			/* We only truncate query file if there's no active writers */
			if (s->n_writers == 0)
			{
				s->extent = 0;
				pg_truncate(PG_TRACING_TEXT_FILE, 0);
			}
			else
				s->stats.failed_truncates++;
		}
		s->stats.last_consume = GetCurrentTimestamp();
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
#define PG_TRACING_INFO_COLS	6
	pgTracingStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};
	int			i = 0;

	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Get a copy of the pg_tracing stats */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		stats = s->stats;
		SpinLockRelease(&s->mutex);
	}

	values[i++] = Int64GetDatum(stats.traces);
	values[i++] = Int64GetDatum(stats.spans);
	values[i++] = Int64GetDatum(stats.dropped_spans);
	values[i++] = Int64GetDatum(stats.failed_truncates);
	values[i++] = TimestampTzGetDatum(stats.last_consume);
	values[i++] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Get an empty pgTracingStats
 */
static pgTracingStats
get_empty_pg_tracing_stats(void)
{
	pgTracingStats stats;

	stats.traces = 0;
	stats.spans = 0;
	stats.dropped_spans = 0;
	stats.failed_truncates = 0;
	stats.last_consume = 0;
	stats.stats_reset = GetCurrentTimestamp();
	return stats;
}

/*
 * Reset pg_tracing statistics.
 */
Datum
pg_tracing_reset(PG_FUNCTION_ARGS)
{
	/*
	 * Reset statistics for pg_tracing since all entries are removed.
	 */
	pgTracingStats empty_stats = get_empty_pg_tracing_stats();

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->stats = empty_stats;
		SpinLockRelease(&s->mutex);
	}
	PG_RETURN_VOID();
}
