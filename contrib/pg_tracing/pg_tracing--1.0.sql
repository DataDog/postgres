/* contrib/pg_tracing/pg_tracing--1.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION pg_tracing" to load this file. \quit


--- Define pg_tracing_info
CREATE FUNCTION pg_tracing_info(
    OUT traces bigint,
    OUT spans bigint,
    OUT stats_reset timestamp with time zone
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION pg_tracing_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_tracing_spans(
	OUT trace_id bigint,
	OUT parent_id bigint,
	OUT span_id bigint,
	OUT resource text,
	OUT span_start timestamp with time zone,
	OUT span_end timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW pg_tracing_info AS
  SELECT * FROM pg_tracing_info();

CREATE VIEW pg_tracing_spans AS
  SELECT * FROM pg_tracing_spans();

GRANT SELECT ON pg_tracing_info TO PUBLIC;
GRANT SELECT ON pg_tracing_spans TO PUBLIC;
