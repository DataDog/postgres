CREATE EXTENSION pg_tracing;
SELECT pg_tracing_reset();


SELECT traces from pg_tracing_info;

/*dddbs='postgres.db',traceparent='00-00000000000000007ed39623a1d7b2d2-7ed39623a1d7b2d2-01'*/ SELECT 1;
SELECT trace_id, parent_id, resource from pg_tracing_spans;
