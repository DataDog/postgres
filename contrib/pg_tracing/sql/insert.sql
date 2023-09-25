-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE TABLE IF NOT EXISTS pg_tracing_test_table_with_constraint (a int, b char(20), CONSTRAINT PK_tracing_test PRIMARY KEY (a));

-- Check create statement spans
SELECT name, resource from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, resource;

-- Simple insertion
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');

-- Check insert spans
SELECT name, resource from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, resource;

-- Trigger constraint violation
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');

-- Check violation spans
SELECT name, resource, sql_error_code from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, resource;
