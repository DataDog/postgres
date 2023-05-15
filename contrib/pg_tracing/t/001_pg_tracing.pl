
# Copyright (c) 2023-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Runs the specified query and returns the emitted server log.
# params is an optional hash mapping GUC names to values;
# any such settings are transmitted to the backend via PGOPTIONS.
sub query_log
{
	my ($node, $sql, $params) = @_;
	$params ||= {};

	local $ENV{PGOPTIONS} = join " ",
	  map { "-c $_=$params->{$_}" } keys %$params;

	my $log    = $node->logfile();
	my $offset = -s $log;

	$node->safe_psql("postgres", $sql);

	return slurp_file($log, $offset);
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init('auth_extra' => [ '--create-role', 'regress_user1' ]);
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_tracing'");
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION pg_tracing;");

query_log($node, "SELECT * FROM pg_tracing_spans;");

# Simple query.
my $log_contents = query_log($node, "/*dddbs='postgres.db',traceparent='00-00000000000000007ed39623a1d7b2d2-7ed39623a1d7b2d2-01'*/ SELECT 1;");

like($log_contents,
	qr/trace_id: 9138813148648157906/,
	"Trace id check");

$node->stop('fast');

done_testing();
