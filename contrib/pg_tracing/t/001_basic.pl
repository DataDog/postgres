
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;


my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;

$node->append_conf('postgresql.conf',
	qq{shared_preload_libraries = 'pg_tracing'
	pg_tracing.notify_channel = pg_tracing_notify
	pg_tracing.notify_threshold = 0
	pg_tracing.sample_rate = 1.0
	pg_tracing.track_utility = off
	});
$node->start;

# setup
$node->safe_psql("postgres", "CREATE EXTENSION pg_tracing;");

my $result =
	$node->safe_psql("postgres", "LISTEN pg_tracing_notify; SELECT 1;");
like($result, qr/Asynchronous notification "pg_tracing_notify" received from server process with PID \d+\.$/, 'pg_tracing notification channel');

$node->stop;

done_testing();
