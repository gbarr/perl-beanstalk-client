
use Test::More tests => 6;

use_ok('Beanstalk::Stats');
use_ok('Beanstalk::Job');
use_ok('Beanstalk::Client');

my $client = Beanstalk::Client->new;

ok($client,"Create client");

unless ($client->connect) {
SKIP: {
    skip("Need local beanstalkd server running", 2);
  }
  exit(0);
}

my $job = $client->put({ priority => 12, ttr => 123, data => 'foobar', delay => 3600 });
ok($job);

my $tube = $client->list_tube_used;
$client->watch_only($tube);

is($client->kick_job($job->id), 1, 'kick_job');

