
use Test::More tests => 16;

use_ok('Beanstalk::Stats');
use_ok('Beanstalk::Job');
use_ok('Beanstalk::Client');

my $client = Beanstalk::Client->new;

ok($client,"Create client");

unless ($client->connect) {
SKIP: {
    skip("Need local beanstalkd server running", 11);
  }
  exit(0);
}

is(
  $client->list_tube_used,
  'default',
  "Using default tube"
);

is(
  $client->ignore('default'),
  undef,
  "Must watch a tube"
);

isa_ok(
  $client->stats_tube('default'),
  'Beanstalk::Stats',
  "Fetch tube stats"
);

my $yaml = <<YAML;
--- 1
--- 2
--- 
- 3
- 4
YAML

test_encoding($client, "YAML", $yaml, 1,2,[3,4]);
SKIP: {
  skip("Need JSON::XS", 4) unless eval { require JSON::XS };
  my $json_client = Beanstalk::Client->new(
    { encoder => sub { JSON::XS::encode_json(\@_) },
      decoder => sub { @{JSON::XS::decode_json(shift)} },
    }
  );
  test_encoding($json_client, "JSON", "[1,2,[3,4]]", 1,2,[3,4]);
}

# test priority override
$client->priority(9000);
my $job = $client->put({priority => 9001}, "foo");
$job = $job->peek;
is(9001, $job->priority, "got the expected priority");

sub test_encoding {
  my $client = shift;
  my $type = shift;
  my $data = shift;
  my @args = @_;

  $client->use("json_test");

  my $job = $client->put({},@args);
  is(
    $job->data,
    $data,
    "$type encoding"
  );
  is_deeply(
    [ $job->args ],
    \@args,
    "$type decoding"
  );
  $job = $job->peek;
  is(
    $job->data,
    $data,
    "$type encoding"
  );
  is_deeply(
    [ $job->args ],
    \@args,
    "$type decoding"
  );
}
