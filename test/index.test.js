var test = require('tape');
var crypto = require('crypto');

var project = crypto.randomBytes(4).toString('hex');

var mocked = require('..')(test, project, 1);
var live = require('..')(test, project, 1, 'us-east-1');

test('sets streamName', function(assert) {
  var re = new RegExp('test-' + project + '-[a-zA-Z0-9]{8}');
  assert.ok(re.test(mocked.streamName), 'mocked sets streamName');
  assert.ok(re.test(live.streamName), 'live sets streamName');
  assert.end();
});

test('provides sdk', function(assert) {
  assert.ok(mocked.kinesis, 'provides kinesis');
  assert.end();
});

mocked.start();

test('mocked start', function(assert) {
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.StreamNames, [mocked.streamName], 'creates the stream');
    assert.equal(mocked.shards.length, 1, 'creates a readable stream for the shard');
    assert.end();
  });
});

mocked.load([{ Data: 'hello', PartitionKey: 'a' }]);

test('mocked fixture load', function(assert) {
  assert.plan(3);

  mocked.shards[0]().on('data', function(items) {
    var item = items[0];
    assert.deepEqual(item.Data, new Buffer('hello'), 'expected data');
    assert.equal(item.PartitionKey, 'a', 'expected partition key');
    assert.ok(item.SequenceNumber, 'has sequence number');
  });
});

mocked.delete();

test('mocked delete: readables were not closed explicitly', function(assert) {
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.length, 0, 'deleted the stream');
    assert.notOk(mocked.shards, 'close the streams');
    assert.end();
  });
});

mocked.test('exposes readable streams', [{ Data: 'hello', PartitionKey: 'a' }], function(assert) {
  var readable = mocked.shards[0]()
    .on('data', function(items) {
      var item = items[0];
      assert.deepEqual(item.Data, new Buffer('hello'), 'expected data');
      assert.equal(item.PartitionKey, 'a', 'expected partition key');
      assert.ok(item.SequenceNumber, 'has sequence number');
    }).on('end', function() {
      assert.end();
    });

  setTimeout(function() { readable.close(); }, 200);
});

mocked.delete();

test('mocked delete: after readables were closed explicitly', function(assert) {
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.length, 0, 'deleted the stream');
    assert.notOk(mocked.shards, 'close the streams');
    assert.end();
  });
});

mocked.test('mocked test', function(assert) {
  assert.pass('runs the test');
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.StreamNames, [mocked.streamName], 'creates the stream');
    assert.equal(mocked.shards.length, 1, 'creates a readable stream for the shard');
    assert.end();
  });
});

test('mocked test cleanup', function(assert) {
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.length, 0, 'deleted the stream');
    assert.notOk(mocked.shards, 'close the streams');
    assert.end();
  });
});

mocked.test('mocked test with fixtures', [{ Data: 'hello', PartitionKey: 'a' }], function(assert) {
  assert.pass('runs the test');
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.StreamNames, [mocked.streamName], 'creates the stream');
    assert.equal(mocked.shards.length, 1, 'creates a readable stream for the shard');
    mocked.shards[0]().on('data', function(items) {
      var item = items[0];
      assert.deepEqual(item.Data, new Buffer('hello'), 'expected data');
      assert.equal(item.PartitionKey, 'a', 'expected partition key');
      assert.ok(item.SequenceNumber, 'has sequence number');
      assert.end();
    });
  });
});

test('mocked test with fixtures cleanup', function(assert) {
  mocked.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.length, 0, 'deleted the stream');
    assert.notOk(mocked.shards, 'close the streams');
    assert.end();
  });
});

mocked.close();

test('mocked close kinesalite', function(assert) {
  assert.end();
});

live.start();

test('live start', function(assert) {
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.ok(data.StreamNames.indexOf(live.streamName), 'creates the stream');
    assert.equal(live.shards.length, 1, 'creates a readable stream for the shard');
    assert.end();
  });
});

live.load([{ Data: 'hello', PartitionKey: 'a' }]);

test('live fixture load', function(assert) {
  assert.plan(3);

  live.shards[0].on('data', function(items) {
    var item = items[0];
    assert.deepEqual(item.Data, new Buffer('hello'), 'expected data');
    assert.equal(item.PartitionKey, 'a', 'expected partition key');
    assert.ok(item.SequenceNumber, 'has sequence number');
  });
});

live.delete();

test('live delete', function(assert) {
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.indexOf(live.streamName), -1, 'deleted the stream');
    assert.notOk(live.shards, 'close the streams');
    assert.end();
  });
});

live.test('live test', function(assert) {
  assert.pass('runs the test');
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.ok(data.StreamNames.indexOf(live.streamName), 'creates the stream');
    assert.equal(live.shards.length, 1, 'creates a readable stream for the shard');
    assert.end();
  });
});

test('live test cleanup', function(assert) {
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.indexOf(live.streamName), -1, 'deleted the stream');
    assert.notOk(live.shards, 'close the streams');
    assert.end();
  });
});

live.test('live test with fixtures', [{ Data: 'hello', PartitionKey: 'a' }], function(assert) {
  assert.pass('runs the test');
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.ok(data.StreamNames.indexOf(live.streamName), 'creates the stream');
    assert.equal(live.shards.length, 1, 'creates a readable stream for the shard');
    live.shards[0].on('data', function(items) {
      var item = items[0];
      assert.deepEqual(item.Data, new Buffer('hello'), 'expected data');
      assert.equal(item.PartitionKey, 'a', 'expected partition key');
      assert.ok(item.SequenceNumber, 'has sequence number');
      assert.end();
    });
  });
});

test('live test with fixtures cleanup', function(assert) {
  live.kinesis.listStreams({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.StreamNames.indexOf(live.streamName), -1, 'deleted the stream');
    assert.notOk(live.shards, 'close the streams');
    assert.end();
  });
});

live.delete();
