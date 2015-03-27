# kinesis-test

Create and destroy Kinesis and Kinesalite streams for use in tape tests

## Usage

```js
var kinesis = require('kinesis-test')('my-project', 1, 'us-east-1');

kinesis.test('my test', function(assert) {
  // creates a stream with 1 shard for you
  assert.end();
  // cleans up the stream after your test is over
});

kinesis.test('another test', function(assert) {
  // new stream!
  assert.end();
});
```

## API

**var kinesis = require('kinesis-test')(projectName, shards, [region])**

Provide an arbitrary `projectName` (used in your stream's name) and number of `shards`. If you specify a `region`, then real-life Kinesis used. If not, then it will work locally using [kinesalite](https://github.com/mhart/kinesalite).

**kinesis.streamName**

Provides the name of your test stream.

**kinesis.kinesis**

Provides an [AWS.Kineis object]() configured to communicate with your stream (real-life or kinesalite).

**kinesis.start()**

Creates a kinesis stream for you to test against.

**kinesis.shards**

Once your stream is created, `shards` will provide you an array of [kinesis-readable](https://github.com/rclark/kinesis-readable) streams, one for each shard.

**kinesis.load(fixtures)**

Loads records into your stream. `fixtures` must be an array of objects which each provde a `Data` and `PartitionKey` property.

**kinesis.delete()**

Deletes the stream.

**kinesis.test(name, [fixtures], callback)**

A wrapper around [tape](https://github.com/substack/tape) that:

- creates a fresh stream
- optionally, loads `fixtures` that you provide
- runs your tests by providing an `assertion` object to your `callback` function
- deletes your stream

**kinesis.close()**

If you're working in a mock test environment, use this call to shut down kinesalite.
