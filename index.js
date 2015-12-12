var crypto = require('crypto');
var AWS = require('aws-sdk');
var _ = require('underscore');
var kinesalite = require('kinesalite')({
  createStreamMs: 1,
  deleteStreaMs: 1,
  ssl: false
});
var queue = require('queue-async');

module.exports = function(test, projectName, shards, region) {
  var live = !!region;

  var options = live ? { region: region } : {
    region: '-',
    accessKeyId: '-',
    secretAccessKey: '-',
    endpoint: 'http://localhost:7654'
  };

  var kinesis = {};

  var client = new AWS.Kinesis(options);
  kinesis.kinesis = client;

  kinesis.streamName = [
    'test',
    projectName,
    crypto.randomBytes(4).toString('hex')
  ].join('-');

  var Readable = require('kinesis-readable').bind(null, client, kinesis.streamName);
  var readables = [];

  var streamRunning = false;

  function start(assert, callback) {
    if (live) assert.timeoutAfter(300000);
    if (streamRunning) return callback();

    function ready(err) {
      if (err) throw err;

      check(client, kinesis.streamName, function(err, status) {
        if (err) throw err;
        if (status !== 'ACTIVE') return setTimeout(ready, 1000);

        streamRunning = true;

        shardids(client, kinesis.streamName, function(err, shardids) {
          if (err) throw err;

          kinesis.shards = shardids.map(function(id) {
            return function(options) {
              var readable = Readable(_({ shardId: id }).extend(options));
              var i = readables.push(readable) - 1;

              readable.on('end', function() {
                readables.splice(i, 1);
              });

              return readable;
            };
          });

          assert.end();
        });
      });
    }

    if (live) return create(client, kinesis.streamName, shards, ready);

    kinesalite.listen(7654, function(err) {
      if (err) throw err;
      create(client, kinesis.streamName, shards, ready);
    });
  }

  kinesis.start = function() {
    test('[kinesis-test] create stream', function(assert) {
      start(assert, function() {
        assert.end();
      });
    });
  };

  kinesis.delete = function() {
    test('[kinesis-test] delete stream', function(assert) {
      if (live) assert.timeoutAfter(300000);
      if (!streamRunning) return assert.end();

      function dead(err) {
        if (err) console.log('delete errored');
        if (err) throw err;

        check(client, kinesis.streamName, function(err, status) {
          if (err) throw err;
          if (status !== 'DOESNOTEXIST') return setTimeout(dead, 1000);
          streamRunning = false;
          assert.end();
        });
      }

      var q = queue();
      readables.forEach(function(shard) {
        q.defer(function(next) {
          shard.close().on('end', next);
        });
      });

      q.awaitAll(function(err) {
        if (err) throw err;
        delete kinesis.shards;
        destroy(client, kinesis.streamName, dead);
      });
    });
  };

  kinesis.load = function(fixtures) {
    test('[kinesis-test] load fixtures', function(assert) {
      if (!streamRunning) start(assert, load);
      else load();

      function load() {
        client.putRecords({
          StreamName: kinesis.streamName,
          Records: fixtures
        }, function(err) {
          if (err) throw err;
          assert.end();
        });
      }
    });
  };

  kinesis.test = function(name, fixtures, callback) {
    kinesis.delete();
    kinesis.start();

    if (typeof fixtures === 'function') {
      callback = fixtures;
      fixtures = null;
    }

    if (fixtures && fixtures.length) kinesis.load(fixtures);

    test(name, callback);
    kinesis.delete();
  };

  if (!live) kinesis.close = function() {
    test('[kinesis-test] close kinesalite', function(assert) {
      kinesalite.close(function(err) {
        if (err) throw err;
        assert.end();
      });
    });
  };

  return kinesis;
};

function create(kinesis, name, shards, callback) {
  kinesis.createStream({
    StreamName: name,
    ShardCount: shards
  }, callback);
}

function destroy(kinesis, name, callback) {
  kinesis.deleteStream({
    StreamName: name
  }, callback);
}

function check(kinesis, name, callback) {
  kinesis.describeStream({
    StreamName: name
  }, function(err, data) {
    if (err && err.code === 'ResourceNotFoundException')
      return callback(null, 'DOESNOTEXIST');
    if (err) {
      console.log('describe errored');
      return callback(err);
    }

    callback(null, data.StreamDescription.StreamStatus);
  });
}

function shardids(kinesis, name, callback) {
  kinesis.describeStream({
    StreamName: name
  }, function(err, data) {
    if (err) return callback(err);
    callback(null, data.StreamDescription.Shards.map(function(shard) {
      return shard.ShardId;
    }));
  });
}
