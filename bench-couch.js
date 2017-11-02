const couchbase = require('couchbase');
const bluebird = require('bluebird');
const getopt = require('node-getopt');

const scriptName = require('path').basename(__filename);

var opt_scheme = getopt.create([
	['h', 'host=ARG+', 'server hostnames. default is localhost. ex> localhost'],
	['b', 'bucket=ARG', 'bucket name'],
	['t', 'time=ARG', 'duration (seconds)'],
	['c', 'concurrency=ARG', 'concurrency'],
	['s', 'dataset=ARG', 'document count for benchmark test'],
	['r', 'readOnly', 'skip write test'],
]);
opt_scheme.setHelp('Usage: node ' + scriptName + ' [OPTION]\n\n[[OPTIONS]]\n');

var opt = opt_scheme.parseSystem();

var couchbaseUri = '';
if (!opt.options.host || opt.options.host.length <= 0)
	couchbaseUri = 'couchbase://localhost';
else {
	opt.options.host.forEach(function (s) {
		couchbaseUri += 'couchbase://' + s + ',';
	});
}
var bucketName = opt.options.bucket;
if (!bucketName)
	bucketName = 'default';

const cluster = new couchbase.Cluster(couchbaseUri);
const bucket = bluebird.promisifyAll(cluster.openBucket(bucketName));
const bucketManager = bluebird.promisifyAll(bucket.manager());

var time = opt.options.time ? opt.options.time : 10;
var concurrency = opt.options.concurrency ? opt.options.concurrency : 10;
var dataset = opt.options.dataset ? opt.options.dataset : 100000;

var main = bluebird.coroutine(function* () {
	yield bucketManager.flushAsync();

	if (!opt.options.readOnly)
		yield* stopwatch('write', testWrite);

	yield* stopwatch('read doc', testRead(function (key) { return bucket.getAsync(key); }));
	yield* stopwatch('read index', testRead(function (key) {
		/*  CREATE INDEX `default_key` ON `default`(`key`) */
		var query = couchbase.N1qlQuery.fromString('select * from default where `key` = ' + key);
		return bucket.queryAsync(query);
	}));
	yield* stopwatch('read view', testRead(function (key) {
		/* default.key -> emit(doc.key, doc.type) */
		var query = couchbase.ViewQuery.from('default', 'key').key(key).reduce(false).limit(1);
		return bucket.queryAsync(query);
	}));
});

function* stopwatch(name, funcGen) {

	var startTime = Date.now();
	var testCount = yield* funcGen();
	var endTime = Date.now();

	var elapsed = endTime - startTime;
	var speed = (elapsed > 0) ? Math.round(testCount * 1000 / elapsed) : 'NaN';

	console.log(name + ': ' + speed + '/sec');
}

function randomKey() {
	return Math.floor(Math.random() * dataset).toString();
}

function* testWrite() {

	var stop = false;
	
	var func = function* (startIdx, endIdx) {
		while (!stop) {
			var key = (startIdx++).toString();
			var doc = { type: 'testdata', key: key };
			yield bucket.insertAsync(key, doc, { expiry: 3600 * 24 });

			if (startIdx >= endIdx)
				break;
		}
		
	}

	var divideset = Math.ceil(dataset / concurrency);
	var startIdx = 0;

	var jobs = [];
	for (var i = 0; i < concurrency; ++i) {
		var endIdx = startIdx + divideset;
		if (endIdx > dataset)
			endIdx = dataset;

		jobs.push(bluebird.coroutine(func)(startIdx, endIdx));

		startIdx = endIdx;
	}

	setTimeout(function() { stop = true; }, time * 1000);
	yield bluebird.all(jobs);

	return dataset;
}

function testRead(readFunction) {
	
	return function* () {
		var stop = false;
		var testCount = 0;
		
		var func = function* () {
			while (!stop) {
				var key = randomKey();
				yield readFunction(key);
				testCount++;
			}
			
		}

		var jobs = [];
		for (var i = 0; i < concurrency; ++i) {
			jobs.push(bluebird.coroutine(func)());
		}

		setTimeout(function() { stop = true; }, time * 1000);
		yield bluebird.all(jobs);

		return testCount;
	}
}

main()
.then(function () {
	console.log('completed.');
	process.exit(0);
})
.catch(function (e) {
	console.log(e.stack);
	process.exit(1);
});

