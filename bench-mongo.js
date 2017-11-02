const MongoClient = require('mongodb').MongoClient;
const bluebird = require('bluebird');
const getopt = require('node-getopt');

const scriptName = require('path').basename(__filename);

var opt_scheme = getopt.create([
	['h', 'host=ARG', 'server hostname. default is localhost. ex> localhost'],
	['d', 'database=ARG', 'database name'],
	['t', 'time=ARG', 'duration (seconds)'],
	['c', 'concurrency=ARG', 'concurrency'],
	['s', 'dataset=ARG', 'document count for benchmark test'],
	['r', 'readOnly', 'skip write test'],
]);
opt_scheme.setHelp('Usage: node ' + scriptName + ' [OPTION]\n\n[[OPTIONS]]\n');

var opt = opt_scheme.parseSystem();

var mongoUri = '';
if (!opt.options.host)
	mongoUri = 'mongodb://localhost';
else
	mongoUri = 'mongodb://' + opt.options.host;

var databaseName = opt.options.database;
if (!databaseName)
	databaseName = 'bench';

mongoUri += '/' + databaseName;

var time = opt.options.time ? opt.options.time : 10;
var concurrency = opt.options.concurrency ? opt.options.concurrency : 10;
var dataset = opt.options.dataset ? opt.options.dataset : 100000;

var db;
var collection;

var main = bluebird.coroutine(function* () {
	db = yield MongoClient.connect(mongoUri);
	collection = db.collection('bench');

	yield collection.drop();
	yield collection.createIndex({ key: 1 }, { unique: true, w: 1 });

	if (!opt.options.readOnly)
		yield* stopwatch('write', testWrite);

	yield* stopwatch('read doc', testRead(function (key) {
		var filter = { _id: key };
		return collection.findOne(filter);
	}));
	yield* stopwatch('read index', testRead(function (key) {
		var filter = { key: key };
		return collection.findOne(filter);
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
			var doc = { _id: key, type: 'testdata', key: key };
			yield collection.insertOne(doc);

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

