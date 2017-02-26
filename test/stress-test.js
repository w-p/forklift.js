
var _ = require('lodash');
var uuid = require('uuid/v4');
var Promise = require('bluebird');

var forklift = require('../');
var cql = forklift.cql;

process.on("unhandledRejection", function(reason, promise) {
    console.log('REJECTED:', reason);
});

process.on("rejectionHandled", function(promise) {
    console.log('REJECTION HANDLED:', reason);
});

function getMem (type='heapUsed') {
    return (process.memoryUsage()[type] / 1024 / 1024).toFixed(3);
}

function buildData (table, type, count) {
    return {
        table: table,
        schema: [
            new cql.Column('id').type('string').primary(),
            new cql.Column('timestamp').type('timestamp'),
            new cql.Column('value').type(type)
        ],
        rows: _.times(count, function () {
            var value = '';
            if (type === 'string') {
                value = 'Lorem ipsum dolor sit amet, consectetur adip...';
            } else if (type === 'integer') {
                value = Math.random() * (100 - 1) + 1;
            } else if (type === 'array(string)') {
                value = ['foo', 'bar', 'baz'];
            } else if (type === 'object') {
                value = {foo: 'bar', baz: 'bah'};
            } else {
                throw Error('Invalid data type provided');
            }
            return {
                id: uuid(),
                timestamp: Date.now(),
                value: value
            }
        })
    };
}

// number of rows to create
var count = 100000;
// shards in cluster
var shards = 6;
// replicas in cluster
var replicas = 1;

// data type - un/comment to switch
var data = buildData('strings', 'string', count);
// var data = buildData('integers', 'integer', count);
// var data = buildData('arrays', 'array(string)', count);
// var data = buildData('objects', 'object', count);

var table = new cql.Table(data.table)
    .columns(...data.schema)
    .clusterShards(shards)
    .with({number_of_replicas: replicas});

forklift.connect();

function fire (chunksize) {
    var startTime = Date.now();
    var maxMem = getMem();
    var chunks = _.chunk(data.rows, chunksize);
    return forklift.send(table.create()).then(function () {
        start = Date.now();
        return Promise.all(_.map(chunks, function (chunk) {
            var mem = getMem();
            maxMem = mem > maxMem ? mem : maxMem;
            return forklift.send(
                new cql.Insert().into(data.table).data(chunk)
            );
        }));
    }).then(function () {
        var ms = Date.now() - startTime;
        var avg_ms = (ms / data.rows.length).toFixed(4);
        var mem = getMem();
        maxMem = mem > maxMem ? mem : maxMem;
        console.log(`* chunk: ${chunksize}  time: ${ms} ms  avg/row: ${avg_ms} ms  max mem: ${maxMem} mb`);
        return forklift.send(table.drop());
    });
}

console.log(`rows:     ${data.rows.length}`);
console.log(`shards:   ${shards}`);
console.log(`replicas: ${replicas}\n`);

fire(1).then(function () {
    return fire(1000);
}).then(function () {
    return fire(2000);
}).then(function () {
    return fire(3000);
}).then(function () {
    return fire(4000);
}).then(function () {
    return fire(5000);
}).then(function () {
    return fire(6000);
}).then(function () {
    return fire(7000);
}).then(function () {
    return fire(8000);
}).then(function () {
    return fire(9000);
}).then(function () {
    return fire(10000);
});
