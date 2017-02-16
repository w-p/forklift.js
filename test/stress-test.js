// setup
var _ = require('lodash');
var uuid = require('uuid/v4');
var Promise = require('bluebird');

var forklift = require('../');
var cql = forklift.cql;
forklift.connect();

var count = 10000;

function getLogData (count) {
    var schema = {
        id: new cql.Column('id').type('string').primary(),
        timestamp: new cql.Column('timestamp').type('timestamp'),
        value: new cql.Column('value').type('string')
    };
    var data = _.times(count, function () {
        return {
            id: uuid(),
            timestamp: Date.now(),
            value: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'
        };
    });
    return {table: 'StressTestLog', schema: schema, data: data};
}

function getMetricData (count) {
    var schema = {
        id: new cql.Column('id').type('string').primary(),
        timestamp: new cql.Column('timestamp').type('timestamp'),
        value: new cql.Column('value').type('integer')
    };
    var data = _.times(count, function () {
        return {
            id: uuid(),
            timestamp: Date.now(),
            value: Math.random() * (100 - 1) + 1
        };
    });
    return {table: 'StressTestMetric', schema: schema, data: data};
}

function getArrayData (count) {
    var schema = {
        id: new cql.Column('id').type('string').primary(),
        timestamp: new cql.Column('timestamp').type('timestamp'),
        value: new cql.Column('value').type('array(string)')
    };
    var data = _.times(count, function () {
        return {
            id: uuid(),
            timestamp: Date.now(),
            value: ['foo', 'bar', 'baz']
        };
    });
    return {table: 'StressTestArray', schema: schema, data: data};
}

function getObjectData (count) {
    var schema = {
        id: new cql.Column('id').type('string').primary(),
        timestamp: new cql.Column('timestamp').type('timestamp'),
        value: new cql.Column('value').type('object')
    };
    var data = _.times(count, function () {
        return {
            id: uuid(),
            timestamp: Date.now(),
            value: {foo: 'bar', baz: 'bah'}
        };
    });
    return {table: 'StressTestObject', schema: schema, data: data};
}

var tests = [
    getLogData(count),
    getArrayData(count),
    getMetricData(count),
    getObjectData(count)
];

var testStart = Date.now();
console.log('starting bulk tests with ', count, ' records');

var results = _.map(tests, function (test) {
    var table = new cql.Table(test.table)
        .create()
        .columns(test.schema);
    var insert = new cql.Insert()
        .into(test.table)
        .bulk(test.data);
    var start = 0;

    return forklift.send(table)
        .then(function (res) {
            start = Date.now();
            return forklift.send(insert);
        })
        .then(function (res) {
            var end = Date.now();
            var total = end - start;
            console.log(test.table);
            console.log(' - insert start:', start);
            console.log(' - insert end:  ', end)
            console.log(' - total time:  ', total / 1000, 's');
            table.drop();
            return forklift.send(table);
        })
        .then(function (res) {
        })
        .catch(function (err) {
            if (err.errorCode === 4093) {
                table.drop();
                return forklift.send(table).then(function () {
                    console.log(test.table, 'duplicate dropped');
                });
            } else {
                throw err;
            }
        });
});

// console.log('starting serial tests with ', count, ' records');

Promise.all(results).then(function () {
    var testEnd = Date.now();
    var testTime = testEnd - testStart;
    console.log('finished');
    console.log(' total time:         ', testTime / 1000, 's');
    console.log(' tables created:     ', results.length);
    console.log(' rows written (bulk):', results.length * count);
});
