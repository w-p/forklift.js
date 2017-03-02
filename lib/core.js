

var pg = require('pg').native;
var Promise = require('bluebird');
var debug = require('debug')('forklift.core');

var forklift = module.exports = {

    pool: null,
    config: null,

    connect: function (ip, port) {
        forklift.initialize(ip, port);
    },

    initialize: function (host, port) {
        var config = {
            user: 'doc',
            database: 'doc',
            host: host || '0.0.0.0',
            port: port || 5432,
            Promise: Promise,
            idleTimeoutMillis: 1000
        };
        debug(`initializing connection on ${config.host}:${config.port}`);
        forklift.config = config;
        forklift.pool = new pg.Pool(config);
    },

    send: function (statement) {
        debug(`sending SQL: ${statement.toString()}`);
        return forklift.pool.query(statement.toString());
    },

    sendAndRefresh: function (statement, table) {
        return forklift.send(statement).then(function (res) {
            table = table || statement._table
            return forklift.send('refresh table ' + table).then(function () {
                return res;
            });
        });
    },

    shutdown: function () {
        debug('closing database connection');
        return forklift.pool.end();
    }
};
