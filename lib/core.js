

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
        forklift.config = {
            user: 'doc',
            database: 'doc',
            host: host || '0.0.0.0',
            port: port || 5432,
            Promise: Promise,
            idleTimeoutMillis: 1000
        };
        debug(`initializing connection on ${forklift.config.host}:${forklift.config.port}`);
        forklift.pool = new pg.Pool(forklift.config);
    },

    send: function (statement) {
        debug(`sending SQL: ${statement.toString()}`);
        return forklift.pool.query(statement.toString());
    },

    shutdown: function () {
        debug('closing database connection');
        return forklift.pool.end();
    }
};
