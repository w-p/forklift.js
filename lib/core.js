

var pg = require('pg').native;
var Promise = require('bluebird');
var debug = require('debug')('forklift');

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
        debug('opening database connection');
        forklift.pool = new pg.Pool(forklift.config);
    },

    send: function (statement) {
        debug(statement.toString());
        return forklift.pool.query(statement.toString());
    },

    shutdown: function () {
        debug('closing database connection');
        return forklift.pool.end();
    }
};
