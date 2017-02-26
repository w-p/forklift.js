
const _ = require('lodash');
const uuid = require('uuid/v4');
const Promise = require('bluebird');
const request = require('request-promise')
    .defaults({'Connection': 'keep-alive'});

var forklift = module.exports = {

    // Holds connection information.
    cluster: {
        current: '',
        nodes: []
    },

    delay: 0,
    waiting: false,

    _uri: function (endpoint) {
        return 'http://' + endpoint + '/_sql';
    },

    _addNode: function (endpoint) {
        var node = this._uri(endpoint);
        if (!_.includes(this.cluster.nodes, node)) {
            this.cluster.nodes.push(node);
        }
        return node;
    },

    /**
     * Creates a 'connection' to a Crate datebase.
     * @param {string} ip IP address of the server
     * @param {number} port Port number of the Crate HTTP API
     */
    connect: function (ip, port) {
        // lib should be connecting via localhost, but...
        var endpoint = (ip || '0.0.0.0') + ':' + (port || 4200);
        var node = this._addNode(endpoint);
        this.cluster.current = node;
    },

    /**
     * Queries the cluster for all nodes, adding them to forklift.cluster.
     * @return {promise}
     */
    nodes: function () {
        var self = this;
        var stmt = 'select hostname, rest_url from sys.nodes';
        return this.send(stmt)
            .then(function (res) {
                _.forEach(res.body, function (node) {
                    self._addNode(node.rest_url);
                });
                return self.cluster.nodes;
            });
    },

    /**
     * Sends an SQL statement to Crate.
     * @param {string} statement A constructed statement object or a literal
     * statement string
     * @param {array} args An array of arguments used in string replacement
     * ('?') in statements
     * @param {array} bulk_args An array of arrays of arguments used in string
     * replacement ('?') in statements
     * @return {promise}
     */
    send: function (statement, args, bulk_args) {
        if (_.has(statement, 'toStatement')) {
            statement = statement.toStatement();
        }
        var body = {
            stmt: statement
        };

        // not necessary due to statement construction
        //
        // if (args) {
        //     body.args = args;
        // } else if (bulk_args) {
        //     body.bulk_args = bulk_args;
        // }

        var options = {
            method: 'POST',
            uri: this.cluster.current,
            json: true,
            body: body,
            resolveWithFullResponse: true
        };

        function reformResponse (res) {
            return {
                responseCode: res.statusCode,
                message: res.statusMessage,
                data: _.map(res.body.rows, function (row) {
                    return _.zipObject(res.body.cols, row);
                })
            };
        };

        function reformError (err) {
            try {
                var innerError = err.error.error;
                var message = innerError.message + ': ' + statement;
                var data = {
                    responseCode: err.statusCode,
                    errorCode: innerError.code,
                    errorMessage: innerError.message,
                    requestOptions: options
                };
                return new forklift.SendError(message, data);
            } catch (e) {
                console.log(err);
                return e;
            }
        };

        function sendRequest (retries) {
            if (retries > 3) throw Error('Maximum retries reached.')

            return Promise.delay(retries * 1000)
            .then(function () {
                return request(options);
            })
            .then(function (res) {
                return reformResponse(res);
            })
            .catch(function (err) {
                err = reformError(err);
                if (_.includes(err.message, 'EsRejectedExecutionException')) {
                    return sendRequest(retries + 1);
                }
            })
        }

        return sendRequest(0);
    },

    /**
     * Creates a SendError representing an error response from Crate.
     * @class
     * @param {string} message An error message
     * @param {object} data An error data object
     * @param {number} data.responseCode HTTP response code
     * @param {number} data.errorCode Crate error code
     * @param {object} data.sent The data sent to Crate
     */
    SendError: function (message, data) {
        this.constructor.prototype.__proto__ = Error.prototype;
        Error.captureStackTrace(this, this.constructor);
        this.name = 'SendError';
        this.message = message;
        this.responseCode = data.responseCode;
        this.errorCode = data.errorCode;
        this.errorMessage = data.errorMessage;
        this.requestOptions = data.requestOptions;
    }

};
