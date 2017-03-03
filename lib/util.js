

const _ = require('lodash');
// var format = require('pg-format');
var Select = require('./cql/Select.js');
var debug = require('debug')('forklift.util.toSqlValue');

function toSqlValue (value) {
    if (_.isString(value)) {
        debug(`string: ${JSON.stringify(value)}`);
        return "'" + value.replace(/'/g, "''") + "'";
    } else if (_.isDate(value)) {
        debug(`string: ${JSON.stringify(value)}`);
        return "'" + value.toISOString() + "'";
    } else if (_.isArray(value)) {
        debug(`array: ${JSON.stringify(value)}`);
        value = _.chain(value)
            .map(toSqlValue)
            .join(', ')
            .value();
        return '[' + value + ']';
    } else if (value instanceof Select) {
        debug(`Select: ${JSON.stringify(value)}`);
        return value.toString();
    } else if (_.isPlainObject(value)) {
        debug(`object: ${JSON.stringify(value)}`);
        value = _.chain(value)
            .toPairs()
            .map(function (pair) {
                return pair[0] + ' = ' + toSqlValue(pair[1]);
            })
            .join(', ')
            .value();
        return '{' + value + '}';
    } else if (value === undefined) {
        debug(`undefined: ${JSON.stringify(value)}`);
        return 'NULL';
    } else {
        debug(`unknown: ${JSON.stringify(value)}`);
        return JSON.stringify(value);
    }
};

module.exports = {
    toSqlValue: toSqlValue
};
