

const _ = require('lodash');
var Select = require('./cql/Select.js');

function toSqlValue (value) {
    if (_.isString(value)) {
        value = value.replace(/'/g, "''");
        return "'" + value + "'";
    } else if (_.isArray(value)) {
        value = _.chain(value)
            .map(toSqlValue)
            .join(', ')
            .value();
        return '[' + value + ']'
    } else if (value instanceof Select) {
        return value.toString();
    } else if (_.isPlainObject(value)) {
        value = _.chain(value)
            .toPairs()
            .map(function (pair) {
                return pair[0] + ' = ' + toSqlValue(pair[1]);
            })
            .join(', ')
            .value();
        return '{' + value + '}';
    } else {
        return JSON.stringify(value).replace(/'/g, "''");
    }
};

module.exports = {
    toSqlValue: toSqlValue
};
