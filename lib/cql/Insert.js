

const _ = require('lodash');
var forklift = require('../');
var Select = require('./Select.js');

class Insert {
    constructor () {
        this._table = null;
        this._columns = [];
        this._values = [];
        this._duplicateKey = null;
    }

    into (table) {
        this._table = table;
        return this;
    }

    data (rows) { // objects (key value pairs)
        if (!_.isArray(rows)) rows = [rows];
        var columns = _.keys(rows[0]);
        var values = _.map(rows, function (row) {
            return _.map(columns, function (column) {
                return row[column];
            });
        });
        this.columns(...columns);
        this.values(...values);
        return this;
    }

    columns () {
        this._columns = _.values(arguments);
        return this;
    }

    values () { // values (array of arrays or sql select statements)
        this._values = _.values(arguments);
        return this;
    }

    onDuplicateKey (raw) { // raw sql
        this._duplicateKey = raw;
        return this;
    }

    toString () {
        var columns = this._columns.join(', ');
        var values = _.chain(this._values)
            .map(function (value) {
                return _.map(value, forklift.util.toSqlValue);
            })
            .join('), (')
            .value();
        var stmt = 'insert into ' + this._table + ' (' + columns + ') values (' + values + ')';
        if (_.includes(stmt, 'select')) {
            stmt = stmt.replace('values ', '');
        }
        if (this._duplicateKey) {
            stmt += ' on duplicate key update ' + this._duplicateKey;
        }
        return stmt;
    }
};

module.exports = Insert;
