

const _ = require('lodash');
var forklift = require('../');

class Update {
    constructor () {
        this._table = null;
        this._column = null;
        this._value = null;
        this._where = null;
    }

    table (name) {
        this._table = name;
        return this;
    }

    column (name) {
        this._column = name;
        return this;
    }

    value (value) {
        this._value = value;
        return this;
    }

    where (expression) {
        this._where = expression;
        return this;
    }

    toString () {
        var value = forklift.util.toSqlValue(this._value);
        // var stmt = `update ${this._table} set ${this._column} = ${value}`;
        var stmt = 'update ' + this._table + ' set ' + this._column + ' = ' + value;
        if (this._where) {
            // stmt = `${stmt} where ${this._where}`;
            stmt += ' where ' + this._where;
        }
        return stmt;
    }
}

module.exports = Update;
