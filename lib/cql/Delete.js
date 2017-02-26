

const _ = require('lodash');
var forklift = require('../');

class Delete {
    constructor () {
        this._table = null;
        this._where = null;
    }

    table (name) {
        this._table = name;
        return this;
    }

    where (expression) {
        this._where = expression;
        return this;
    }

    toString () {
        // return `delete from ${this._table} where ${this._where}`;
        return 'delete from ' + this._table + ' where ' + this._where;
    }
};

module.exports = Delete;
