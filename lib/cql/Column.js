

const _ = require('lodash');
var forklift = require('../');
var BaseColumn = require('./BaseColumn.js');

class Column extends BaseColumn {
    constructor (name) {
        super(name);
        this._type = null;
        this._primary = false;
        this._required = false;
        this._index = null;
        this._analyzer = null;
    }

    type (type) {
        this._type = type;
        return this;
    }

    primary (bool=true) {
        this._primary = bool;
        return this;
    }

    required (bool=true) {
        this._required = bool;
        return this;
    }

    index (index) {
        this._index = index;
        return this;
    }

    analyzer (analyzer='default') {
        this._analyzer = analyzer;
        return this;
    }

    toString () {
        // var stmt = `${this._name} ${this._type}`;
        var stmt = this._name + ' ' + this._type;
        if (this._primary) {
            stmt += ' primary key';
        } else if (this._required) {
            stmt += ' not null';
        }
        if (this._index === 'off') {
            stmt += ' index off';
        } else if (this._index === 'fulltext') {
            stmt += ' index using fulltext';
            if (this._analyzer) {
                // return `${stmt} with (analyzer=\'${this._analyzer}\')`;
                return stmt + ' with (analyzer=\'' + this._analyzer + '\')';
            }
        }
        return stmt;
    }
};

module.exports = Column;
