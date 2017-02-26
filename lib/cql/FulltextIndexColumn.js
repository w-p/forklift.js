

const _ = require('lodash');
var forklift = require('../');
var BaseColumn = require('./BaseColumn.js');

class FulltextIndexColumn extends BaseColumn {
    constructor (name) {
        super(name);
        this._column = null;
        this._analyzer = null;
    }

    column (column) {
        this._column = column;
        return this;
    }

    analyzer (analyzer='default') {
        this._analyzer = analyzer;
        return this;
    }

    toString () {
        // var stmt = `index ${this._name} using fulltext(${this._column})`;
        var stmt = 'index ' + this._name + ' using fulltext(' + this._column + ')';
        if (this._analyzer) {
            // return `${stmt} with (analyzer=\'${this._analyzer}\')`;
            return stmt + ' with (analyzer=\'' + this._analyzer + '\')';
        } else {
            return stmt;
        }
    }
};

module.exports = FulltextIndexColumn;
