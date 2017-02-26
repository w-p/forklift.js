

const _ = require('lodash');
var forklift = require('../');
var BaseColumn = require('./BaseColumn.js');

class GeneratedColumn extends BaseColumn {
    constructor (name) {
        super(name);
        this._expression = null;
    }

    expression (expression) {
        // TODO: create expression class for function SQL calls
        this._expression = expression;
        return this;
    }

    toString () {
        // return `${this._name} as (${this._expression})`;
        return this._name + ' as (' + this._expression + ')';
    }
};

module.exports = GeneratedColumn;
