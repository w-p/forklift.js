

const _ = require('lodash');
var forklift = require('../');
var BaseExpression = require('./BaseExpression.js');

class FulltextMatchExpression extends BaseExpression {
    constructor () {
        super();
        this._not = null;
        this._columns = [];
        this._term = null;
        this._using = null;
        this._with = {};
    }

    not () {
        this._not = 'not';
        return this;
    }

    column (name, boost=1) {
        this._columns.push([name, boost]);
        return this;
    }

    term (term) {
        this._term = "'" + term + "'";
        return this;
    }

    using (match) { // match type, ie: phrase_prefix
        // https://crate.io/docs/reference/sql/fulltext.html#match-types
        this._using = match;
        return this;
    }

    with (config) {
        // https://crate.io/docs/reference/sql/fulltext.html#options
        this._with = config;
        return this;
    }

    toString () {
        var columns = _.map(this._columns, function (column) {
            return column.join(' ').trim();
        });
        // var stmt = `match ((${columns}), ${this._term})`;
        var stmt = 'match ((' + columns + '), ' + this._term + ')';
        if (this._using) {
            stmt += ' using ' + this._using;
        }
        if (!_.isEmpty(this._with)) {
            var config = _.chain(this._with)
                .flatMap(function (val, key) {
                    return key + ' = ' + forklift.util.toSqlValue(val);
                })
                .join(', ')
                .value();
            stmt += ' with (' + config + ')';
        }
        if (this._not) {
            stmt = 'not ' + stmt;
        }
        return stmt;
    }
};

module.exports = FulltextMatchExpression;
