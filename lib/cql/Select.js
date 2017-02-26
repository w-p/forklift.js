

const _ = require('lodash');
var forklift = require('../');
var BaseExpression = require('./BaseExpression.js');


class Select {
    constructor () {
        this._columns = [];
        this._from = [];
        this._on = [];
        this._where = null;
        this._orderBy = [];
        this._sort = null;
        this._groupBy = [];
        this._having = null;
        this._limit = null;
        this._offset = null;
    }

    all () {
        this._columns.push('*');
        return this;
    }

    distinct (column) {
        this._columns.push('distinct ' + column);
        return this;
    }

    columns () {
        this._columns = _.values(arguments);
        return this;
    }

    from (table) { // table name or expression
        this._from.push(table);
        return this;
    }

    join (table) {
        this._from.push(table);
        return this;
    }

    leftJoin (left, right) {
        this._from.push([left, 'left join', right]);
        return this;
    }

    rightJoin (left, right) {
        this._from.push([left, 'right join', right]);
        return this;
    }

    fullJoin (left, right) {
        this._from.push([left, 'full join', right]);
        return this;
    }

    on (left, right) {
        this._on = [left, right];
        return this;
    }

    where (expression) {
        this._where = expression;
        return this;
    }

    orderBy (column) {
        this._orderBy.push(column);
        return this;
    }

    sort (order) {
        this._sort = order;
        return this;
    }

    groupBy (column) {
        this._groupBy.push(column);
        return this;
    }

    having (expression) {
        this._having = expression;
        return this;
    }

    limit (count) {
        this._limit = count;
        return this;
    }

    offset (start) {
        this._offset = start;
        return this;
    }

    toString () {
        var columns = this._columns.join(', ');
        var tables = _.chain(this._from)
            .map(function (table) {
                if (table instanceof Select) {
                    // return `(${table.toString()})`;
                    return '(' + table.toString() + ')';
                }
                if (_.isArray(table)) {
                    return table.join(' ');
                }
                return table;
            })
            .join(', ')
            .value();
        // var stmt = `select ${columns} from ${tables}`;
        var stmt = 'select ' + columns + ' from ' + tables;
        if (this._columns.length > 1 &&
            this._on.length > 0 &&
            _.includes(stmt, 'join')) {
            stmt += ' on ' + this._on.join(' = ');
        }
        if (this._where) {
            stmt += ' where ' + this._where;
        }
        if (this._groupBy.length) {
            stmt += ' group by ' + this._groupBy.join(', ');
        }
        if (this._orderBy.length) {
            stmt += ' order by ' + this._orderBy.join(', ');
        }
        if (this._sort) {
            stmt += ' ' + this._sort;
        }
        if (this._limit) {
            stmt += ' limit ' + this._limit;
        }
        if (this._offset !== null) {
            stmt += ' offset ' + this._offset;
        }
        return stmt;
    }
};

module.exports = Select;
