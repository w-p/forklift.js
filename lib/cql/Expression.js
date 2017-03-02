

const _ = require('lodash');
var forklift = require('../');
var BaseExpression = require('./BaseExpression.js');

class Expression extends BaseExpression {
    constructor () {
        super();
        this._expression = [];
    }

    op (column, operand, value) {
        value = forklift.util.toSqlValue(value);
        this._expression.push([column, operand, value]);
        return this;
    }

    lt (column, value) {
        return this.op(column, '<', value);
    }

    lte (column, value) {
        return this.op(column, '<=', value);
    }

    gt (column, value) {
        return this.op(column, '>', value);
    }

    gte (column, value) {
        return this.op(column, '>=', value);
    }

    eq (column, value) {
        return this.op(column, '=', value);
    }

    ne (column, value) {
        return this.op(column, '!=', value);
    }

    like (column, value) {
        return this.op(column, 'like', value);
    }

    match (column, value) {
        return this.op(column, '~', value);
    }

    notMatch (column, value) {
        return this.op(column, '!~', value);
    }

    between (column, lower, upper) {
        lower = forklift.util.toSqlValue(lower);
        upper = forklift.util.toSqlValue(upper);
        this._expression.push([column, 'between', lower, 'and', upper]);
        return this;
    }

    and () {
        this._expression.push('and');
        return this;
    }

    or () {
        this._expression.push('or');
        return this;
    }

    expression (value) {
        this._expression.push(value);
        return this;
    }

    toString () {
        var stmt = _.chain(this._expression)
            .cloneDeep()
            .map(function (e) {
                if (_.isArray(e)) {
                    return e.join(' ');
                } else {
                    return e;
                }
            })
            .join(' ')
            .value();
        return '(' + stmt + ')';
    };
};

module.exports = Expression;
