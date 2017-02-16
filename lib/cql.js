
const _ = require('lodash');

var cql = module.exports = {

    // A dictionary of all Crate's error codes, keyed by code.
    Errors: {
        '4000': 'The statement contains an invalid syntax or unsupported SQL statement',
        '4001': 'The statement contains an invalid analyzer definition.',
        '4002': 'The name of the table is invalid.',
        '4003': 'Field type validation failed',
        '4004': 'Possible feature not supported (yet)',
        '4005': 'Alter table using a table alias is not supported.',
        '4006': 'The used column alias is ambiguous.',
        '4031': 'Only read operations are allowed on this node.',
        '4041': 'Unknown table.',
        '4042': 'Unknown analyzer.',
        '4043': 'Unknown column.',
        '4044': 'Unknown type.',
        '4045': 'Unknown schema.',
        '4046': 'Unknown Partition.',
        '4047': 'Unknown Repository.',
        '4048': 'Unknown Snapshot.',
        '4091': 'A document with the same primary key exists already.',
        '4092': 'A VersionConflict. Might be thrown if an attempt was made to update the same document concurrently.',
        '4093': 'A table with the same name exists already.',
        '4094': 'The used table alias contains tables with different schema.',
        '4095': 'A repository with the same name exists already.',
        '4096': 'A snapshot with the same name already exists in the repository.',
        '4097': 'A partition for the same values already exists in this table.',
        '5000': 'Unhandled server error.',
        '5001': 'The execution of one or more tasks failed.',
        '5002': 'one or more shards are not available.',
        '5003': 'the query failed on one or more shards',
        '5004': 'creating a snapshot failed',
        '5030': 'the query was killed by a kill statement'
    },

    toSqlValue: function (value) {
        if (_.isString(value)) {
            return "'" + value.replace(/'/g, "''") + "'";
        } else if (_.isArray(value)) {
            return '[' + _.chain(value)
                .map(cql.toSqlValue)
                .join(', ')
                .value() + ']';
        } else if (_.isPlainObject(value)) {
            return '{' + _.chain(_.toPairs(value))
                .map(function (pair) {
                    return pair[0] + ' = ' + cql.toSqlValue(pair[1]);
                })
                .join(', ')
                .value() + '}';
        } else {
            return JSON.stringify(value).replace(/'/g, "''");
        }
    },

    // A general expression builder.
    Expression: function () {
        var _expr = [];

        /**
         * Add a 'column < value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.lt = function (column, value) {
            _expr.push([column, '<', value]);
            return this;
        };

        /**
         * Add a 'column <= value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.lte = function (column, value) {
            _expr.push([column, '<=', value]);
            return this;
        };

        /**
         * Add a 'column > value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.gt = function (column, value) {
            _expr.push([column, '>', value]);
            return this;
        };

        /**
         * Add a 'column >= value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.gte = function (column, value) {
            _expr.push([column, '>=', value]);
            return this;
        };

        /**
         * Add a 'column = value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.eq = function (column, value) {
            _expr.push([column, '=', value]);
            return this;
        };

        /**
         * Add a 'column != value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.ne = function (column, value) {
            _expr.push([column, '!=', value]);
            return this;
        };

        /**
         * Add a 'column like value' statement.
         * @param {string} column Column name
         * @param value A comparison value
         */
        this.like = function (column, value) {
            _expr.push([column, 'like', value]);
            return this;
        };

        /**
         * Addd a 'column ~ regex' statement.
         * @param {string} column Column name
         * @param regex A regular expression
         */
        this.match = function (column, regex) {
            // regular expression
            _expr.push([column, '~', regex]);
            return this;
        };

        /**
         * Add a 'column !~ regex' statement.
         * @param {string} column Column name
         * @param regex A regular expression
         */
        this.notMatch = function (column, regex) {
            // regular expression
            _expr.push([column, '!~', regex]);
            return this;
        };

        /**
         * Add a 'column between lower and upper' statement.
         * @param {string} column Column name
         * @param lower The lower bound value
         * @param upper The upper bound value
         */
        this.between = function (column, lower, upper) {
            _expr.push([column, 'between', lower, 'and', upper]);
            return this;
        };

        /**
         * Add an AND operator.
         */
        this.and = function () {
            _expr.push('and');
            return this;
        };

        /**
         * Add an OR operator.
         */
        this.or = function () {
            _expr.push('or');
            return this;
        };

        /**
         * Add a nested expression.
         * @param {forklift.cql.Expression} expression A nested expression
         */
        this.expression = function (expression) {
            _expr.push(expression);
            return this;
        };

        /**
         * Renders the object to an SQL statement.
         */
        this.toStatement = function () {
            return _.chain(_expr)
                .cloneDeep()
                .map(function (e) {
                    if (_.isArray(e)) {
                        if (_.isString(e[2])) {
                            e[2] = cql.toSqlValue(e[2]);
                            // e[2] = "'" + e[2].replace(/'/g, "''") + "'";
                        }
                        return e.join(' ');
                    }
                    return e;
                })
                .join(' ')
                .value();
        };

        /**
         * Renders the object to a string.
         */
        this.toString = function () {
            return '(' + this.toStatement() + ')';
        }
    },

    // A fulltext search (match) expression builder.
    FulltextMatch: function () {
        var _not = '';
        var _columns = [];
        var _term = '';
        var _using = '';
        var _with = {};

        /**
         * NOT's the match.
         */
        this.not = function () {
            _not = 'not';
            return this;
        };

        /**
         * Add a column to search.
         * @param {string} column A column name
         * @param {number} boost A number to boost the resulting columns score
         */
        this.column = function (column, boost=1) {
            _columns.push([column, boost]);
            return this;
        };

        /**
         * Set the columns to search.
         * @param {array} columns An array of column names or column, boost
         * pairs
         */
        this.columns = function (columns) {
            var self = this;
            _.forEach(columns, function (column) {
                if (!_.isArray(column)) {
                    column = [column];
                }
                self.column(...column);
            });
            return this;
        };

        /**
         * Set the term to search for.
         * @param {string} term The search term
         */
        this.term = function (term) {
            _term = "'" + term + "'";
            return this;
        };

        /**
         * Set the match type.
         * @param {string} match A match type from
         * https://crate.io/docs/reference/sql/fulltext.html#match-types
         */
        this.using = function (match) {
            _using = match;
            return this;
        };

        /**
         * Set the search options.
         * @param {object} config A dictionary of option, value pairs from
         * https://crate.io/docs/reference/sql/fulltext.html#options
         */
        this.with = function (config) {
            _with = config;
            return this;
        };

        /**
         * Renders the object to an SQL statement.
         */
        this.toStatement = function () {
            var stmt = 'match ((';
            if (_not) stmt = _not + ' ' + stmt;
            stmt += _.map(_columns, function (column) {
                return column.join(' ').trim();
            });
            stmt += '), ' + _term + ')';
            if (_using) {
                stmt += ' using ' + _using;
            }
            if (!_.isEmpty(_with)) {
                config = _.chain(_with)
                    .flatMap(function (val, key) {
                        // if (_.isString(val)) {
                        //     return key + ' = ' + "'" + val + "'";
                        // }
                        // return key + ' = ' + val;
                        return key + ' = ' + cql.toSqlValue(val);
                    })
                    .join(', ')
                    .value();
                stmt += ' with (' + config + ')';
            }
            return stmt;
        };

        /**
         * Renders the object to a string.
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    // An update statement builder.
    Update: function () {
        var _table = '';
        var _column = '';
        var _value = '';
        var _where = '';

        /**
         * Set the target table.
         * @param {string} table The table on which to operate
         */
        this.table = function (table) {
            _table = table;
            return this;
        };

        /**
         * Set the target column.
         * @param {string} column The column on which to operate
         */
        this.column = function (column) {
            _column = column;
            return this;
        };

        /**
         * Set the value.
         * @param {string} value The value to set
         */
        this.value = function (value) {
            _value = value;
            return this;
        };

        /**
         * Set the where expression.
         * @param {forklift.cql.Expression|string} expression The criteria used
         * to select columns for updating
         */
        this.where = function (expression) {
            _where = expression;
            return this;
        };

        /**
         * Renders the object to an SQL statement.
         */
        this.toStatement = function () {
            var stmt = 'update ' + _table + ' set ' + _column + ' = ';
            // if (_.isString(_value)) {
            //     // https://github.com/crate/crate/pull/3110/files#r47931015
            //     stmt += "'" + _value.replace(/'/g, "''") + "'";
            // } else {
            //     stmt += _value;
            // }
            stmt += cql.toSqlValue(_value);
            if (_where) {
                stmt += ' where ';
                if (_.has(_where, 'toStatement')) {
                    stmt += _where.toStatement();
                } else {
                    stmt += _where;
                }
            }
            return stmt;
        };

        /**
         * Renders the object to a string.
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    // A delete statement builder.
    Delete: function () {
        var _table = '';
        var _where = '';

        /**
         * Set the target table.
         * @param {string} table The table on which to operate
         */
        this.table = function (table) {
            _table = table;
            return this;
        };

        /**
         * Set the where expression.
         * @param {forklift.cql.Expression|string} expression The criteria used
         * to select rows for deleting
         */
        this.where = function (expression) {
            _where = expression;
            return this;
        };

        /**
         * Renders the object to an SQL statement.
         */
        this.toStatement = function () {
            var stmt = 'delete from ' + _table;
            if (_where) {
                stmt += ' where ';
                if (_.has(_where, 'toStatement')) {
                    stmt += _where.toStatement();
                } else {
                    stmt += _where;
                }
            }
            return stmt;
        };

        /**
         * Renders the object to a string.
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    // An insert statement builder.
    Insert: function () {
        var _into = '';
        var _columns = [];
        var _values = [];
        var _duplicateKey = '';

        /**
         * Set all columns and values at once.
         * @param {object} data An object who's keys map to column names
         */
        this.bulk = function (data) {
            var keys = _.keys(data[0]);
            var values = _.map(data, function (value) {
                return _.map(keys, function (key) {
                    return value[key];
                });
            });
            this.columns(keys);
            this.values(values);
            return this;
        };

        /**
         * Set the target table.
         * @param {string} table The name of the target table
         */
        this.into = function (table) {
            _into = table;
            return this;
        };

        /**
         * Set the target column name.
         * @param {string} column A column name in the table
         */
        this.column = function (column) {
            _columns.push(column);
            return this;
        };

        /**
         * Set all target column names.
         * @param {array} columns An array of column names
         */
        this.columns = function (columns) {
            _columns = columns;
            return this;
        };

        /**
         * Set a value to be inserted.
         * @param object A value to be inserted
         */
        this.value = function (value) {
            // can be either value or select statement
            // if (_.isArray(value)) {
            //     value = _.map(value, function (v) {
            //         if (_.isString(v)) {
            //             // https://github.com/crate/crate/pull/3110/files#r47931015
            //             return "'" + v.replace(/'/g, "''") + "'";
            //         }
            //         return v;
            //     });
            // }
            _values.push(value);
            return this;
        };

        /**
         * Set all values to be inserted.
         * @param object An array of values to be inserted
         */
        this.values = function (values) {
            // _values = _.map(values, function (value) {
            //     if (_.isArray(value)) {
            //         return _.map(value, function (v) {
            //             if (_.isString(v)) {
            //                 // https://github.com/crate/crate/pull/3110/files#r47931015
            //                 return "'" + v.replace(/'/g, "''") + "'";
            //             }
            //             return v;
            //         });
            //     }
            //     return value;
            // });
            _values = values;
            return this;
        };

        /**
         * Set the action to take when a duplicate key is written.
         * @param {string} raw A raw SQL statement
         */
        this.onDuplicateKey = function (raw) {
            _duplicateKey = raw;
            return this;
        };

        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = 'insert into ' + _into;
            stmt += ' (' + _columns.join(', ') + ')';
            stmt += ' values (' + _.chain(_values)
                .map(function (value) {
                    if (_.has(value, 'toStatement')) {
                        return value.toStatement();
                    }
                    if (_.isArray(value)) {
                        return _.map(value, function (v) {
                            return cql.toSqlValue(v);
                        })
                    }
                    return cql.toSqlValue(value);
                })
                .join('), (')
                .value() + ')';
            if (_duplicateKey) {
                stmt += ' on duplicate key update ' + _duplicateKey;
            }
            return stmt;
        };

        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    // A select statement builder.
    Select: function () {
        var _columns = [];
        var _from = [];
        var _on = [];
        var _where = '';
        var _orderBy = [];
        var _sortBy = '';
        var _groupBy = [];
        var _having = '';
        var _limit = '';
        var _offset = '';

        /**
         * Sets columns to *, the equivalent of ALL.
         */
        this.all = function () {
            _columns.push('*');
            return this;
        };

        /**
         * Set a DISTINCT column.
         * @param {string} column A column name
         */
        this.distinct = function (column) {
            _columns.push('distinct ' + column);
            return this;
        };

        /**
         * Add a column from which to select.
         * @param {string} column A column name
         */
        this.column = function (column) {
            _columns.push(column);
            return this;
        };

        /**
         * Add all columns from which to select.
         * @param {array} columns An array of column names
         */
        this.columns = function (columns) {
            _columns = columns;
            return this;
        };

        // cross and inner joins are implicit when multiple FROM tables provided
        // from and all joins take a table name or an expression that results
        // in a table

        /**
         * Set the table from which to search.
         * @param {string} table A table name
         */
        this.from = function (table) {
            _from.push(table);
            return this;
        };

        /**
         * Add a table with which to join.
         * @param {string} table A table name
         */
        this.join = function (table) {
            _from.push(table);
            return this;
        };

        /**
         * Set two tables to left join.
         * @param {string} left A table name
         * @param {string} right A table name
         */
        this.leftJoin = function (left, right) {
            _from.push([left, 'left join', right]);
            return this;
        };

        /**
         * Set two tables to right join.
         * @param {string} left A table name
         * @param {string} right A table name
         */
        this.rightJoin = function (left, right) {
            _from.push([left, 'right join', right]);
            return this;
        };

        /**
         * Set two tables to full join.
         * @param {string} left A table name
         * @param {string} right A table name
         */
        this.fullJoin = function (left, right) {
            _from.push([left, 'full join', right]);
            return this;
        };

        /**
         * Set an equality qualifier for a join.
         * @param {string} left A column name
         * @param {string} right A column name
         */
        this.on = function (left, right) {
            _on = [left, right];
            return this;
        };

        /**
         * Add a WHERE expression.
         * @param {forklift.cql.Expression} expression An expression
         */
        this.where = function (expression) {
            _where = expression;
            return this;
        };

        /**
         * Add a column to use for ordering / sorting.
         * @param {string} column A column name
         */
        this.orderBy = function (column) {
            _orderBy.push(column);
            return this;
        };

        /**
         * Set the sorting direction; ASC, DESC, etc.
         * @param {string} order The column sort direction
         */
        this.sortBy = function (order) {
            _sortBy = order;
            return this;
        };

        /**
         * Add a column to use for grouping.
         * @param {string} column A column name
         */
        this.groupBy = function (column) {
            _groupBy.push(column);
            return this;
        };

        /**
         * Add a HAVING expression.
         * @param {string} expression A HAVING expression
         */
        this.having = function (expression) {
            _having = expression;
            return this;
        };

        /**
         * Add a record limit.
         * @param {number} count The maximum number of returned results
         */
        this.limit = function (count) {
            _limit = count;
            return this;
        };

        /**
         * Add a result offset.
         * @param {number} start The index at which to start the returned
         * result set
         */
        this.offset = function (start) {
            _offset = start;
            return this;
        };

        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = 'select ' + _columns.join(', ');
            stmt += ' from ' + _.chain(_from)
                .map(function (table) {
                    if (_.has(table, 'toStatement')) {
                        return table.toStatement();
                    }
                    if (_.isArray(table)) {
                        return table.join(' ');
                    }
                    return table;
                })
                .join(', ')
                .value();
            if (_columns.length > 1 &&
                _on.length > 0 &&
                _.includes(stmt, 'join')) {
                stmt += ' on ' + _on.join(' = ');
            }
            if (_where) {
                stmt += ' where ';
                if (_.has(_where, 'toStatement')) {
                    stmt += _where.toStatement();
                } else {
                    stmt += _where;
                }
            }
            if (_groupBy.length) {
                stmt += ' group by ' + _groupBy.join(', ');
            }
            if (_orderBy.length) {
                stmt += ' order by ' + _orderBy.join(', ');
            }
            if (_sortBy) {
                stmt += ' ' + _sortBy;
            }
            if (_limit) {
                stmt += ' limit ' + _limit;
            }
            if (_offset) {
                stmt += ' offset ' + _offset;
            }
            return stmt;
        };

        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    /**
     * A column schema builder.
     * @param {string} name The name of the column
     */
    Column: function (name) {
        var _name = name;
        var _type = null;
        var _primary = false;
        var _required = false;
        var _index = null; // off, fulltext
        var _analyzer = null; // name of

        /**
         * Set the column type.
         * @param {string} type The name of the data type
         */
        this.type = function (type) {
            _type = type;
            return this;
        };

        /**
         * Set this as the primary key column.
         * @param {boolean} bool Toggle via true or false
         */
        this.primary = function (bool=true) {
            _primary = bool;
            return this;
        };

        /**
         * Set this as a required (NOT NULL) column.
         * @param {boolean} bool Toggle via true or false
         */
        this.required = function (bool=true) {
            _required = bool;
            return this;
        };

        /**
         * Set index type.
         * @param {string} index The type of index, or off
         */
        this.index = function (index) {
            _index = index ? index : '';
            return this;
        };

        /**
         * Set the index analyzer.
         * @param {string} analyzer The name of the analyzer to use
         */
        this.analyzer = function (analyzer='default') {
            _analyzer = analyzer;
            return this;
        };

        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = _name + ' ' + _type;
            if (_primary) {
                stmt += ' primary key';
            } else if (_required) {
                stmt += ' not null';
            }
            if (_index === 'off') {
                stmt += ' index off';
            } else if (_index === 'fulltext') {
                stmt += ' index using fulltext';
                if (_analyzer) {
                    stmt += ' with (analyzer=\'' + _analyzer + '\')';
                }
            }
            return stmt;
        };

        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    /**
     * A generated column schema builder.
     * @class
     * @param {string} name The name of the column
     */
    GeneratedColumn: function (name) {
        var _name = name;
        var _expression = '';

        /**
         * An expression that generates a value.
         * @param {string} expression The computed value expression
         */
        this.expression = function (expression) {
            _expression = expression;
            return this;
        };
        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = _name + ' as (';
            if (_.has(_expression.toStatement)) {
                stmt += _expression.toStatement() + ')';
            } else {
                stmt += _expression + ')';
            }
            return stmt;
        };
        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    /**
     * A fulltext search index column schema builder.
     * @class
     * @param {string} name The name of the index
     */
    FulltextIndexColumn: function (name) {
        var _name = name;
        var _column = '';
        var _analyzer = '';

        /**
         * Set the column to index.
         * @param {string} column The name of the column to index
         */
        this.column = function (column) {
            _column = column;
            return this;
        };

        /**
         * Set the analyzer to use.
         * @param {string} column The name of the analyzer to use
         */
        this.analyzer = function (analyzer='default') {
            _analyzer = analyzer;
            return this;
        };

        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = 'index ' + _name + ' using fulltext(' + _column + ')';
            if (_analyzer) {
                stmt += ' with (analyzer=\'' + _analyzer + '\')';
            }
            return stmt;
        };

        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    },

    /**
     * A table statement builder.
     * @class
     * @param {string} name The name of the table
     */
    Table: function (name) {
        var _name = name;
        var _columns = [];
        var _partitionColumns = [];
        var _clusterColumn = '';
        var _clusterShards = '';
        var _with = {};
        var _action = '';

        /**
         * Add a column to build.
         * @param {forklift.cql.Column} column The column to build
         */
        this.column = function (column) {
            _columns.push(column);
            return this;
        };

        /**
         * Adds all columns to build.
         * @param {array} columns An array of columns to build
         */
        this.columns = function (columns) {
            _columns = columns;
            return this;
        };

        /**
         * Sets the paritioning column name.
         * @param {string} columnNames A list of column names to use for
         * partitioning
         */
        this.partitionColumns = function (columnNames) {
            _partitionColumns = columnNames;
            return this;
        };

        /**
         * Set the column on which to cluster.
         * @param {string} column A column name
         */
        this.clusterColumn = function (column) {
            // routing column must be a primary key column if
            // primary key column is defined
            _clusterColumn = column;
            return this;
        };

        /**
         * Set the number of shards for the table.
         * @param {number} shards A number of shards
         */
        this.clusterShards = function (shards) {
            _clusterShards = shards;
            return this;
        };

        /**
         * Set the configuration options for the table.
         * @param {object} config A object of option, value pairs
         */
        this.with = function (config) {
            _with = config;
            return this;
        };

        /**
         * Sets the create flag.
         * @param {boolean} bool Flag table for creation
         */
        this.create = function (bool=true) {
            if (bool) {
                _action = 'create';
            }
            return this;
        };

        /**
         * Sets the drop flag.
         * @param {boolean} bool Flag table for dropping
         */
        this.drop = function (bool=true) {
            if (bool) {
                _action = 'drop';
            }
            return this;
        };

        /**
         * Renders the object to an SQL statement
         */
        this.toStatement = function () {
            var stmt = _action + ' table ' + _name;
            if (_action === 'drop') {
                return stmt;
            }
            var cols = _.chain(_columns)
                .map(function (column) {
                    return column.toStatement();
                })
                .join(', ')
                .value();
            stmt += ' (' + cols + ')';
            if (_partitionColumns.length) {
                cols = _partitionColumns.join(', ');
                stmt += ' partitioned by (' + cols + ')';
            }
            if (_clusterColumn || _clusterShards) {
                stmt += ' clustered';
                if (_clusterColumn) {
                    stmt += ' by (' + _clusterColumn + ')';
                }
                if (_clusterShards) {
                    stmt += ' into ' + _clusterShards + ' shards';
                }
            }
            if (!_.isEmpty(_with)) {
                config = _.chain(_with)
                    .flatMap(function (val, key) {
                        // if (_.isString(val)) {
                        //     return key + ' = ' + "'" + val + "'";
                        // } else {
                        //     return key + ' = ' + val;
                        // }
                        return key + ' = ' + cql.toSqlValue(val);
                    })
                    .join(', ')
                    .value();
                stmt += ' with (' + config + ')';
            }
            return stmt;
        };

        /**
         * Renders the object to a string
         */
        this.toString = function () {
            return this.toStatement();
        };
    }

}
