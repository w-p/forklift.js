

const _ = require('lodash');
var forklift = require('../');

class Table {
    constructor (name) {
        this._name = name;
        this._ignore_existing = false;
        this._columns = [];
        this._partitionColumns = [];
        this._clusterColumn = null;
        this._clusterShards = null;
        this._with = {};
        this._action = 'create';
    }

    columns () {
        this._columns = _.values(arguments);
        return this;
    }

    partitionColumns (names) {
        this._partitionColumns = names;
        return this;
    }

    clusterColumn (name) {
        this._clusterColumn = name;
        return this;
    }

    clusterShards (count) {
        this._clusterShards = count;
        return this;
    }

    with (config) {
        this._with = config;
        return this;
    }

    create () {
        this._action = 'create';
        return this;
    }

    drop () {
        this._action = 'drop';
        return this;
    }

    ifNotExists () {
        this._ignore_existing = true;
        return this;
    }

    toString () {
        var stmt = this._action + ' table ';
        if (this._action === 'drop') {
            return stmt + this._name;
        }
        if (this._ignore_existing) {
            stmt += 'if not exists ';
        }
        stmt += this._name;
        var columns = _.chain(this._columns)
            .map(String)
            .join(', ')
            .value();
        if (columns) {
            stmt += ' (' + columns + ')';
        }
        if (this._partitionColumns.length) {
            stmt += ' partitioned by (' + this._partitionColumns.join(', ') + ')';
        }
        if (this._clusterColumn || this._clusterShards) {
            stmt += ' clustered';
            if (this._clusterColumn) {
                stmt += ' by (' + this._clusterColumn + ')';
            }
            if (this._clusterShards) {
                stmt += ' into ' + this._clusterShards + ' shards';
            }
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
        return stmt;
    }
};

module.exports = Table;
