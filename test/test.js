
var _ = require('lodash');
var mocha = require('mocha');
var uuid = require('uuid/v4');
var assert = require('assert');
var log = require('mocha-logger');
var expect = require('chai').expect;
var wait = require('wait-for-stuff');

// set gray colors to something visible, here, white.
_.forEach(mocha.reporters.base.colors, function (v, k) {
    if (v === 90) {
        mocha.reporters.base.colors[k] = 89;
    }
});

describe('forklift', function () {
    this.timeout(6000);
    var data = require('./stackoverflow-posts.json');
    var users = require('./stackoverflow-users.json');
    var forklift = require('../lib/index.js');
    var cql = forklift.cql;
    // needed to drop later
    var postsTable = '';
    var usersTable = '';

    before(function () {
        forklift.connect();
        _.forEach(data, function (datum) {
            datum.id = uuid();
            datum.creationdate = +(new Date(datum.creationdate));
        });
    });

    describe('client', function (done) {
        it('should have a properly formatted URI', function (done) {
            expect(forklift)
                .to.have.deep.property(
                    'cluster.current',
                    'http://0.0.0.0:4200/_sql'
                );
            done();
        });

        it('should have a list of node URIs', function () {
            return forklift.nodes()
                .then(function (res) {
                    _.forEach(res, function (r) {
                        expect(r).to.include('http://');
                        expect(r).to.include(':4200/_sql');
                    });
                    return expect(res).to.equal(forklift.cluster.nodes);
                });
        });
    });

    describe('cql.Table', function () {
        it('should create a table named SoPosts', function () {
            var pk = new cql.Column('id')
                .type('string')
                .primary();
            var date = new cql.Column('creationdate')
                .type('timestamp');
            var score = new cql.Column('score')
                .type('integer');
            var name = new cql.Column('ownerdisplayname')
                .type('string');
            var title = new cql.Column('title')
                .type('string')
                .required();
            var nameScore = new cql.GeneratedColumn('name_score')
                .expression("concat(ownerdisplayname, ' ', score)");
            var titleFt = new cql.FulltextIndexColumn('title_ft')
                .column('title');

            pk.toString(); // for code coverage
            nameScore.toString(); // for code coverage
            titleFt.toString(); // for code coverage

            postsTable = new cql.Table('SoPosts')
                .create()
                .column(pk)
                .columns([
                    pk,
                    date,
                    score,
                    name,
                    title,
                    nameScore,
                    titleFt
                ])
                // .partitionColumns(['creationdate']) // only if no primary key?
                .clusterColumn('id') // must match a primary key
                .clusterShards(3)
                .with({
                    column_policy: 'strict',
                    number_of_replicas: '0-all'
                });
            postsTable.toString();
            return forklift.send(postsTable)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });

        it('should report a duplicated table error', function () {
            postsTable.create();
            return forklift.send(postsTable)
                .then(function (res) {
                    return expect(res).to.not.have.property('responseCode', 200);
                })
                .catch(function (err) {
                    return expect(err.errorCode).to.equal(4093);
                });
        });

        it('should create a table named SoUsers', function () {
            usersTable = new cql.Table('SoUsers')
                .create()
                .columns([
                    new cql.Column('displayname').type('string').primary(),
                    new cql.Column('reputation').type('integer'),
                    new cql.Column('upvotes').type('integer'),
                    new cql.Column('downvotes').type('integer')
                ]);
            log.log(usersTable);
            return forklift.send(usersTable)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });
    });

    describe('cql.Insert', function () {
        it('should insert a single row to SoPosts', function () {
            var row = data.shift();
            var keys = _.keys(row);
            var value = _.map(keys, function (key) {
                return row[key];
            });
            var insert = new cql.Insert().into('SoPosts');
            _.forEach(keys, function (key) {
                insert.column(key);
            });
            insert.value(value);
            insert.toString(); // for code coverage
            return forklift.send(insert)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });

        it('should bulk insert all remaining rows to SoPosts', function () {
            var keys = _.keys(data[0]);
            var values = _.map(data, function (value) {
                return _.map(keys, function (key) {
                    return value[key];
                });
            });
            var insert = new cql.Insert()
                .into('SoPosts')
                .columns(keys)
                .values(values);
            return forklift.send(insert)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });

        it('should bulk insert all rows in SoUsers', function () {
            var insert = new cql.Insert()
                .into('SoUsers')
                .bulk(users);
            return forklift.send(insert)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });
    });

    describe('cql.Select', function () {
        it('should return all 100 records', function () {

            log.log('waiting 3 sec. for the database catch up');
            wait.for.time(1);

            var query = new cql.Select()
                .all()
                .from('SoPosts')
                .orderBy('creationdate');
            query.toString(); // for code coverage
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(100);
                });
        });

        it('should return 99 records from a \'kitchen sink\' query', function () {
            var expr = new cql.Expression()
                .expression(
                    new cql.Expression()
                        .between('score', 2000, 4000)
                )
                .or()
                .expression(
                    new cql.Expression()
                        .lt('score', 10000)
                        .or()
                        .lte('score', 2000)
                        .or()
                        .gt('score', 0)
                        .or()
                        .gte('score', 4000)
                )
                .and()
                .ne('score', 0)
                .and()
                .match('ownerdisplayname', '/.+/g')
                .or()
                .notMatch('ownerdisplayname', '/\D+/g')
                .or()
                .like('title', '%');
            expr.toString(); // for code coverage
            var query = new cql.Select()
                .columns([
                    'title',
                    'score',
                    'ownerdisplayname'
                ])
                .from('SoPosts')
                .where(expr)
                .orderBy('score')
                .sortBy('desc')
                .groupBy('score')
                .groupBy('title')
                .groupBy('ownerdisplayname')
                .limit(500)
                .offset(1);
            query.toString(); // for code coverage
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(99);
                });
        });

        it('should return 1 record from an equality query', function () {
            var query = new cql.Select()
                .distinct('score')
                .from('SoPosts')
                .where(
                    new cql.Expression()
                        .eq('score', 1793)
                );
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(1);
                });
        });

        it('should return 24 records from 2009', function () {
            var start = new Date(2009, 0, 1);
            var end = new Date(2009, 11, 31);
            var query = new cql.Select()
                .column('creationdate')
                .from('SoPosts')
                .where(
                    new cql.Expression()
                        .between('creationdate', +start, +end)
                )
                .orderBy('creationdate');
            return forklift.send(query)
                .then(function (res) {
                    var result = _.every(res.data, function (datum) {
                        return (
                            datum.creationdate >= +start
                            &&
                            datum.creationdate <= +end
                        );
                    });
                    log.log(query);
                    expect(result).to.equal(true);
                    return expect(res.data).to.have.lengthOf(24);
                });
        });

        it('should return 7 fulltext matches of \'python dictionary\'', function () {
            var ftExpr = new cql.FulltextMatch()
                .column('title_ft', 2)
                .term('python dictionary');
            ftExpr.toString(); // for code coverage
            var query = new cql.Select()
                .column('title')
                .column('_score')
                .from('SoPosts')
                .where(ftExpr)
                .orderBy('_score');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(7);
                });
        });

        it('should return 94 results not matching a prefix of \'if a\'', function () {
            var query = new cql.Select()
                .column('title')
                .column('_score')
                .from('SoPosts')
                .where(
                    new cql.FulltextMatch()
                        .columns(['title_ft'])
                        .not()
                        .term('if a')
                        .using('phrase_prefix')
                        .with({slop: 4})
                )
                .orderBy('_score');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(94);
                });
        });

        it('should 7200 results from cross joining SoPosts with SoUsers', function () {
            var query = new cql.Select()
                .columns([
                    'p.score',
                    'u.reputation',
                    'u.displayname'
                ])
                .from('SoUsers as u')
                .join('SoPosts as p')
                .orderBy('p.score')
                .sortBy('desc');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(7200);
                });
        });

        it('should 76 results from inner joining SoPosts with SoUsers', function () {
            var query = new cql.Select()
                .columns([
                    'p.score',
                    'u.reputation',
                    'u.displayname'
                ])
                .from('SoUsers as u')
                .join('SoPosts as p')
                .where('u.displayname = p.ownerdisplayname')
                .orderBy('p.score')
                .sortBy('desc');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(76);
                });
        });

        it('should 78 results from left joining SoPosts with SoUsers on displayname', function () {
            var query = new cql.Select()
                .columns([
                    'p.score',
                    'u.reputation',
                    'u.displayname'
                ])
                .leftJoin('SoUsers as u', 'SoPosts as p')
                .on('u.displayname', 'p.ownerdisplayname')
                .orderBy('p.score')
                .sortBy('desc');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(78);
                });
        });

        it('should 100 results from right joining SoPosts with SoUsers on displayname', function () {
            var query = new cql.Select()
                .columns([
                    'p.score',
                    'u.reputation',
                    'u.displayname'
                ])
                .rightJoin('SoUsers as u', 'SoPosts as p')
                .on('u.displayname', 'p.ownerdisplayname')
                .orderBy('p.score')
                .sortBy('desc');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(100);
                });
        });

        it('should 102 results from full joining SoPosts with SoUsers on displayname', function () {
            var query = new cql.Select()
                .columns([
                    'p.score',
                    'u.reputation',
                    'u.displayname'
                ])
                .fullJoin('SoUsers as u', 'SoPosts as p')
                .on('u.displayname', 'p.ownerdisplayname')
                .orderBy('p.score')
                .sortBy('desc');
            return forklift.send(query)
                .then(function (res) {
                    log.log(query);
                    return expect(res.data).to.have.lengthOf(102);
                });
        });
    });

    describe('cql.Table', function () {
        it('should drop a table named SoPosts', function () {
            postsTable.drop();
            return forklift.send(postsTable)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });

        it('should drop a table named SoUsers', function () {
            usersTable.drop();
            return forklift.send(usersTable)
                .then(function (res) {
                    expect(res).to.have.property('responseCode', 200);
                    return expect(res).to.have.property('message', 'OK');
                });
        });
    });
});
