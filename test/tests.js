
var _ = require('lodash');
var mocha = require('mocha');
var assert = require('assert');
var log = require('mocha-logger');
var expect = require('chai').expect;
var forklift = require('../');
var cql = forklift.cql;

var games = require('./games.json');
var reviewers = require('./reviewers.json');

// set gray colors to something visible, here, white.
_.forEach(mocha.reporters.base.colors, function (v, k) {
    if (v === 90) {
        mocha.reporters.base.colors[k] = 89;
    }
});

process.on("unhandledRejection", function(reason, promise) {
    console.log('REJECTED:', arguments);
});

process.on("rejectionHandled", function(promise) {
    console.log('REJECTION HANDLED:', arguments);
});

var delay = 1;


describe('forklift', function () {

    forklift.connect();
    this.timeout(10000);

    var test_table = new cql.Table('data_types')
        .create()
        .ifNotExists()
        .columns(
            new cql.Column('integer_col').type('integer').primary(),
            new cql.Column('short_col').type('short').required(),
            new cql.Column('long_col').type('long'),
            new cql.Column('float_col').type('float'),
            new cql.Column('double_col').type('double'),
            new cql.Column('boolean_col').type('boolean'),
            new cql.Column('string_col').type('string').index('fulltext').analyzer('english'),
            new cql.Column('byte_col').type('byte').index('off'),
            new cql.Column('ip_col').type('ip'),
            new cql.Column('timestamp_col').type('timestamp'),
            new cql.Column('object_col').type('object'),
            new cql.Column('array_col').type('array(string)'),
            new cql.Column('geo_point_col').type('geo_point'),
            new cql.Column('geo_shape_col').type('geo_shape'),
            new cql.GeneratedColumn('short_plus_long_col').expression('short_col + long_col'),
            new cql.FulltextIndexColumn('ft_string_col').column('string_col').analyzer('english')
        )
        .clusterColumn('integer_col')
        .clusterShards(3)
        .with({number_of_replicas: 1});

    var game_table = new cql.Table('games')
        .columns(
            new cql.Column('id').type('integer').primary(),
            new cql.Column('name').type('string'),
            new cql.Column('platform').type('string'),
            new cql.Column('type').type('string')
        );
    var reviewer_table = new cql.Table('reviewers')
        .columns(
            new cql.Column('id').type('integer').primary(),
            new cql.Column('name').type('string'),
            new cql.Column('favorite_game').type('string')
        );

    var row = {
        integer_col: 127,
        short_col: 127,
        long_col: 127,
        float_col: 127.127,
        double_col: 127.127,
        boolean_col: true,
        string_col: 'a series of strings',
        byte_col: 127,
        ip_col: '192.168.0.1',
        timestamp_col: Date.now(),
        object_col: {foo: 'bar'},
        array_col: ['foo', 'bar', 'baz'],
        geo_point_col: [35.7796, 78.6382],
        geo_shape_col: {
            type: 'Polygon',
            coordinates: [
                [
                    [28.59193,69.064777],[28.445944,68.364613],[29.977426,67.698297],[29.054589,66.944286],[30.21765,65.80598],[29.54443,64.948672],[30.444685,64.204453],[30.035872,63.552814],[31.516092,62.867687],[31.139991,62.357693],[30.211107,61.780028],[28.069998,60.503517],[26.255173,60.423961],[24.496624,60.057316],[22.869695,59.846373],[22.290764,60.391921],[21.322244,60.72017],[21.544866,61.705329],[21.059211,62.607393],[21.536029,63.189735],[22.442744,63.81781],[24.730512,64.902344],[25.398068,65.111427],[25.294043,65.534346],[23.903379,66.006927],[23.56588,66.396051],[23.539473,67.936009],[21.978535,68.616846],[20.645593,69.106247],[21.244936,69.370443],[22.356238,68.841741],[23.66205,68.891247],[24.735679,68.649557],[25.689213,69.092114],[26.179622,69.825299],[27.732292,70.164193],[29.015573,69.766491],[28.59193,69.064777]
                ]
            ]
        }
    };

    var game_rows = new cql.Insert().into('games').data(games);
    var reviewer_rows = new cql.Insert().into('reviewers').data(reviewers);

    describe('core', function () {
        it('should respond with system table rows', function () {
            return forklift.send('select *').then(function (res) {
                return expect(res.rows.length).to.be.gt(0);
            })
        });
        it('should respond with an error', function () {
            return forklift.send('foo sel #$% from!').catch(function (err) {
                expect(err).to.have.property('name');
                return expect(err).to.have.property('sqlState');
            });
        });
    });

    describe('cql', function () {
        it('should create a table with one data type per column', function () {
            return forklift.send(test_table).then(function (res) {
                return expect(res.rows.length).to.equal(0);
            });
        });
        it('should create a table of games and reviewers', function () {
            return forklift.send(game_table).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.send(reviewer_table);
            }).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.sendAndRefresh(game_rows);
            }).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.sendAndRefresh(reviewer_rows);
            }).then(function (res) {
                return expect(res.rows.length).to.equal(0);
            })
        });
        it('should insert a row - manually', function () {
            var columns = _.keys(row);
            var value = _.map(columns, function (column) {
                return row[column];
            });
            var insert = new cql.Insert()
                .into('data_types')
                .columns(...columns)
                .values(value)
                .onDuplicateKey('short_col = short_col + 1');
            return forklift.sendAndRefresh(insert).then(function (res) {
                return forklift.send('select * from data_types');
            }).then(function (res) {
                return expect(res.rows.length).to.equal(1);
            });
        });
        it('should insert a row - automatically', function () {
            row.integer_col += 1; // bump the primary key
            var insert = new cql.Insert()
                .into('data_types')
                .data(row);
            return forklift.sendAndRefresh(insert).then(function (res) {
                return forklift.send('select * from data_types');
            }).then(function (res) {
                return expect(res.rows.length).to.equal(2);
            });
        });
        it('should insert a row - from Select statement', function () {
            var insert = new cql.Insert()
                .into('data_types')
                .columns(
                    'integer_col',
                    'short_col',
                    'long_col'
                )
                .values(
                    [
                        new cql.Select()
                            .columns(
                                'count(integer_col)',
                                'short_col',
                                'long_col'
                            )
                            .from('data_types')
                            .where(
                                new cql.Expression().eq('integer_col', row.integer_col)
                            )
                            .groupBy('short_col')
                            .groupBy('long_col')
                    ]
                );
            return forklift.sendAndRefresh(insert).then(function (res) {
                return forklift.send('select * from data_types');
            }).then(function (res) {
                return expect(res.rows.length).to.equal(3);
            });
        });
        it('should update a row', function () {
            var update = new cql.Update()
                .table('data_types')
                .column("object_col['foo']")
                .value('something else')
                .where(
                    new cql.Expression().eq('integer_col', 127)
                );
            return forklift.sendAndRefresh(update).then(function (res) {
                return forklift.send('select object_col from data_types where integer_col = 127');
            }).then(function (res) {
                return expect(res.rows[0].object_col.foo).to.equal('something else');
            });
        });
        it('should select a row - simple search', function () {
            var select = new cql.Select()
                .all()
                .from('data_types')
                .where(
                    new cql.Expression()
                        .expression(
                            new cql.Expression()
                                .gt('integer_col', 0)
                                .and()
                                .gte('integer_col', 1)
                                .and()
                                .lt('integer_col', 999)
                                .and()
                                .lte('integer_col', 128)
                                .and()
                                .ne('integer_col', 1000)
                                .and()
                                .between('integer_col', 0, 1000)
                        )
                        .or()
                        .expression(
                            new cql.Expression()
                                .like('string_col', 'series')
                                .or()
                                .match('string_col', '/.+/g')
                                .or()
                                .notMatch('string_col', '/.+/g')
                        )
                )
                .orderBy('integer_col')
                .sort('desc')
                .limit(100)
                .offset(0);
            return forklift.send(select).then(function (res) {
                return expect(res.rows.length).to.be.gt(0);
            });
        });
        it('should select a row - fulltext search', function () {
            var select = new cql.Select()
                .distinct('string_col')
                .columns('_score')
                .from('data_types')
                .where(
                    new cql.FulltextMatchExpression()
                        .not()
                        .column('ft_string_col')
                        .term('foo')
                        .using('phrase_prefix')
                        .with({slop: 4})
                );
            return forklift.send(select).then(function (res) {
                return expect(res.rows.length).to.be.gt(0);
            });
        });

        it('should join games and reviewers - cross', function () {
            var select = new cql.Select()
                .columns(
                    'g.name as game',
                    'g.platform',
                    'g.type',
                    'r.name as reviewer',
                    'r.favorite_game'
                )
                .from('games as g')
                .join('reviewers as r');
            return forklift.send(select).then(function (res) {
                var count = games.length * reviewers.length;
                return expect(res.rows.length).to.equal(count);
            });
        });

        it('should join games and reviewers - left', function () {
            var select = new cql.Select()
                .columns(
                    'g.name as game',
                    'g.platform',
                    'g.type',
                    'r.name as reviewer',
                    'r.favorite_game'
                )
                .leftJoin('games as g', 'reviewers as r')
                .on('g.id', 'r.favorite_game');
            return forklift.send(select).then(function (res) {
                return expect(res.rows.length).to.equal(games.length);
            });
        });
        it('should join games and reviewers - right', function () {
            var select = new cql.Select()
                .columns(
                    'g.name as game',
                    'g.platform',
                    'g.type',
                    'r.name as reviewer',
                    'r.favorite_game'
                )
                .rightJoin('games as g', 'reviewers as r')
                .on('g.id', 'r.favorite_game');
            return forklift.send(select).then(function (res) {
                return expect(res.rows.length).to.equal(reviewers.length);
            });
        });
        it('should join games and reviewers - full', function () {
            var select = new cql.Select()
                .columns(
                    'g.name as game',
                    'g.platform',
                    'g.type',
                    'r.name as reviewer',
                    'r.favorite_game'
                )
                .fullJoin('games as g', 'reviewers as r')
                .on('g.id', 'r.favorite_game');
            return forklift.send(select).then(function (res) {
                return expect(res.rows.length).to.be.gte(
                    Math.min(games.length, reviewers.length)
                );
            });
        });
        it('should delete a row', function () {
            var del = new cql.Delete()
                .table('data_types')
                .where(
                    new cql.Expression().eq('integer_col', 127)
                );
            return forklift.send(del).then(function (res) {
                return forklift.send('refresh table data_types');
            }).then(function (res) {
                return forklift.send('select * from data_types');
            }).then(function (res) {
                return expect(res.rows.length).to.be.eq(2);
            });
        });
        it('should drop all tables', function () {
            test_table.drop();
            game_table.drop();
            reviewer_table.drop();
            return forklift.send(test_table).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.send(game_table);
            }).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.send(reviewer_table);
            }).then(function (res) {
                expect(res.rows.length).to.equal(0);
                return forklift.shutdown();
            });
        });
    });
});
