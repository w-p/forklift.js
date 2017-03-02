

var debug = require('debug')('test');
var forklift = require('../');
var cql = forklift.cql;
forklift.connect();

var games = [
    {
        id: 1,
        name: 'Uncharted 4: A Thief\'s End',
        platform: 'PS4',
        rating: 10,
        info: {
            publisher: 'Sony Computer Entertainment',
            developer: 'Naughty Dog',
            released: new Date('May 10, 2016')
        }
    },
    {
        id: 2,
        name: 'Call of Duty: Black Ops III',
        platform: 'PS4',
        rating: 7,
        info: {
            publisher: 'Activision',
            developer: 'Treyarch',
            released: new Date('Nov 06, 2015')
        }
    },
    {
        id: 3,
        name: 'Fallout 3',
        platform: 'PC',
        rating: 10,
        info: {
            publisher: 'Bethesda Softworks LLC',
            developer: 'Bethesda Game Studios',
            released: new Date('Oct 28, 2008')
        }
    },
    {
        id: 4,
        name: 'Fallout 4',
        platform: 'PS4',
        rating: 9,
        info: {
            publisher: 'Bethesda Softworks LLC',
            developer: 'Bethesda Game Studios',
            released: new Date('Nov 10, 2015')
        }
    }
];

var table = new cql.Table('game_data')
    .ifNotExists()
    .columns(
        new cql.Column('id').type('integer').primary(),
        new cql.Column('name').type('string').required(),
        new cql.Column('platform').type('string'),
        new cql.Column('rating').type('integer'),
        new cql.Column('info').type('object')
    );

var insert = new cql.Insert()
    .into('game_data')
    .data(games);

var query = new cql.Select()
    .all()
    .from('game_data')
    .where(
        new cql.Expression()
            .gt('rating', 5)
            .and()
            .eq('platform', 'PS4')
    )
    .orderBy('rating');

var query_date = new cql.Select()
    .all()
    .from('game_data')
    .where(
        new cql.Expression()
            .between("info['released']", new Date('Oct 01, 2015'), new Date('Dec 01, 2015'))
    )
    .orderBy("info['released']");

var update = new cql.Update()
    .table('game_data')
    .column('rating')
    .value(10)
    .where(
        new cql.Expression()
            .eq('name', 'Fallout 4')
    );

var del = new cql.Delete()
    .table('game_data')
    .where(
        new cql.Expression()
            .lte('rating', 8)
    );

forklift.send(table).then(function (res) {
    debug(`table create response: ${JSON.stringify(res)}`);
    return forklift.send(insert);
}).then(function (res) {
    return forklift.send('refresh table game_data');
}).then(function (res) {
    debug(`insert response: ${JSON.stringify(res)}`);
    return forklift.send(query);
}).then(function (res) {
    debug('query response:');
    res.rows.forEach(function (row) {
        debug(` ${row.name}, ${row.platform}, ${row.rating}`);
    });
    return forklift.send(query_date);
}).then(function (res) {
    debug('query response:');
    res.rows.forEach(function (row) {
        debug(` ${row.name}, ${row.info.released}`);
    });
    return forklift.sendAndRefresh(update);
}).then(function (res) {
    debug(`update response: ${JSON.stringify(res)}`);
    return forklift.sendAndRefresh(del);
}).then(function (res) {
    debug(`delete response: ${JSON.stringify(res)}`);
    return forklift.send(table.drop());
}).then(function (res) {
    debug(`table drop response: ${JSON.stringify(res)}`);
});
