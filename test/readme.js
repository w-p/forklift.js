

var wait = require('wait-for-stuff').for.time;
var forklift = require('../');
var cql = forklift.cql;
forklift.connect();

var games = [
    {
        id: 1,
        name: 'Uncharted 4: A Thief\'s End',
        platform: 'PS4',
        rating: 10
    },
    {
        id: 2,
        name: 'Call of Duty: Black Ops III',
        platform: 'PS4',
        rating: 7
    },
    {
        id: 3,
        name: 'Fallout 3',
        platform: 'PC',
        rating: 10
    },
    {
        id: 4,
        name: 'Fallout 4',
        platform: 'PS4',
        rating: 9
    }
];

var table = new cql.Table('games')
    .columns(
        new cql.Column('id').type('integer').primary(),
        new cql.Column('name').type('string').required(),
        new cql.Column('platform').type('string'),
        new cql.Column('rating').type('integer')
    );

var insert = new cql.Insert()
    .into('games')
    .data(games);

var query = new cql.Select()
    .all()
    .from('games')
    .where(
        new cql.Expression()
            .gt('rating', 5)
            .and()
            .eq('platform', 'PS4')
    )
    .orderBy('rating');

var update = new cql.Update()
    .table('games')
    .column('rating')
    .value(10)
    .where(
        new cql.Expression()
            .eq('name', 'Fallout 4')
    );

var del = new cql.Delete()
    .table('games')
    .where(
        new cql.Expression()
            .lte('rating', 8)
    );

forklift.send(table).then(function (res) {
    console.log('table:', res);
    return forklift.send(insert);
}).then(function (res) {
    console.log('insert:', res);
    wait(1);
    return forklift.send(query);
}).then(function (res) {
    console.log('query:');
    res.rows.forEach(function (row) {
        console.log(`${row.name}, ${row.platform}, ${row.rating}`);
    });
    return forklift.send(update);
}).then(function (res) {
    console.log('update:', res);
    return forklift.send(del);
}).then(function (res) {
    console.log('delete:', res);
    return forklift.send(table.drop());
}).then(function (res) {
    console.log('table:', res);
});
