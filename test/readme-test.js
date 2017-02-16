
var wait = require('wait-for-stuff');
// setup
var forklift = require('../');
var cql = forklift.cql;
forklift.connect();

// table / schema
var table = new cql.Table('games')
    .create()
    .columns([
        new cql.Column('id').type('integer').primary(),
        new cql.Column('name').type('string').required(),
        new cql.Column('awesomeness').type('integer'),
        new cql.Column('platform').type('string')
    ])
    .clusterColumn('id')
    .clusterShards(3)
    .with({
        column_policy: 'strict',
        number_of_replicas: '0-all'
    });

forklift.send(table)
    .then(function (res) {
        console.log(res.responseCode, res.message);
        // 200 'OK'
    });

wait.for.time(1);

// data
var data = [
    {
        id: 1,
        name: 'Uncharted 4: A Thief\'s End',
        awesomeness: 4,
        platform: 'PS4'
    },
    {
        id: 2,
        name: 'Last of Us',
        awesomeness: 5,
        platform: 'PS3'
    },
    {
        id: 3,
        name: 'Grand Theft Auto: Episodes from Liberty City',
        awesomeness: 2,
        platform: 'PS3'
    },
    {
        id: 4,
        name: 'Fallout 3',
        awesomeness: 4,
        platform: 'PC'
    },
    {
        id: 5,
        name: 'Far Cry 3',
        awesomeness: 3,
        platform: 'PS3'
    },
    {
        id: 6,
        name: 'Call of Duty: Black Ops 3',
        awesomeness: 2,
        platform: 'PS4'
    }
];

// insert
var insert = new cql.Insert()
    .into('games')
    .bulk(data);

forklift.send(insert)
    .then(function (res) {
        console.log(res.responseCode, res.message);
        // 200 'OK'
    });

wait.for.time(1);

// query
var query = new cql.Select()
    .all()
    .from('games')
    .where(
        new cql.Expression()
            .gt('awesomeness', 2)
            .and()
            .eq('platform', 'PS3')
    )
    .orderBy('awesomeness');

forklift.send(query)
    .then(function (res) {
        console.log('query results:');
        res.data.forEach(function (d) {
            console.log(` ${d.id}, ${d.name}, ${d.platform}, ${d.awesomeness}`);
        });
        // query results:
        //  5, Far Cry 3, PS3, 3
        //  2, Last of Us, PS3, 5
    });

wait.for.time(1);

// update
var update = new cql.Update()
    .table('games')
    .column('awesomeness')
    .value(5)
    .where(
        new cql.Expression()
            .gte('awesomeness', 4)
    );

forklift.send(update)
    .then(function (res) {
        console.log(res.responseCode, res.message);
        // 200 'OK'
    });

wait.for.time(1);

// delete
var del = new cql.Delete()
    .table('games')
    .where(
        new cql.Expression()
            .lte('awesomeness', 4)
    );

forklift.send(update)
    .then(function (res) {
        console.log(res.responseCode, res.message);
        // 200 'OK'
    });

wait.for.time(1);

// drop table
table.drop();

forklift.send(table)
    .then(function (res) {
        console.log(res.responseCode, res.message);
        // 200 'OK'
    });

wait.for.time(1);
