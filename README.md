
<p align='center'>
    <img src='https://github.com/w-p/forklift.js/raw/master/forklift.png' alt='forklift.js' width=150>
</p>

# forklift.js

<img src='https://img.shields.io/badge/statement_coverage-97.9%25-brightgreen.svg?style=flat-square' alt='statement-coverage'>
<img src='https://img.shields.io/badge/branch_coverage-86%25-brightgreen.svg?style=flat-square' alt='branch-coverage'>
<img src='https://img.shields.io/badge/function_coverage-97.9%25-brightgreen.svg?style=flat-square' alt='function-coverage'>
<img src='https://img.shields.io/badge/line_coverage-97.9%25-brightgreen.svg?style=flat-square' alt='line-coverage'>

<img src='https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square' alt='license'>
<img src='https://nodei.co/npm/forklift.js.png?mini=true' alt='npm'>

## A Promise-based node.js library for the [Crate](https://crate.io) database

## Status
TODO:
- [x] Table
- [x] Schema: Columns
- [x] Schema: Generated Columns
- [x] Schema: Fulltext Index Columns
- [x] Expressions: Standard
- [ ] Expressions: Math Operators
- [x] Expressions: Fulltext Match
- [x] Select
- [x] Row: Insert
- [x] Row: Delete
- [x] Row: Update
- [ ] Models

PERF:

3 node cluster, 1 host (i7-4712HQ, 8 core, 2.30GHz, 16GB, Samsung SSD 840)
```
npm run stress-test

> forklift.js@1.5.0 stress-test /data/projects/vortex/lib/forklift.js
> nodejs --expose-gc test/stress-test.js

rows:     100000
shards:   6
replicas: 1

* chunk: 1  time: 150394 ms  avg/row: 1.5039 ms  max mem: 99.998 mb
* chunk: 1000  time: 2458 ms  avg/row: 0.0246 ms  max mem: 88.875 mb
* chunk: 2000  time: 4375 ms  avg/row: 0.0437 ms  max mem: 77.209 mb
* chunk: 3000  time: 2386 ms  avg/row: 0.0239 ms  max mem: 96.321 mb
* chunk: 4000  time: 1994 ms  avg/row: 0.0199 ms  max mem: 99.527 mb
* chunk: 5000  time: 2118 ms  avg/row: 0.0212 ms  max mem: 85.365 mb
* chunk: 6000  time: 2212 ms  avg/row: 0.0221 ms  max mem: 99.820 mb
* chunk: 7000  time: 3035 ms  avg/row: 0.0303 ms  max mem: 80.765 mb
* chunk: 8000  time: 2146 ms  avg/row: 0.0215 ms  max mem: 93.468 mb
* chunk: 9000  time: 2381 ms  avg/row: 0.0238 ms  max mem: 91.611 mb
* chunk: 10000  time: 2588 ms  avg/row: 0.0259 ms  max mem: 96.143 mb
```

## Usage

Install.
```
sudo apt-get install libpq-dev
npm install forklift.js
```

Stand up a Crate cluster, visit 0.0.0.0:5432.
```
docker-compose up -d seed && docker-compose scale member=2
```

Initialize and connect the client.
Note: see test/readme.js for runnable version of this.
```
var forklift = require('forklift.js');
var cql = forklift.cql;
forklift.connect();
```

Create a table and schema with a few columns.
```
var table = new cql.Table('games')
    .columns(
        new cql.Column('id').type('integer').primary(),
        new cql.Column('name').type('string').required(),
        new cql.Column('platform').type('string'),
        new cql.Column('rating').type('integer')
    );

forklift.send(table).then(function (res) {
    console.log(res);
});
```

Make some data.
```
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
        platform: 'PC'
        rating: 10,
    },
    {
        id: 4,
        name: 'Fallout 4',
        platform: 'PS4',
        rating: 9
    }
];
```

Write the data to the table.
```
var insert = new cql.Insert()
    .into('games')
    .data(games);

forklift.send(games).then(function (res) {
    console.log(res);
});
```

Query the data.
```
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

forklift.send(query).then(function (res) {
    res.rows.forEach(function (row) {
        console.log(`${row.name}, ${row.platform}, ${row.rating}`);
    });
});
// Call of Duty: Black Ops III, PS4, 7
// Fallout 4, PS4, 9
// Uncharted 4: A Thief's End, PS4, 10
```

Update some data.
```
var update = new cql.Update()
    .table('games')
    .column('rating')
    .value(10)
    .where(
        new cql.Expression()
            .eq('name', 'Fallout 4')
    );

forklift.send(update).then(function (res) {
    console.log(res);
});
```

Delete some data.
```
var del = new cql.Delete()
    .table('games')
    .where(
        new cql.Expression()
            .lte('rating', 8)
    );

forklift.send(update).then(function (res) {
    console.log(res);
});
```

Drop the table.
```
table.drop();

forklift.send(table).then(function (res) {
    console.log(res);
});
```

## Developing / Contributing / Testing
Contributions welcome. Aim for clean. The tests are not particularly awesome.

Clone, bring up a cluster, and test.
```
git clone https://github.com/w-p/forklift.js.git
cd forklift.js
docker-compose up -d seed && docker-compose scale member=2
npm install --dev
npm test
npm run stress-test
```

## License
MIT
