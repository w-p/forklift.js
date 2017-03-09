
<p align='center'>
    <img src='https://github.com/w-p/forklift.js/raw/master/forklift.png' alt='forklift.js' width=150>
</p>

# forklift.js

<img src='https://img.shields.io/badge/statement_coverage-96.96%25-brightgreen.svg?style=flat-square' alt='statement-coverage'>
<img src='https://img.shields.io/badge/branch_coverage-85.06%25-brightgreen.svg?style=flat-square' alt='branch-coverage'>
<img src='https://img.shields.io/badge/function_coverage-98.04%25-brightgreen.svg?style=flat-square' alt='function-coverage'>
<img src='https://img.shields.io/badge/line_coverage-96.95%25-brightgreen.svg?style=flat-square' alt='line-coverage'>

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

rows:     100000
shards:   6
replicas: 1

* chunk: 1  time: 243756 ms  avg/row: 2.4376 ms  max mem: 99.990 mb
* chunk: 1000  time: 5946 ms  avg/row: 0.0595 ms  max mem: 83.736 mb
* chunk: 2000  time: 4771 ms  avg/row: 0.0477 ms  max mem: 58.311 mb
* chunk: 3000  time: 4443 ms  avg/row: 0.0444 ms  max mem: 61.225 mb
* chunk: 4000  time: 4035 ms  avg/row: 0.0403 ms  max mem: 81.539 mb
* chunk: 5000  time: 4936 ms  avg/row: 0.0494 ms  max mem: 73.033 mb
* chunk: 6000  time: 4474 ms  avg/row: 0.0447 ms  max mem: 92.932 mb
* chunk: 7000  time: 3917 ms  avg/row: 0.0392 ms  max mem: 164.269 mb
* chunk: 8000  time: 3723 ms  avg/row: 0.0372 ms  max mem: 68.700 mb
* chunk: 9000  time: 3744 ms  avg/row: 0.0374 ms  max mem: 86.052 mb
* chunk: 10000  time: 4156 ms  avg/row: 0.0416 ms  max mem: 86.429 mb
```

## Usage

Install.
```
sudo apt-get install libpq-dev
npm install forklift.js
```

Stand up a Crate cluster, visit 0.0.0.0:4200.
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

forklift.send(insert).then(function (res) {
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

forklift.send(del).then(function (res) {
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
