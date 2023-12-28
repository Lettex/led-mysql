# MySQL Wrapper

MySQL Connection Helper is a wrapper around the `mysql` npm package that provides a simple API for MySQL database
operations, with built-in connection resilience and error handling. It is designed to be used in Node.js projects.

## Installation

```shell
npm install --save led-mysql
```

## Usage

```javascript
const dbConnection = require('led-mysql');
const config = {host: "localhost", user: "user", password: "password", database: "mydb", maxAttempts: 5};
const db = dbConnection(config);
let data = await db.query('SELECT * FROM table');
```

## Functions

- `query(sql, args)`: Executes a SQL query and returns a promise which resolves with the data.

- `getRow(sql, args)`: Retrieves the first row of the result set from a SQL query.

- `getVal(sql, args)`: Retrieves the first value from the first row of the result set from a SQL query.

- `insert(table, data)`: Inserts data into a table and returns the last inserted ID.

- `update(table, data, where)`: Updates rows in a table and returns the number of affected rows.

- `uidTable(table, id, min, max)`: Generates a unique random ID for the table.

- `close()`: Closes the database connection.

Please check the [MySQL npm package documentation](https://www.npmjs.com/package/mysql) for more details about the
configuration options.