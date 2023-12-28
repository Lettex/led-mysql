const util = require('util');
const mysql = require('mysql');
module.exports = function (config, logger = null) {
    let connection;
    let promisifiedQuery;
    let connectionAttempts = 0;
    if (!config.maxAttempts) {
        config.maxAttempts = 5;
    }
    const log = logger || console;

    function connect() {
        return new Promise((resolve, reject) => {
            connection = mysql.createConnection(config);
            connection.connect(function (err) {
                if (err) {
                    log.error('db/connect', {
                        msg: err.message,
                        code: err.code,
                        text: 'Error on initial connection'
                    });
                    reject(err);
                } else {
                    promisifiedQuery = util.promisify(connection.query).bind(connection);
                    connection.on('connect', function () {
                        connectionAttempts = 0;
                    });
                    connection.on('error', handleConnectionError);
                    resolve();
                }
            });
        });
    }

    function handleConnectionError(e) {
        if (connectionAttempts < config.maxAttempts) {
            connectionAttempts++;
            log.error('db/connect', {
                msg: e.message,
                code: e.code,
                text: 'Reconnecting',
                attempts: connectionAttempts
            });
            connect().catch(e => {
                log.error('db/connect', {
                    msg: e.message,
                    code: e.code,
                    text: 'Error during reconnection attempt',
                    attempts: connectionAttempts
                });
            });
        } else {
            log.error('db/connect', {
                msg: e.message,
                code: e.code,
                text: 'Maximum connection attempts reached',
                attempts: connectionAttempts
            });
        }
    }

    return {
        /**
         * Executes a SQL query and returns a promise.
         * @param {string} sql - The SQL query string.
         * @param {[]|{}} args - An array of values to substitute in the SQL query.
         * @returns {Promise} A promise that resolves with the query result.
         */
        async query(sql, args = []) {
            if (typeof promisifiedQuery !== 'function') {
                await connect();
            }
            return promisifiedQuery(sql, args)
                .then(results => results)
                .catch(err => {
                    log.error('db/query', {
                        args: {sql: sql, args: args},
                        msg: err.message
                    });
                    return null;
                });
        },
        /**
         * Retrieves the first row of the result from a SQL query.
         * @param {string} sql - The SQL query string.
         * @param {[]|{}} args - An array of values to substitute in the SQL query.
         * @returns {Promise} A promise that resolves with the first row of the query result.
         */
        getRow(sql, args = []) {
            return this.query(sql, args).then((res) => {
                return res[0] ? res[0] : null;
            }).catch((e) => {
                log.error('db/getRow', {
                    args: {sql: sql, args: args},
                    msg: e.message
                });
                return null;
            });
        },
        /**
         * Retrieves the first value of the result from a SQL query.
         * @param {string} sql - The SQL query string.
         * @param {Array} args - An array of values to substitute in the SQL query.
         * @returns Value or null
         */
        getVal(sql, args = []) {
            return this.query(sql, args).then((res) => {
                return res[0] && Object.values(res[0])[0] !== undefined ? Object.values(res[0])[0] : null;
            }).catch((e) => {
                log.error('db/getVal', {
                    args: {sql: sql, args: args},
                    msg: e.message
                });
                return null;
            });
        },
        /**
         * Inserts data into a table and returns the last inserted ID.
         * @param {string} table - The name of the table to insert data into.
         * @param {object} data - An object with field-value pairs to be inserted.
         * @returns id or null
         */
        async insert(table, data) {
            const sql = `INSERT INTO \`${table}\`
                         SET ?`;
            return this.query(sql, data).then((res) => {
                return res.insertId;
            }).catch((e) => {
                log.error('db/insert', {
                    args: {table: table, data: data},
                    msg: e.message
                });
                return null;
            });
        },
        async update(table, data = {}, where = {}) {
            const columns = Object.keys(data).map(key => `\`${key}\` = ?`).join(', ');
            const whereClause = Object.keys(where).map(key => `\`${key}\` = ?`).join(' AND ');

            const sql = `UPDATE \`${table}\`
                         SET ${columns}
                         WHERE ${whereClause}`;
            const params = [...Object.values(data), ...Object.values(where)]; // Concatenate data and where values

            return this.query(sql, params).then((res) => {
                return res.affectedRows;
            }).catch((e) => {
                log.error('db/update', {
                    args: {table: table, data: data, where: where},
                    msg: e.message
                });
                return null;
            });
        },
        /**
         * Generates a unique random ID for the table.
         * @param {string} table - The name of the table.
         * @param {string} [id='id'] - The ID field name.
         * @param {number} [min=1000] - The minimum number for the random ID.
         * @param {number} [max=0] - The maximum number for the random ID. If not provided, defaults to `Number.MAX_SAFE_INTEGER`.
         * @returns {Promise} A promise that resolves with the unique random ID.
         */
        uidTable(table, id = 'id', min = 10000, max = 0) {
            if (!max) {
                //max mysql int
                max = 2147483647;
            }

            return new Promise(async (resolve) => {
                let uniq;
                let existingId;
                do {
                    uniq = Math.floor(min + Math.random() * (max - min + 1));
                    const sql = `SELECT \`${id}\`
                                 FROM \`${table}\`
                                 WHERE \`${id}\` = ?`;
                    existingId = await this.getVal(sql, [uniq]);
                    // Check if the ID already exists in the table
                } while (existingId !== null);

                resolve(uniq);
            })
                .catch((e) => {
                    log.error('db/uidTable', {
                        args: {table: table, id: id, min: min, max: max},
                        msg: e.message
                    });
                    return false;
                });
        },
        close() {
            return util.promisify(connection.end).call(connection);
        }
    };

}