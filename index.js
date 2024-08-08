const util = require('util');
const mysql = require('mysql');
module.exports = function (config, logger = null) {

    let connectionAttempts = 0;
    if (typeof config.maxAttempts === 'undefined') {
        config.maxAttempts = 5;
    }
    if (typeof config.connectionLimit === 'undefined') {
        config.connectionLimit = 10;
    }
    if (typeof config.acquireTimeout === 'undefined') {
        config.acquireTimeout = 10000;
    }
    if (typeof config.waitForConnections === 'undefined') {
        config.waitForConnections = true;
    }
    if (typeof config.queueLimit === 'undefined') {
        config.queueLimit = 0;
    }
    if (typeof config.queryLog === 'undefined') {
        config.queryLog = false;
    }
    const log = logger || console;
    let pool = null;

    async function connect() {
        if (connectionAttempts >= config.maxAttempts) {
            log.error('db/pool', {
                msg: 'Maximum connection attempts reached',
                text: 'Exiting'
            });
            process.exit(1);
        }
        return await mysql.createPool(config);
    }

    async function getConnection() {
        if (!pool) pool = await connect();
        if (connectionAttempts >= config.maxAttempts) {
            log.error('db/pool', {
                msg: 'Maximum connection attempts reached',
                text: 'Exiting',
                attempts: connectionAttempts
            });
            process.exit(1);
        }
        try {
            const getConnectionPromisified = util.promisify(pool.getConnection).bind(pool);
            let conn = await getConnectionPromisified();

            const errorListener = async function (e) {
                connectionAttempts++;
                log.error('db/pool', {
                    msg: e.message,
                    code: e.code,
                    text: 'Error in connection, reconnecting...',
                    attempts: connectionAttempts
                });
                await new Promise(resolve => setTimeout(resolve, 100 * connectionAttempts));
                return await getConnection();
            };

            conn.on('error', errorListener);
            conn.query = util.promisify(conn.query);
            conn.release = (function (release) {
                return function () {
                    conn.removeListener('error', errorListener);
                    release.call(this);
                };
            })(conn.release);

            return conn;
        } catch (e) {
            connectionAttempts++;
            log.error('db/pool', {
                msg: e.message,
                code: e.code,
                attempts: connectionAttempts,
                text: 'Error on initial connection'
            });
            await new Promise(resolve => setTimeout(resolve, 100 * connectionAttempts));
            return await getConnection();
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
            let connection;
            try {
                if (config.queryLog) {
                    log.debug('db/query', {sql: sql, args: args});
                }
                connection = await getConnection();
                return await connection.query(sql, args);
            } catch (err) {
                log.error('db/query', {
                    args: {sql: sql, args: args},
                    msg: err.message
                });
                return null;
            } finally {
                if (connection) connection.release();
            }
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
            return util.promisify(pool.end).call(pool);
        }
    };

}