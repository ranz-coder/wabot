import jsonStore from './core/message-store-json.js'
import mysqlStore from './core/message-store-mysql.js'
import pgsqlStore from './core/message-store-pgsql.js'
import redisStore from './core/message-store-redis.js'
import sqliteStore from './core/message-store-sqlite.js'

const store = process.env?.USE_STORE?.includes('mysql')
    ? mysqlStore
    : process.env?.USE_STORE?.includes('pgsql')
        ? pgsqlStore
        : process.env?.USE_STORE?.includes('redis')
            ? redisStore
            : process.env?.USE_STORE?.includes('sqlite')
                ? sqliteStore
                : jsonStore

export const messages = store.messages
export default store