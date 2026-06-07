import jsonStore from './core/memory-store-json.js'
import sqliteStore from './core/memory-store-sqlite.js'

const store = process.env?.USE_STORE?.includes('sqlite')
    ? sqliteStore
    : jsonStore

export const messages = store.messages
export default store