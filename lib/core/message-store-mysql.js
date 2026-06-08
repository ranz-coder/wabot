"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_path_1 = __importDefault(require("node:path"));
let MySQLConstructor = null;
const loadMySQL = async () => {
    if (MySQLConstructor)
        return MySQLConstructor;
    try {
        const moduleName = String('mysql2/promise');
        const module = await import(moduleName);
        MySQLConstructor = module.default || module;
        return MySQLConstructor;
    }
    catch (e) {
        return null;
    }
};
class MessageStore {
    constructor(dir = 'messages', max = 250, uri) {
        this.pool = null;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.uri = uri || process.env.USE_STORE;
        this._messages = Object.create(null);
        this.loadedJids = new Set();
        this.loadingJids = new Set();
        this.maxCachedJids = 50;
        const self = this;
        this.messages = new Proxy(this._messages, {
            get(target, prop, receiver) {
                if (typeof prop === 'string' && !['prototype', 'constructor', 'toJSON'].includes(prop)) {
                    self.loadJidData(prop);
                    self.touchJid(prop);
                }
                return Reflect.get(target, prop, receiver);
            },
            set(target, prop, value, receiver) {
                if (typeof prop === 'string' && !['prototype', 'constructor', 'toJSON'].includes(prop)) {
                    self.touchJid(prop);
                }
                return Reflect.set(target, prop, value, receiver);
            },
            deleteProperty(target, prop) {
                if (typeof prop === 'string') {
                    self.loadedJids.delete(prop);
                }
                return Reflect.deleteProperty(target, prop);
            }
        });
    }
    async initDB() {
        const mysql = await loadMySQL();
        if (!mysql) {
            console.warn('[message-store-mysql] mysql2 module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-mysql] MySQL URI not provided! Running in RAM-only mode.');
            return;
        }
        if (this.pool) {
            try {
                await this.pool.end();
            }
            catch (e) { }
        }
        try {
            this.pool = mysql.createPool(this.uri);
            await this.pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
               jid VARCHAR(255) NOT NULL,
               id VARCHAR(255) NOT NULL,
               data LONGTEXT NOT NULL,
               created_at BIGINT NOT NULL,
               PRIMARY KEY (jid, id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
         `);
        }
        catch (error) {
            console.error('[message-store-mysql] Failed to initialize MySQL:', error);
            this.pool = null;
        }
    }
    config({ dir, max, uri }) {
        let needsReinit = false;
        if (dir) {
            this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        }
        if (max !== undefined) {
            this.max = max;
        }
        if (uri && uri !== this.uri) {
            this.uri = uri;
            needsReinit = true;
        }
        if (needsReinit) {
            this.initDB();
        }
        return this;
    }
    bind(client) {
        this.client = client;
        this.initDB();
        client.loadMessage = this.loadMessage.bind(this);
        client.loadMessages = this.loadMessages.bind(this);
        client.addMessage = this.addMessage.bind(this);
        client.messages = this.messages;
        return client;
    }
    loadJidData(jid) {
        if (!this._messages[jid]) {
            this._messages[jid] = [];
            this.asyncLoadJid(jid);
        }
    }
    async asyncLoadJid(jid) {
        if (!this.pool)
            return;
        this.loadingJids.add(jid);
        try {
            const [rows] = await this.pool.query('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC', [jid]);
            const history = [];
            for (const row of rows) {
                try {
                    history.push(JSON.parse(row.data));
                }
                catch { }
            }
            const current = this._messages[jid] || [];
            const merged = [...history];
            for (const msg of current) {
                const id = msg.key?.id || msg.id;
                const exists = merged.some(v => (v.key?.id === id || v.id === id));
                if (!exists) {
                    merged.push(msg);
                }
            }
            if (merged.length > this.max) {
                merged.splice(0, merged.length - this.max);
            }
            this._messages[jid] = merged;
        }
        catch (error) {
            console.error(`[message-store-mysql] Failed to load JID ${jid} from MySQL:`, error);
        }
        finally {
            this.loadingJids.delete(jid);
        }
    }
    touchJid(jid) {
        this.loadedJids.delete(jid);
        this.loadedJids.add(jid);
        if (this.loadedJids.size > this.maxCachedJids) {
            for (const oldJid of this.loadedJids) {
                if (this.loadingJids.has(oldJid))
                    continue;
                delete this._messages[oldJid];
                this.loadedJids.delete(oldJid);
                break;
            }
        }
    }
    loadMessage(jid, id) {
        this.loadJidData(jid);
        this.touchJid(jid);
        return this._messages[jid]?.find(v => v.key?.id === id || v.id === id) || null;
    }
    loadMessages(jid, count) {
        this.loadJidData(jid);
        this.touchJid(jid);
        const list = this._messages[jid];
        if (!list || list.length === 0)
            return null;
        const slice = count ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    addMessage(jid, msg) {
        this.loadJidData(jid);
        this._messages[jid].push(msg);
        const msgId = msg.key?.id || msg.id;
        if (this._messages[jid].length > this.max) {
            this._messages[jid].splice(0, this._messages[jid].length - this.max);
        }
        this.touchJid(jid);
        if (msgId && this.pool) {
            this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)', [jid, msgId, JSON.stringify(msg), Date.now()]).then(() => {
                return this.pool.query('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?) as tmp)', [jid, jid, this.max]);
            }).catch((error) => {
                console.error('[message-store-mysql] Failed to save message to MySQL:', error);
            });
        }
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-mysql.js.map