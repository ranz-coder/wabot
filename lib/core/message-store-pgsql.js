"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_path_1 = __importDefault(require("node:path"));
let PGConstructor = null;
const loadPG = async () => {
    if (PGConstructor)
        return PGConstructor;
    try {
        const moduleName = String('pg');
        const module = await import(moduleName);
        PGConstructor = module.default?.Pool || module.Pool || module;
        return PGConstructor;
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
        this.messages = Object.create(null);
    }
    async initDB() {
        const Pool = await loadPG();
        if (!Pool) {
            console.warn('[message-store-pg] pg module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-pg] PostgreSQL URI not provided! Running in RAM-only mode.');
            return;
        }
        if (this.pool) {
            try {
                await this.pool.end();
            }
            catch (e) { }
        }
        try {
            this.pool = new Pool({ connectionString: this.uri });
            await this.pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
               jid VARCHAR(255) NOT NULL,
               id VARCHAR(255) NOT NULL,
               data TEXT NOT NULL,
               created_at BIGINT NOT NULL,
               PRIMARY KEY (jid, id)
            )
         `);
            const { rows } = await this.pool.query('SELECT jid, data FROM messages ORDER BY created_at ASC');
            const loadedMessages = Object.create(null);
            for (const row of rows) {
                if (!loadedMessages[row.jid]) {
                    loadedMessages[row.jid] = [];
                }
                try {
                    loadedMessages[row.jid].push(JSON.parse(row.data));
                }
                catch { }
            }
            this.messages = loadedMessages;
            if (this.client) {
                this.client.messages = this.messages;
            }
        }
        catch (error) {
            console.error('[message-store-pg] Failed to initialize PostgreSQL:', error);
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
        if (!this.messages[jid]) {
            this.messages[jid] = [];
        }
    }
    loadMessage(jid, id) {
        this.loadJidData(jid);
        return this.messages[jid]?.find(v => v.key?.id === id || v.id === id) || null;
    }
    loadMessages(jid, count) {
        this.loadJidData(jid);
        const list = this.messages[jid];
        if (!list || list.length === 0)
            return null;
        const slice = count ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    addMessage(jid, msg) {
        this.loadJidData(jid);
        this.messages[jid].push(msg);
        const msgId = msg.key?.id || msg.id;
        if (this.messages[jid].length > this.max) {
            this.messages[jid].splice(0, this.messages[jid].length - this.max);
        }
        if (msgId && this.pool) {
            this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at', [jid, msgId, JSON.stringify(msg), Date.now()]).then(() => {
                return this.pool.query('DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at DESC LIMIT $3)', [jid, jid, this.max]);
            }).catch((error) => {
                console.error('[message-store-pg] Failed to save message to PG:', error);
            });
        }
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-pgsql.js.map