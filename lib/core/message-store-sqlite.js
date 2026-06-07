"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
let DatabaseConstructor = null;
const loadSqlite = async () => {
    if (DatabaseConstructor)
        return DatabaseConstructor;
    try {
        const moduleName = String('better-sqlite3');
        const module = await import(moduleName);
        DatabaseConstructor = module.default || module;
        return DatabaseConstructor;
    }
    catch (e) {
        return null;
    }
};
class MessageStore {
    constructor(dir = 'messages', max = 250) {
        this.db = null;
        this.insertStmt = null;
        this.cleanupStmt = null;
        this.getAllStmt = null;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.messages = Object.create(null);
        this.initDB();
    }
    async initDB() {
        const SQLite = await loadSqlite();
        if (!SQLite) {
            console.warn('[message-store-sqlite] better-sqlite3 module not installed! Running in RAM-only mode.');
            return;
        }
        if (!node_fs_1.default.existsSync(this.storeDir)) {
            node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
        }
        const dbPath = node_path_1.default.join(this.storeDir, 'store.db');
        if (this.db) {
            this.db.close();
        }
        try {
            this.db = new SQLite(dbPath);
            this.db.pragma('journal_mode = WAL');
            this.db.exec(`
            CREATE TABLE IF NOT EXISTS messages (
               jid TEXT,
               id TEXT,
               data TEXT,
               created_at INTEGER,
               PRIMARY KEY (jid, id)
            )
         `);
            this.insertStmt = this.db.prepare('INSERT OR REPLACE INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?)');
            this.cleanupStmt = this.db.prepare(`
            DELETE FROM messages 
            WHERE jid = ? AND id NOT IN (
               SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?
            )
         `);
            this.getAllStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC');
        }
        catch (error) {
            console.error('[message-store-sqlite] Failed to initialize SQLite database:', error);
            this.db = null;
        }
    }
    config({ dir, max }) {
        let dbNeedsReinit = false;
        if (dir) {
            const newDir = node_path_1.default.join(process.cwd(), '.cache', dir);
            if (this.storeDir !== newDir) {
                this.storeDir = newDir;
                dbNeedsReinit = true;
            }
        }
        if (max !== undefined) {
            this.max = max;
        }
        if (dbNeedsReinit) {
            this.initDB();
        }
        return this;
    }
    bind(client) {
        this.client = client;
        client.loadMessage = this.loadMessage.bind(this);
        client.loadMessages = this.loadMessages.bind(this);
        client.addMessage = this.addMessage.bind(this);
        client.messages = this.messages;
        return client;
    }
    loadJidData(jid) {
        if (this.messages[jid])
            return;
        if (!this.getAllStmt) {
            this.messages[jid] = [];
            return;
        }
        try {
            const rows = this.getAllStmt.all(jid);
            this.messages[jid] = rows.map(row => JSON.parse(row.data));
        }
        catch (error) {
            console.error(`[message-store-sqlite] Failed to load JID ${jid} from SQLite:`, error);
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
        if (msgId && this.insertStmt && this.cleanupStmt) {
            try {
                this.insertStmt.run(jid, msgId, JSON.stringify(msg), Date.now());
                this.cleanupStmt.run(jid, jid, this.max);
            }
            catch (error) {
                console.error('[message-store-sqlite] Failed to save message to SQLite:', error);
            }
        }
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-sqlite.js.map