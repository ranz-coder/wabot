"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = __importDefault(require("node:path"));
const node_fs_1 = __importDefault(require("node:fs"));
const utils_js_1 = require("../utils.js");
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
class Store {
    constructor(dir = 'stores', max = 250) {
        this.db = null;
        this.fallbackStore = null;
        this.fallbackChats = null;
        this.contacts = Object.create(null);
        this.stories = Object.create(null);
        this.presences = Object.create(null);
        this.state = { connection: 'close' };
        this.messageId = new Map();
        this.insertStmt = null;
        this.cleanupStmt = null;
        this.getOneStmt = null;
        this.getLimitStmt = null;
        this.getAllDescStmt = null;
        this.getAllWithOffsetStmt = null;
        this.countStmt = null;
        this.deleteWithOffsetStmt = null;
        this.getChatStmt = null;
        this.insertChatStmt = null;
        this.getAllChatIdsStmt = null;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.database = 'sqlite';
        this.fallbackStore = Object.create(null);
        this.fallbackChats = Object.create(null);
        this.chatsProxyInstance = this.createChatsProxy();
        if (process.env?.USE_STORE?.includes('sqlite')) {
            this.initDB();
        }
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    toPOJO(obj, seen = new WeakSet()) {
        if (obj === null || typeof obj !== 'object')
            return obj;
        if (seen.has(obj))
            return null;
        if (Buffer.isBuffer(obj) || obj instanceof Uint8Array)
            return obj;
        seen.add(obj);
        if (Array.isArray(obj)) {
            return obj.map(v => this.toPOJO(v, seen));
        }
        const res = {};
        for (const key of Object.keys(obj)) {
            const val = obj[key];
            if (typeof val !== 'function') {
                res[key] = this.toPOJO(val, seen);
            }
        }
        return res;
    }
    async initDB() {
        const SQLite = await loadSqlite();
        if (!SQLite) {
            console.warn('[store-sqlite] better-sqlite3 module not installed! Running in RAM-only mode.');
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
            this.db.pragma('synchronous = NORMAL');
            this.db.pragma('temp_store = MEMORY');
            this.db.pragma('cache_size = 10000');
            this.db.exec(`
            CREATE TABLE IF NOT EXISTS messages (
               jid TEXT,
               id TEXT,
               data TEXT,
               created_at INTEGER,
               PRIMARY KEY (jid, id)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_jid_created_at ON messages (jid, created_at DESC);
            CREATE TABLE IF NOT EXISTS chats (
               id TEXT PRIMARY KEY,
               data TEXT
            );
         `);
            this.insertStmt = this.db.prepare('INSERT OR REPLACE INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?)');
            this.cleanupStmt = this.db.prepare('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?)');
            this.getOneStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? AND id = ?');
            this.getLimitStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?');
            this.getAllDescStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC');
            this.getAllWithOffsetStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?');
            this.countStmt = this.db.prepare('SELECT COUNT(*) as count FROM messages WHERE jid = ?');
            this.deleteWithOffsetStmt = this.db.prepare('DELETE FROM messages WHERE jid = ? AND id IN (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?)');
            this.getChatStmt = this.db.prepare('SELECT data FROM chats WHERE id = ?');
            this.insertChatStmt = this.db.prepare('INSERT OR REPLACE INTO chats (id, data) VALUES (?, ?)');
            this.getAllChatIdsStmt = this.db.prepare('SELECT id FROM chats');
        }
        catch (error) {
            console.error('[store-sqlite] Failed to initialize SQLite database. Falling back to RAM-only mode:', error);
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
    createChatsProxy() {
        const self = this;
        return new Proxy(Object.create(null), {
            get: (target, prop) => {
                if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop))
                    return undefined;
                if (self.db && self.getChatStmt) {
                    try {
                        const row = self.getChatStmt.get(prop);
                        return row ? JSON.parse(row.data) : undefined;
                    }
                    catch {
                        return undefined;
                    }
                }
                return self.fallbackChats?.[prop];
            },
            set: (target, prop, value) => {
                if (typeof prop !== 'string')
                    return false;
                if (self.db && self.insertChatStmt) {
                    try {
                        self.insertChatStmt.run(prop, JSON.stringify(self.toPOJO(value)));
                        return true;
                    }
                    catch {
                        return false;
                    }
                }
                if (self.fallbackChats) {
                    self.fallbackChats[prop] = value;
                    return true;
                }
                return false;
            },
            ownKeys: () => {
                if (self.db && self.getAllChatIdsStmt) {
                    try {
                        const rows = self.getAllChatIdsStmt.all();
                        return rows.map(r => r.id);
                    }
                    catch {
                        return [];
                    }
                }
                return self.fallbackChats ? Object.keys(self.fallbackChats) : [];
            },
            getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
        });
    }
    get chats() {
        return this.chatsProxyInstance;
    }
    bind(client) {
        this.client = client;
        client.loadMessage = this.loadMessage.bind(this);
        client.loadMessages = this.loadMessages.bind(this);
        client.addMessage = this.addMessage.bind(this);
        client.getAllMessages = this.getAllMessages.bind(this);
        client.chatUpdate = this.chatUpdate.bind(this);
        client.contactsUpsert = this.contactsUpsert.bind(this);
        client.contactUpdate = this.contactUpdate.bind(this);
        client.getContact = this.getContact.bind(this);
        client.getAllContacts = this.getAllContacts.bind(this);
        client.updateMessageWithReceipt = this.updateMessageWithReceipt.bind(this);
        client.updateMessageWithReaction = this.updateMessageWithReaction.bind(this);
        client.loadStories = this.loadStories.bind(this);
        client.loadStory = this.loadStory.bind(this);
        client.addStory = this.addStory.bind(this);
        client.getAllStories = this.getAllStories.bind(this);
        client.recordMessageId = this.recordMessageId.bind(this);
        client.contacts = this.contacts;
        client.stories = this.stories;
        client.presences = this.presences;
        client.state = this.state;
        client.messageId = this.messageId;
        client.chats = this.chats;
        return client;
    }
    loadMessage(jid, id) {
        if (this.db && this.getOneStmt) {
            try {
                const row = this.getOneStmt.get(jid, id);
                return row ? JSON.parse(row.data) : null;
            }
            catch (error) {
                console.error(`[store-sqlite] Failed to load message ${id} for JID ${jid}:`, error);
                return null;
            }
        }
        if (this.fallbackStore) {
            const list = this.fallbackStore[jid] || [];
            return list.find(v => v.key?.id === id || v.id === id) || null;
        }
        return null;
    }
    loadMessages(jid, count) {
        if (this.db) {
            try {
                let rows = [];
                if (count !== undefined && count > 0) {
                    if (this.getLimitStmt) {
                        rows = this.getLimitStmt.all(jid, count);
                    }
                }
                else {
                    if (this.getAllDescStmt) {
                        rows = this.getAllDescStmt.all(jid);
                    }
                }
                if (rows.length === 0)
                    return null;
                return rows.map(row => JSON.parse(row.data));
            }
            catch (error) {
                console.error(`[store-sqlite] Failed to load messages for JID ${jid}:`, error);
                return null;
            }
        }
        if (this.fallbackStore) {
            const list = this.fallbackStore[jid];
            if (!list || list.length === 0)
                return null;
            const slice = count ? list.slice(-count) : list;
            return [...slice].reverse();
        }
        return null;
    }
    addMessage(jid, msg) {
        if (this.db && this.insertStmt && this.cleanupStmt) {
            const msgId = msg.key?.id || msg.id;
            if (msgId) {
                try {
                    this.insertStmt.run(jid, msgId, JSON.stringify(this.toPOJO(msg)), Date.now());
                    this.cleanupStmt.run(jid, jid, this.max);
                }
                catch (error) {
                    console.error('[store-sqlite] Failed to save message to SQLite:', error);
                }
            }
            return;
        }
        if (this.fallbackStore) {
            if (!this.fallbackStore[jid]) {
                this.fallbackStore[jid] = [];
            }
            this.fallbackStore[jid].push(msg);
            if (this.fallbackStore[jid].length > this.max) {
                this.fallbackStore[jid].splice(0, this.fallbackStore[jid].length - this.max);
            }
        }
    }
    getAllMessages(jid, offset = 0) {
        if (this.db && this.getAllWithOffsetStmt && this.countStmt && this.deleteWithOffsetStmt) {
            try {
                const rows = this.getAllWithOffsetStmt.all(jid, offset);
                const messages = rows.map(row => JSON.parse(row.data));
                messages.count = () => {
                    try {
                        const result = this.countStmt.get(jid);
                        const total = result ? result.count : 0;
                        return Math.max(0, total - offset);
                    }
                    catch (error) {
                        console.error('[store-sqlite] Failed to count messages:', error);
                        return 0;
                    }
                };
                messages.clear = () => {
                    try {
                        this.deleteWithOffsetStmt.run(jid, jid, offset);
                    }
                    catch (error) {
                        console.error(`[store-sqlite] Failed to clear messages for JID ${jid} with offset ${offset}:`, error);
                    }
                };
                return messages;
            }
            catch (error) {
                console.error(`[store-sqlite] Failed to get all messages for JID ${jid}:`, error);
            }
        }
        if (this.fallbackStore) {
            const list = this.fallbackStore[jid] || [];
            const sliced = (offset > 0 ? list.slice(offset) : list);
            sliced.count = () => {
                const currentList = this.fallbackStore?.[jid] || [];
                return Math.max(0, currentList.length - offset);
            };
            sliced.clear = () => {
                if (this.fallbackStore) {
                    if (offset === 0) {
                        delete this.fallbackStore[jid];
                    }
                    else {
                        const currentList = this.fallbackStore[jid] || [];
                        if (offset < currentList.length) {
                            this.fallbackStore[jid] = currentList.slice(0, offset);
                        }
                    }
                }
            };
            return sliced;
        }
        const emptyResult = [];
        emptyResult.count = () => 0;
        emptyResult.clear = () => { };
        return emptyResult;
    }
    chatUpdate(updates) {
        for (const update of updates) {
            if (update.id) {
                const id = update.id;
                this.chats[id] = Object.assign(this.chats[id] || { id }, update);
            }
        }
    }
    contactsUpsert(newContacts) {
        const oldContacts = new Set(Object.keys(this.contacts));
        for (const contact of newContacts) {
            const id = (0, utils_js_1.noSuffix)(contact.id);
            let jid = id;
            if (this.client && jid?.endsWith('lid')) {
                // @ts-ignore
                jid = this.client?.getJidFromJSON(jid)?.jid ?? id;
            }
            oldContacts.delete(jid);
            this.contacts[jid] = Object.assign(this.contacts[jid] || { jid }, contact);
        }
        return oldContacts;
    }
    contactUpdate(updates) {
        for (const update of updates) {
            if (update.id) {
                const id = (0, utils_js_1.noSuffix)(update.id);
                let jid = id;
                if (this.client && jid?.endsWith('lid')) {
                    // @ts-ignore
                    jid = this.client?.getJidFromJSON(jid)?.jid ?? id;
                }
                this.contacts[jid] = Object.assign(this.contacts[jid] || { jid, id: jid }, update);
            }
        }
    }
    getContact(id) {
        if (!id)
            return null;
        if (this.contacts[id])
            return this.contacts[id];
        const found = Object.values(this.contacts).find((c) => c.id === id || c.jid === id || c.sender_pn === id);
        return found || null;
    }
    getAllContacts(offset = 0) {
        const list = Object.values(this.contacts);
        const sliced = (offset > 0 ? list.slice(offset) : list);
        sliced.count = () => {
            const currentList = Object.values(this.contacts);
            return Math.max(0, currentList.length - offset);
        };
        sliced.clear = () => {
            if (offset === 0) {
                for (const key in this.contacts) {
                    delete this.contacts[key];
                }
            }
            else {
                const keys = Object.keys(this.contacts);
                if (offset < keys.length) {
                    for (let i = offset; i < keys.length; i++) {
                        delete this.contacts[keys[i]];
                    }
                }
            }
        };
        return sliced;
    }
    updateMessageWithReceipt(msg, receipt) {
        if (!msg)
            return;
        msg.userReceipt = msg.userReceipt || [];
        const recp = msg.userReceipt.find((m) => m.userJid === receipt.userJid);
        if (recp)
            Object.assign(recp, receipt);
        else
            msg.userReceipt.push(receipt);
        const jid = msg.key?.remoteJid;
        const id = msg.key?.id || msg.id;
        if (this.db && this.insertStmt && jid && id) {
            try {
                this.insertStmt.run(jid, id, JSON.stringify(this.toPOJO(msg)), Date.now());
            }
            catch (error) {
                console.error('[store-sqlite] Failed to update receipt in SQLite:', error);
            }
        }
    }
    updateMessageWithReaction(msg, reaction) {
        if (!msg)
            return;
        const authorID = (0, utils_js_1.getKeyAuthor)(reaction.key);
        msg.reactions = (msg.reactions || []).filter((r) => (0, utils_js_1.getKeyAuthor)(r.key) !== authorID);
        if (reaction.text)
            msg.reactions.push(reaction);
        const jid = msg.key?.remoteJid;
        const id = msg.key?.id || msg.id;
        if (this.db && this.insertStmt && jid && id) {
            try {
                this.insertStmt.run(jid, id, JSON.stringify(this.toPOJO(msg)), Date.now());
            }
            catch (error) {
                console.error('[store-sqlite] Failed to update reaction in SQLite:', error);
            }
        }
    }
    loadStories(jid, count) {
        const list = this.stories[jid];
        if (!list || list.length === 0)
            return null;
        const slice = count && count > 0 ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    loadStory(jid, id) {
        const list = this.stories[jid];
        if (!list || list.length === 0)
            return null;
        return list.find((v) => v.key?.id === id || v.id === id) || null;
    }
    addStory(jid, story) {
        if (!this.stories[jid]) {
            this.stories[jid] = [];
        }
        this.stories[jid].push(story);
        if (this.stories[jid].length > this.max) {
            this.stories[jid].splice(0, this.stories[jid].length - this.max);
        }
    }
    getAllStories(jid, offset = 0) {
        const list = this.stories[jid] || [];
        const sliced = (offset > 0 ? list.slice(offset) : list);
        sliced.count = () => {
            const currentList = this.stories[jid] || [];
            return Math.max(0, currentList.length - offset);
        };
        sliced.clear = () => {
            if (offset === 0) {
                delete this.stories[jid];
            }
            else {
                const currentList = this.stories[jid] || [];
                if (offset < currentList.length) {
                    this.stories[jid] = currentList.slice(0, offset);
                }
            }
        };
        return sliced;
    }
    recordMessageId(sock, msg) {
        if (msg.fromMe)
            return true;
        const id = msg.key?.id || msg.id;
        if (!id)
            return true;
        const instance = (0, utils_js_1.noSuffix)(sock.user.id);
        let instanceMap = this.messageId.get(instance);
        if (!instanceMap) {
            instanceMap = new Map();
            this.messageId.set(instance, instanceMap);
        }
        if (instanceMap.has(id) && !msg.updated)
            return false;
        instanceMap.set(id, { at: Date.now() });
        if (instanceMap.size > 5000) {
            const firstKey = instanceMap.keys().next().value;
            if (firstKey)
                instanceMap.delete(firstKey);
        }
        return true;
    }
    cleanupExpiredMessages() {
        if (this.fallbackStore) {
            Object.values(this.fallbackStore).forEach((msgArray) => {
                if (msgArray && msgArray.length > 100) {
                    msgArray.splice(0, msgArray.length - 100);
                }
            });
        }
        const now = Date.now();
        this.messageId.forEach((instanceMap, instance) => {
            instanceMap.forEach((value, msgId) => {
                if (now - value.at > 900000)
                    instanceMap.delete(msgId);
            });
            if (instanceMap.size === 0)
                this.messageId.delete(instance);
        });
        Object.values(this.stories).forEach((storyArray) => {
            if (storyArray && storyArray.length > 30) {
                storyArray.splice(0, storyArray.length - 30);
            }
        });
    }
}
const store = new Store('stores');
exports.default = store;
//# sourceMappingURL=store-sqlite.js.map