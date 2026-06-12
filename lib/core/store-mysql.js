"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = __importDefault(require("node:path"));
const utils_js_1 = require("../utils.js");
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
class Store {
    constructor(dir = 'stores', max = 250, uri) {
        this.pool = null;
        this.fallbackStore = null;
        this.fallbackChats = null;
        this.contacts = Object.create(null);
        this.stories = Object.create(null);
        this.presences = Object.create(null);
        this.state = { connection: 'close' };
        this.messageId = new Map();
        this.cache = new Map();
        this.maxCachedJids = 15;
        this.writeQueues = new Map();
        this.chatsCache = new Map();
        this.isChatsPreloaded = false;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.uri = uri || process.env.USE_STORE;
        this.database = 'mysql';
        this.fallbackStore = Object.create(null);
        this.fallbackChats = Object.create(null);
        this.chatsProxyInstance = this.createChatsProxy();
        if (process.env?.USE_STORE?.includes('mysql')) {
            this.initDB();
        }
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    async initDB() {
        const mysql = await loadMySQL();
        if (!mysql) {
            console.warn('[store-mysql] mysql2 module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[store-mysql] MySQL URI not provided! Running in RAM-only mode.');
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
         `);
            try {
                await this.pool.query(`
               ALTER TABLE messages ADD INDEX idx_messages_jid_created_at (jid, created_at DESC);
            `);
            }
            catch (e) { }
            await this.pool.query(`
            CREATE TABLE IF NOT EXISTS chats (
               id VARCHAR(255) NOT NULL,
               data LONGTEXT NOT NULL,
               PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
         `);
            await this.preloadChats();
        }
        catch (error) {
            console.error('[store-mysql] Failed to initialize MySQL. Falling back to RAM-only mode:', error);
            this.pool = null;
        }
    }
    async preloadChats() {
        if (!this.pool)
            return;
        try {
            const [rows] = await this.pool.query('SELECT id, data FROM chats');
            for (const row of rows) {
                this.chatsCache.set(row.id, JSON.parse(row.data));
            }
            this.isChatsPreloaded = true;
        }
        catch (error) {
            console.error('[store-mysql] Failed to preload chats:', error);
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
    createChatsProxy() {
        const self = this;
        return new Proxy(Object.create(null), {
            get: (target, prop) => {
                if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop))
                    return undefined;
                return self.chatsCache.get(prop) || self.fallbackChats?.[prop];
            },
            set: (target, prop, value) => {
                if (typeof prop !== 'string')
                    return false;
                self.chatsCache.set(prop, value);
                if (self.pool) {
                    self.pool.query('INSERT INTO chats (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data)', [prop, JSON.stringify(value)]).catch(() => { });
                }
                else if (self.fallbackChats) {
                    self.fallbackChats[prop] = value;
                }
                return true;
            },
            ownKeys: () => {
                return self.pool ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : []);
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
    touchJid(jid) {
        const data = this.cache.get(jid);
        if (data) {
            this.cache.delete(jid);
            this.cache.set(jid, data);
        }
    }
    evictOldestCache() {
        if (this.cache.size > this.maxCachedJids) {
            for (const [key] of this.cache) {
                if (this.writeQueues.has(key))
                    continue;
                this.cache.delete(key);
                if (this.cache.size <= this.maxCachedJids)
                    break;
            }
        }
    }
    async getMySQLData(jid) {
        if (this.cache.has(jid)) {
            this.touchJid(jid);
            return this.cache.get(jid);
        }
        if (!this.pool)
            return [];
        try {
            const [rows] = await this.pool.query('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC', [jid]);
            const data = rows.map((row) => JSON.parse(row.data));
            this.cache.set(jid, data);
            this.evictOldestCache();
            return data;
        }
        catch (error) {
            console.error(`[store-mysql] Failed to load messages for JID ${jid}:`, error);
            return [];
        }
    }
    async loadMessage(jid, id) {
        const list = await this.getMySQLData(jid);
        return list.find(v => v.key?.id === id || v.id === id) || null;
    }
    async loadMessages(jid, count) {
        const list = await this.getMySQLData(jid);
        if (list.length === 0)
            return null;
        const slice = count ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    async addMessage(jid, msg) {
        const list = await this.getMySQLData(jid);
        list.push(msg);
        if (list.length > this.max) {
            list.splice(0, list.length - this.max);
        }
        const msgId = msg.key?.id || msg.id;
        if (this.pool && msgId) {
            const previous = this.writeQueues.get(jid) || Promise.resolve();
            const current = previous
                .then(async () => {
                try {
                    await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)', [jid, msgId, JSON.stringify(msg), Date.now()]);
                    await this.pool.query('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?) as tmp)', [jid, jid, this.max]);
                }
                catch (error) {
                    console.error('[store-mysql] Failed to save message to MySQL:', error);
                }
            })
                .finally(() => {
                if (this.writeQueues.get(jid) === current) {
                    this.writeQueues.delete(jid);
                }
            });
            this.writeQueues.set(jid, current);
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
        const self = this;
        const promise = (async () => {
            let list = [];
            if (self.pool) {
                list = await self.getMySQLData(jid);
            }
            else if (self.fallbackStore) {
                list = self.fallbackStore[jid] || [];
            }
            const sliced = (offset > 0 ? list.slice(offset) : list);
            sliced.count = async () => {
                if (self.pool) {
                    const currentList = await self.getMySQLData(jid);
                    return Math.max(0, currentList.length - offset);
                }
                if (self.fallbackStore) {
                    const total = (self.fallbackStore[jid] || []).length;
                    return Math.max(0, total - offset);
                }
                return 0;
            };
            sliced.clear = async () => {
                self.cache.delete(jid);
                if (self.pool) {
                    const previous = self.writeQueues.get(jid) || Promise.resolve();
                    const current = previous
                        .then(async () => {
                        try {
                            if (offset === 0) {
                                await self.pool.query('DELETE FROM messages WHERE jid = ?', [jid]);
                            }
                            else {
                                await self.pool.query('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT ?) as tmp)', [jid, jid, offset]);
                            }
                        }
                        catch (error) {
                            console.error(`[store-mysql] Failed to clear messages for JID ${jid}:`, error);
                        }
                    })
                        .finally(() => {
                        if (self.writeQueues.get(jid) === current) {
                            self.writeQueues.delete(jid);
                        }
                    });
                    self.writeQueues.set(jid, current);
                    return;
                }
                if (self.fallbackStore) {
                    if (offset === 0) {
                        delete self.fallbackStore[jid];
                    }
                    else {
                        const currentList = self.fallbackStore[jid] || [];
                        if (offset < currentList.length) {
                            self.fallbackStore[jid] = currentList.slice(0, offset);
                        }
                    }
                }
            };
            return sliced;
        })();
        const promiseWithMethods = promise;
        promiseWithMethods.count = async () => {
            if (self.pool) {
                const currentList = await self.getMySQLData(jid);
                return Math.max(0, currentList.length - offset);
            }
            if (self.fallbackStore) {
                const total = (self.fallbackStore[jid] || []).length;
                return Math.max(0, total - offset);
            }
            return 0;
        };
        promiseWithMethods.clear = async () => {
            self.cache.delete(jid);
            if (self.pool) {
                const previous = self.writeQueues.get(jid) || Promise.resolve();
                const current = previous
                    .then(async () => {
                    try {
                        if (offset === 0) {
                            await self.pool.query('DELETE FROM messages WHERE jid = ?', [jid]);
                        }
                        else {
                            await self.pool.query('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT ?) as tmp)', [jid, jid, offset]);
                        }
                    }
                    catch (error) {
                        console.error(`[store-mysql] Failed to clear messages for JID ${jid}:`, error);
                    }
                })
                    .finally(() => {
                    if (self.writeQueues.get(jid) === current) {
                        self.writeQueues.delete(jid);
                    }
                });
                self.writeQueues.set(jid, current);
                return;
            }
            if (self.fallbackStore) {
                if (offset === 0) {
                    delete self.fallbackStore[jid];
                }
                else {
                    const currentList = self.fallbackStore[jid] || [];
                    if (offset < currentList.length) {
                        self.fallbackStore[jid] = currentList.slice(0, offset);
                    }
                }
            }
        };
        return promiseWithMethods;
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
    async updateMessageWithReceipt(msg, receipt) {
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
        if (jid && id) {
            const list = await this.getMySQLData(jid);
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
            }
            if (this.pool) {
                const previous = this.writeQueues.get(jid) || Promise.resolve();
                const current = previous
                    .then(async () => {
                    try {
                        await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)', [jid, id, JSON.stringify(msg), Date.now()]);
                    }
                    catch (error) {
                        console.error('[store-mysql] Failed to update receipt in MySQL:', error);
                    }
                })
                    .finally(() => {
                    if (this.writeQueues.get(jid) === current) {
                        this.writeQueues.delete(jid);
                    }
                });
                this.writeQueues.set(jid, current);
            }
        }
    }
    async updateMessageWithReaction(msg, reaction) {
        if (!msg)
            return;
        const authorID = (0, utils_js_1.getKeyAuthor)(reaction.key);
        msg.reactions = (msg.reactions || []).filter((r) => (0, utils_js_1.getKeyAuthor)(r.key) !== authorID);
        if (reaction.text)
            msg.reactions.push(reaction);
        const jid = msg.key?.remoteJid;
        const id = msg.key?.id || msg.id;
        if (jid && id) {
            const list = await this.getMySQLData(jid);
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
            }
            if (this.pool) {
                const previous = this.writeQueues.get(jid) || Promise.resolve();
                const current = previous
                    .then(async () => {
                    try {
                        await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)', [jid, id, JSON.stringify(msg), Date.now()]);
                    }
                    catch (error) {
                        console.error('[store-mysql] Failed to update reaction in MySQL:', error);
                    }
                })
                    .finally(() => {
                    if (this.writeQueues.get(jid) === current) {
                        this.writeQueues.delete(jid);
                    }
                });
                this.writeQueues.set(jid, current);
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
//# sourceMappingURL=store-mysql.js.map