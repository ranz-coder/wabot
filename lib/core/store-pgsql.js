"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = __importDefault(require("node:path"));
const utils_js_1 = require("../utils.js");
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
        this.chatsCache = new Map();
        this.isChatsPreloaded = false;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.uri = uri || process.env.USE_STORE;
        this.database = 'pgsql';
        if (process.env?.USE_STORE?.includes('pg')) {
            this.initDB();
        }
        else {
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    async initDB() {
        const Pool = await loadPG();
        if (!Pool) {
            console.warn('[message-store-pg] pg module not installed! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-pg] PostgreSQL URI not provided! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
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
            );
            CREATE TABLE IF NOT EXISTS chats (
               id VARCHAR(255) NOT NULL,
               data TEXT NOT NULL,
               PRIMARY KEY (id)
            );
         `);
            await this.preloadChats();
        }
        catch (error) {
            console.error('[message-store-pg] Failed to initialize PostgreSQL. Falling back to RAM-only mode:', error);
            this.pool = null;
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
    }
    async preloadChats() {
        if (!this.pool)
            return;
        try {
            const { rows } = await this.pool.query('SELECT id, data FROM chats');
            for (const row of rows) {
                this.chatsCache.set(row.id, JSON.parse(row.data));
            }
            this.isChatsPreloaded = true;
        }
        catch (error) {
            console.error('[message-store-pg] Failed to preload chats:', error);
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
    get chats() {
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
                    self.pool.query('INSERT INTO chats (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data', [prop, JSON.stringify(value)]).catch(() => { });
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
    async loadMessage(jid, id) {
        if (this.pool) {
            try {
                const { rows } = await this.pool.query('SELECT data FROM messages WHERE jid = $1 AND id = $2', [jid, id]);
                return rows.length > 0 ? JSON.parse(rows[0].data) : null;
            }
            catch (error) {
                console.error(`[message-store-pg] Failed to load message ${id} for JID ${jid}:`, error);
                return null;
            }
        }
        if (this.fallbackStore) {
            const list = this.fallbackStore[jid] || [];
            return list.find(v => v.key?.id === id || v.id === id) || null;
        }
        return null;
    }
    async loadMessages(jid, count) {
        if (this.pool) {
            try {
                let query = 'SELECT data FROM messages WHERE jid = $1 ORDER BY created_at DESC';
                const params = [jid];
                if (count !== undefined && count > 0) {
                    query += ' LIMIT $2';
                    params.push(count);
                }
                const { rows } = await this.pool.query(query, params);
                if (rows.length === 0)
                    return null;
                return rows.map((row) => JSON.parse(row.data));
            }
            catch (error) {
                console.error(`[message-store-pg] Failed to load messages for JID ${jid}:`, error);
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
    async addMessage(jid, msg) {
        const msgId = msg.key?.id || msg.id;
        if (this.pool && msgId) {
            try {
                await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at', [jid, msgId, JSON.stringify(msg), Date.now()]);
                await this.pool.query('DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at DESC LIMIT $3)', [jid, jid, this.max]);
            }
            catch (error) {
                console.error('[message-store-pg] Failed to save message to PG:', error);
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
        const self = this;
        const promise = (async () => {
            let list = [];
            if (self.pool) {
                try {
                    const { rows } = await self.pool.query('SELECT data FROM messages WHERE jid = $1 ORDER BY created_at ASC OFFSET $2', [jid, offset]);
                    list = rows.map((row) => JSON.parse(row.data));
                }
                catch (error) {
                    console.error(`[message-store-pg] Failed to get messages for JID ${jid}:`, error);
                }
            }
            else if (self.fallbackStore) {
                const rawList = self.fallbackStore[jid] || [];
                list = offset > 0 ? rawList.slice(offset) : rawList;
            }
            const sliced = list;
            sliced.count = async () => {
                if (self.pool) {
                    try {
                        const { rows } = await self.pool.query('SELECT COUNT(*) as count FROM messages WHERE jid = $1', [jid]);
                        const total = parseInt(rows[0]?.count || '0', 10);
                        return Math.max(0, total - offset);
                    }
                    catch (error) {
                        console.error(`[message-store-pg] Failed to count messages for JID ${jid}:`, error);
                        return 0;
                    }
                }
                if (self.fallbackStore) {
                    const total = (self.fallbackStore[jid] || []).length;
                    return Math.max(0, total - offset);
                }
                return 0;
            };
            sliced.clear = async () => {
                if (self.pool) {
                    try {
                        if (offset === 0) {
                            await self.pool.query('DELETE FROM messages WHERE jid = $1', [jid]);
                        }
                        else {
                            await self.pool.query('DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at ASC LIMIT $3)', [jid, jid, offset]);
                        }
                    }
                    catch (error) {
                        console.error(`[message-store-pg] Failed to clear messages for JID ${jid}:`, error);
                    }
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
                try {
                    const { rows } = await self.pool.query('SELECT COUNT(*) as count FROM messages WHERE jid = $1', [jid]);
                    const total = parseInt(rows[0]?.count || '0', 10);
                    return Math.max(0, total - offset);
                }
                catch (error) {
                    console.error(`[message-store-pg] Failed to count messages for JID ${jid}:`, error);
                    return 0;
                }
            }
            if (self.fallbackStore) {
                const total = (self.fallbackStore[jid] || []).length;
                return Math.max(0, total - offset);
            }
            return 0;
        };
        promiseWithMethods.clear = async () => {
            if (self.pool) {
                try {
                    if (offset === 0) {
                        await self.pool.query('DELETE FROM messages WHERE jid = $1', [jid]);
                    }
                    else {
                        await self.pool.query('DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at ASC LIMIT $3)', [jid, jid, offset]);
                    }
                }
                catch (error) {
                    console.error(`[message-store-pg] Failed to clear messages for JID ${jid}:`, error);
                }
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
        if (this.pool && jid && id) {
            try {
                await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at', [jid, id, JSON.stringify(msg), Date.now()]);
            }
            catch (error) {
                console.error('[message-store-pg] Failed to update receipt in PG:', error);
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
        if (this.pool && jid && id) {
            try {
                await this.pool.query('INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at', [jid, id, JSON.stringify(msg), Date.now()]);
            }
            catch (error) {
                console.error('[message-store-pg] Failed to update reaction in PG:', error);
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
        if (this.fallbackStore) {
            Object.values(this.fallbackStore).forEach((msgArray) => {
                if (msgArray && msgArray.length > 100) {
                    msgArray.splice(0, msgArray.length - 100);
                }
            });
        }
        Object.values(this.stories).forEach((storyArray) => {
            if (storyArray && storyArray.length > 30) {
                storyArray.splice(0, storyArray.length - 30);
            }
        });
    }
}
const store = new Store('stores');
exports.default = store;
//# sourceMappingURL=store-pgsql.js.map