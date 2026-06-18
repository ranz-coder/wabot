"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = __importDefault(require("node:path"));
const utils_js_1 = require("../utils.js");
let RedisConstructor = null;
const loadRedis = async () => {
    if (RedisConstructor)
        return RedisConstructor;
    try {
        const moduleName = String('redis');
        const module = await import(moduleName);
        RedisConstructor = module.createClient ? module : (module.default || module);
        return RedisConstructor;
    }
    catch (e) {
        return null;
    }
};
class Store {
    constructor(dir = 'stores', max = 250, uri) {
        this.redis = null;
        this.fallbackStore = null;
        this.fallbackChats = null;
        this.fallbackContacts = null;
        this.contactsCache = new Map();
        this.stories = Object.create(null);
        this.presences = Object.create(null);
        this.state = { connection: 'close' };
        this.messageId = new Map();
        this.cache = new Map();
        this.maxCachedJids = 10;
        this.pendingJidWrites = new Set();
        this.writeQueues = new Map();
        this.chatsCache = new Map();
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.uri = uri || process.env.USE_STORE;
        this.database = 'redis';
        this.fallbackStore = Object.create(null);
        this.fallbackChats = Object.create(null);
        this.fallbackContacts = Object.create(null);
        this.chatsProxyInstance = this.createChatsProxy();
        this.contactsProxyInstance = this.createContactsProxy();
        if (process.env?.USE_STORE?.includes('redis')) {
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
        const RedisModule = await loadRedis();
        if (!RedisModule || (!RedisModule.createClient && !RedisModule.default?.createClient)) {
            console.warn('[store-redis] Redis module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[store-redis] Redis URI not provided! Running in RAM-only mode.');
            return;
        }
        if (this.redis) {
            try {
                await this.redis.disconnect();
            }
            catch (e) { }
        }
        try {
            const createClient = RedisModule.createClient || RedisModule.default?.createClient;
            this.redis = createClient({ url: this.uri });
            this.redis.on('error', (err) => {
                console.error('[store-redis] Redis Client Error:', err);
            });
            await this.redis.connect();
            await this.preloadChats();
            await this.preloadContacts();
            this.fallbackStore = null;
            this.fallbackChats = null;
            this.fallbackContacts = null;
        }
        catch (error) {
            console.error('[store-redis] Failed to initialize Redis. Falling back to RAM-only mode:', error);
            this.redis = null;
        }
    }
    async preloadChats() {
        if (!this.redis)
            return;
        try {
            let cursor = '0';
            const reply = await this.redis.scan(cursor, { MATCH: 'chat_store:*', COUNT: 500 });
            const keys = reply.keys;
            if (keys && keys.length > 0) {
                for (const key of keys) {
                    const raw = await this.redis.get(key);
                    if (raw) {
                        const id = key.replace('chat_store:', '');
                        this.chatsCache.set(id, JSON.parse(raw));
                    }
                }
            }
        }
        catch (error) {
            console.error('[store-redis] Failed to preload chats:', error);
        }
    }
    async preloadContacts() {
        if (!this.redis)
            return;
        try {
            let cursor = '0';
            const reply = await this.redis.scan(cursor, { MATCH: 'contact_store:*', COUNT: 1000 });
            const keys = reply.keys;
            if (keys && keys.length > 0) {
                for (const key of keys) {
                    const raw = await this.redis.get(key);
                    if (raw) {
                        const jid = key.replace('contact_store:', '');
                        this.contactsCache.set(jid, JSON.parse(raw));
                    }
                }
            }
        }
        catch (error) {
            console.error('[store-redis] Failed to preload contacts:', error);
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
                const cleanedValue = self.toPOJO(value);
                self.chatsCache.set(prop, cleanedValue);
                if (self.redis) {
                    self.redis.set(`chat_store:${prop}`, JSON.stringify(cleanedValue)).catch(() => { });
                }
                else if (self.fallbackChats) {
                    self.fallbackChats[prop] = cleanedValue;
                }
                return true;
            },
            ownKeys: () => {
                return self.redis ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : []);
            },
            getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
        });
    }
    createContactsProxy() {
        const self = this;
        return new Proxy(Object.create(null), {
            get: (target, prop) => {
                if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop))
                    return undefined;
                return self.contactsCache.get(prop) || self.fallbackContacts?.[prop];
            },
            set: (target, prop, value) => {
                if (typeof prop !== 'string')
                    return false;
                const cleanedValue = self.toPOJO(value);
                self.contactsCache.set(prop, cleanedValue);
                if (self.redis) {
                    self.redis.set(`contact_store:${prop}`, JSON.stringify(cleanedValue)).catch(() => { });
                }
                else if (self.fallbackContacts) {
                    self.fallbackContacts[prop] = cleanedValue;
                }
                return true;
            },
            ownKeys: () => {
                return self.redis ? Array.from(self.contactsCache.keys()) : (self.fallbackContacts ? Object.keys(self.fallbackContacts) : []);
            },
            getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
        });
    }
    get chats() {
        return this.chatsProxyInstance;
    }
    get contacts() {
        return this.contactsProxyInstance;
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
                if (this.pendingJidWrites.has(key))
                    continue;
                this.cache.delete(key);
                if (this.cache.size <= this.maxCachedJids)
                    break;
            }
        }
    }
    async getRedisData(jid) {
        if (this.cache.has(jid)) {
            this.touchJid(jid);
            return this.cache.get(jid);
        }
        if (!this.redis)
            return [];
        try {
            const raw = await this.redis.get(`msg_store:${jid}`);
            const data = raw ? JSON.parse(raw) : [];
            this.cache.set(jid, data);
            this.evictOldestCache();
            return data;
        }
        catch (error) {
            console.error(`[store-redis] Failed to load JID ${jid} from Redis:`, error);
            return [];
        }
    }
    async setRedisData(jid, data) {
        this.cache.set(jid, data);
        this.touchJid(jid);
        this.evictOldestCache();
        if (this.pendingJidWrites.has(jid))
            return;
        this.pendingJidWrites.add(jid);
        setTimeout(() => {
            this.pendingJidWrites.delete(jid);
            const currentData = this.cache.get(jid);
            if (!currentData || !this.redis)
                return;
            const previous = this.writeQueues.get(jid) || Promise.resolve();
            const current = previous
                .then(async () => {
                try {
                    const cleanData = this.toPOJO(currentData);
                    await this.redis.set(`msg_store:${jid}`, JSON.stringify(cleanData));
                }
                catch (error) {
                    console.error(`[store-redis] Failed to save JID ${jid} to Redis:`, error);
                }
            })
                .finally(() => {
                if (this.writeQueues.get(jid) === current) {
                    this.writeQueues.delete(jid);
                }
            });
            this.writeQueues.set(jid, current);
        }, 1500);
    }
    async loadMessage(jid, id) {
        const list = await this.getRedisData(jid);
        return list.find(v => v.key?.id === id || v.id === id) || null;
    }
    async loadMessages(jid, count) {
        const list = await this.getRedisData(jid);
        if (list.length === 0)
            return null;
        const slice = count ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    async addMessage(jid, msg) {
        const list = await this.getRedisData(jid);
        list.push(msg);
        if (list.length > this.max) {
            list.splice(0, list.length - this.max);
        }
        await this.setRedisData(jid, list);
    }
    getAllMessages(jid, offset = 0) {
        const self = this;
        const promise = (async () => {
            let list = [];
            if (self.redis) {
                list = await self.getRedisData(jid);
            }
            else if (self.fallbackStore) {
                list = self.fallbackStore[jid] || [];
            }
            const sliced = (offset > 0 ? list.slice(offset) : list);
            sliced.count = async () => {
                if (self.redis) {
                    const currentList = await self.getRedisData(jid);
                    return Math.max(0, currentList.length - offset);
                }
                if (self.fallbackStore) {
                    const currentList = self.fallbackStore[jid] || [];
                    return Math.max(0, currentList.length - offset);
                }
                return 0;
            };
            sliced.clear = async () => {
                self.pendingJidWrites.delete(jid);
                self.cache.delete(jid);
                if (self.redis) {
                    if (offset === 0) {
                        try {
                            await self.redis.del(`msg_store:${jid}`);
                        }
                        catch (error) {
                            console.error(`[store-redis] Failed to clear JID ${jid} from Redis:`, error);
                        }
                    }
                    else {
                        const currentList = await self.getRedisData(jid);
                        if (offset < currentList.length) {
                            const updated = currentList.slice(0, offset);
                            await self.setRedisData(jid, updated);
                        }
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
            if (self.redis) {
                const currentList = await self.getRedisData(jid);
                return Math.max(0, currentList.length - offset);
            }
            if (self.fallbackStore) {
                const currentList = self.fallbackStore[jid] || [];
                return Math.max(0, currentList.length - offset);
            }
            return 0;
        };
        promiseWithMethods.clear = async () => {
            self.pendingJidWrites.delete(jid);
            self.cache.delete(jid);
            if (self.redis) {
                if (offset === 0) {
                    try {
                        await self.redis.del(`msg_store:${jid}`);
                    }
                    catch (error) {
                        console.error(`[store-redis] Failed to clear JID ${jid} from Redis:`, error);
                    }
                }
                else {
                    const currentList = await self.getRedisData(jid);
                    if (offset < currentList.length) {
                        const updated = currentList.slice(0, offset);
                        await self.setRedisData(jid, updated);
                    }
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
            this.contactsCache.clear();
            if (offset === 0) {
                if (this.redis) {
                    this.redis.scan('0', { MATCH: 'contact_store:*' }).then(async (reply) => {
                        const keys = reply.keys;
                        if (keys && keys.length > 0) {
                            await Promise.all(keys.map((k) => this.redis.del(k)));
                        }
                    }).catch(() => { });
                }
                if (this.fallbackContacts) {
                    this.fallbackContacts = Object.create(null);
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
        if (jid) {
            const list = await this.getRedisData(jid);
            const id = msg.key?.id || msg.id;
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
                await this.setRedisData(jid, list);
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
        if (jid) {
            const list = await this.getRedisData(jid);
            const id = msg.key?.id || msg.id;
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
                await this.setRedisData(jid, list);
            }
        }
    }
    async loadStories(jid, count) {
        if (this.redis) {
            try {
                const reply = await this.redis.scan('0', { MATCH: `story_store:${jid}:*`, COUNT: 100 });
                const keys = reply.keys;
                if (!keys || keys.length === 0)
                    return null;
                const loadPromises = keys.map((k) => this.redis.get(k));
                const raws = await Promise.all(loadPromises);
                const stories = raws.filter(Boolean).map(r => JSON.parse(r));
                stories.sort((a, b) => (b.created_at || 0) - (a.created_at || 0));
                return count ? stories.slice(0, count).reverse() : stories.reverse();
            }
            catch {
                return null;
            }
        }
        const list = this.stories[jid];
        if (!list || list.length === 0)
            return null;
        const slice = count && count > 0 ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    async loadStory(jid, id) {
        if (this.redis) {
            try {
                const raw = await this.redis.get(`story_store:${jid}:${id}`);
                return raw ? JSON.parse(raw) : null;
            }
            catch {
                return null;
            }
        }
        const list = this.stories[jid];
        if (!list || list.length === 0)
            return null;
        return list.find((v) => v.key?.id === id || v.id === id) || null;
    }
    async addStory(jid, story) {
        const storyId = story.key?.id || story.id;
        if (!storyId)
            return;
        if (this.redis) {
            try {
                await this.redis.set(`story_store:${jid}:${storyId}`, JSON.stringify(this.toPOJO(story)), { EX: 86400 });
            }
            catch { }
            return;
        }
        if (!this.stories[jid]) {
            this.stories[jid] = [];
        }
        this.stories[jid].push(story);
        if (this.stories[jid].length > this.max) {
            this.stories[jid].splice(0, this.stories[jid].length - this.max);
        }
    }
    async getAllStories(jid, offset = 0) {
        let list = [];
        if (this.redis) {
            try {
                const reply = await this.redis.scan('0', { MATCH: `story_store:${jid}:*`, COUNT: 100 });
                const keys = reply.keys;
                if (keys && keys.length > 0) {
                    const raws = await Promise.all(keys.map((k) => this.redis.get(k)));
                    list = raws.filter(Boolean).map(r => JSON.parse(r));
                    list.sort((a, b) => (b.created_at || 0) - (a.created_at || 0)).reverse();
                }
            }
            catch { }
        }
        else {
            list = this.stories[jid] || [];
        }
        const sliced = (offset > 0 ? list.slice(offset) : list);
        sliced.count = async () => {
            if (this.redis) {
                try {
                    const reply = await this.redis.scan('0', { MATCH: `story_store:${jid}:*`, COUNT: 100 });
                    const total = reply.keys?.length || 0;
                    return Math.max(0, total - offset);
                }
                catch {
                    return 0;
                }
            }
            const currentList = this.stories[jid] || [];
            return Math.max(0, currentList.length - offset);
        };
        sliced.clear = async () => {
            if (this.redis) {
                try {
                    const reply = await this.redis.scan('0', { MATCH: `story_store:${jid}:*` });
                    const keys = reply.keys;
                    if (keys && keys.length > 0) {
                        await Promise.all(keys.map((k) => this.redis.del(k)));
                    }
                }
                catch { }
            }
            else {
                if (offset === 0) {
                    delete this.stories[jid];
                }
                else {
                    const currentList = this.stories[jid] || [];
                    if (offset < currentList.length) {
                        this.stories[jid] = currentList.slice(0, offset);
                    }
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
        if (!this.redis) {
            Object.values(this.stories).forEach((storyArray) => {
                if (storyArray && storyArray.length > 30) {
                    storyArray.splice(0, storyArray.length - 30);
                }
            });
        }
    }
}
const store = new Store('stores');
exports.default = store;
//# sourceMappingURL=store-redis.js.map