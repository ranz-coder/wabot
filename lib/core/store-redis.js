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
        this.database = 'redis';
        if (process.env?.USE_STORE?.includes('redis')) {
            this.initDB();
        }
        else {
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    async initDB() {
        const RedisModule = await loadRedis();
        if (!RedisModule || (!RedisModule.createClient && !RedisModule.default?.createClient)) {
            console.warn('[message-store-redis] Redis module not installed! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-redis] Redis URI not provided! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
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
                console.error('[message-store-redis] Redis Client Error:', err);
            });
            await this.redis.connect();
            await this.preloadChats();
        }
        catch (error) {
            console.error('[message-store-redis] Failed to initialize Redis. Falling back to RAM-only mode:', error);
            this.redis = null;
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
    }
    async preloadChats() {
        if (!this.redis)
            return;
        try {
            const keys = await this.redis.keys('chat_store:*');
            const loadPromises = keys.map(async (key) => {
                const raw = await this.redis.get(key);
                if (raw) {
                    const id = key.replace('chat_store:', '');
                    this.chatsCache.set(id, JSON.parse(raw));
                }
            });
            await Promise.all(loadPromises);
            this.isChatsPreloaded = true;
        }
        catch (error) {
            console.error('[message-store-redis] Failed to preload chats:', error);
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
                if (self.redis) {
                    self.redis.set(`chat_store:${prop}`, JSON.stringify(value)).catch(() => { });
                }
                else if (self.fallbackChats) {
                    self.fallbackChats[prop] = value;
                }
                return true;
            },
            ownKeys: () => {
                return self.redis ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : []);
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
    async getRedisData(jid) {
        if (!this.redis)
            return [];
        try {
            const raw = await this.redis.get(`msg_store:${jid}`);
            return raw ? JSON.parse(raw) : [];
        }
        catch (error) {
            console.error(`[message-store-redis] Failed to load JID ${jid} from Redis:`, error);
            return [];
        }
    }
    async setRedisData(jid, data) {
        if (!this.redis)
            return;
        try {
            await this.redis.set(`msg_store:${jid}`, JSON.stringify(data));
        }
        catch (error) {
            console.error(`[message-store-redis] Failed to save JID ${jid} to Redis:`, error);
        }
    }
    async loadMessage(jid, id) {
        if (this.redis) {
            const list = await this.getRedisData(jid);
            return list.find(v => v.key?.id === id || v.id === id) || null;
        }
        if (this.fallbackStore) {
            const list = this.fallbackStore[jid] || [];
            return list.find(v => v.key?.id === id || v.id === id) || null;
        }
        return null;
    }
    async loadMessages(jid, count) {
        if (this.redis) {
            const list = await this.getRedisData(jid);
            if (list.length === 0)
                return null;
            const slice = count ? list.slice(-count) : list;
            return [...slice].reverse();
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
        if (this.redis) {
            const list = await this.getRedisData(jid);
            list.push(msg);
            if (list.length > this.max) {
                list.splice(0, list.length - this.max);
            }
            await this.setRedisData(jid, list);
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
                if (self.redis) {
                    if (offset === 0) {
                        try {
                            await self.redis.del(`msg_store:${jid}`);
                        }
                        catch (error) {
                            console.error(`[message-store-redis] Failed to clear JID ${jid} from Redis:`, error);
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
            if (self.redis) {
                if (offset === 0) {
                    try {
                        await self.redis.del(`msg_store:${jid}`);
                    }
                    catch (error) {
                        console.error(`[message-store-redis] Failed to clear JID ${jid} from Redis:`, error);
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
        if (this.redis && jid) {
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
        if (this.redis && jid) {
            const list = await this.getRedisData(jid);
            const id = msg.key?.id || msg.id;
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
                await this.setRedisData(jid, list);
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
//# sourceMappingURL=store-redis.js.map