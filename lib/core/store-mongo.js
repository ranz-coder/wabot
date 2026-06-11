"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = __importDefault(require("node:path"));
const utils_js_1 = require("../utils.js");
let MongoConstructor = null;
const loadMongo = async () => {
    if (MongoConstructor)
        return MongoConstructor;
    try {
        const moduleName = String('mongodb');
        const module = await import(moduleName);
        MongoConstructor = module.MongoClient || module.default?.MongoClient || module;
        return MongoConstructor;
    }
    catch (e) {
        return null;
    }
};
class Store {
    constructor(dir = 'stores', max = 250, uri) {
        this.mongoClient = null;
        this.db = null;
        this.messagesCollection = null;
        this.chatsCollection = null;
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
        this.database = 'mongodb';
        if (process.env?.USE_STORE?.includes('mongodb')) {
            this.initDB();
        }
        else {
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    async initDB() {
        const MongoClient = await loadMongo();
        if (!MongoClient) {
            console.warn('[message-store-mongodb] mongodb module not installed! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-mongodb] MongoDB URI not provided! Running in RAM-only mode.');
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
            return;
        }
        if (this.mongoClient) {
            try {
                await this.mongoClient.close();
            }
            catch (e) { }
        }
        try {
            this.mongoClient = new MongoClient(this.uri);
            await this.mongoClient.connect();
            this.db = this.mongoClient.db();
            this.messagesCollection = this.db.collection('messages');
            this.chatsCollection = this.db.collection('chats');
            await this.messagesCollection.createIndex({ jid: 1, id: 1 }, { unique: true });
            await this.chatsCollection.createIndex({ id: 1 }, { unique: true });
            await this.preloadChats();
        }
        catch (error) {
            console.error('[message-store-mongodb] Failed to initialize MongoDB. Falling back to RAM-only mode:', error);
            this.mongoClient = null;
            this.db = null;
            this.messagesCollection = null;
            this.chatsCollection = null;
            this.fallbackStore = Object.create(null);
            this.fallbackChats = Object.create(null);
        }
    }
    async preloadChats() {
        if (!this.chatsCollection)
            return;
        try {
            const docs = await this.chatsCollection.find({}).toArray();
            for (const doc of docs) {
                this.chatsCache.set(doc.id, doc.data);
            }
            this.isChatsPreloaded = true;
        }
        catch (error) {
            console.error('[message-store-mongodb] Failed to preload chats:', error);
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
                if (self.chatsCollection) {
                    self.chatsCollection.updateOne({ id: prop }, { $set: { data: value } }, { upsert: true }).catch(() => { });
                }
                else if (self.fallbackChats) {
                    self.fallbackChats[prop] = value;
                }
                return true;
            },
            ownKeys: () => {
                return self.chatsCollection ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : []);
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
        if (this.messagesCollection) {
            try {
                const doc = await this.messagesCollection.findOne({ jid, id });
                return doc ? doc.data : null;
            }
            catch (error) {
                console.error(`[message-store-mongodb] Failed to load message ${id} for JID ${jid}:`, error);
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
        if (this.messagesCollection) {
            try {
                let cursor = this.messagesCollection.find({ jid }).sort({ created_at: -1 });
                if (count !== undefined && count > 0) {
                    cursor = cursor.limit(count);
                }
                const docs = await cursor.toArray();
                if (docs.length === 0)
                    return null;
                return docs.map((doc) => doc.data);
            }
            catch (error) {
                console.error(`[message-store-mongodb] Failed to load messages for JID ${jid}:`, error);
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
        if (this.messagesCollection && msgId) {
            try {
                await this.messagesCollection.updateOne({ jid, id: msgId }, { $set: { data: msg, created_at: Date.now() } }, { upsert: true });
                const docsToKeep = await this.messagesCollection
                    .find({ jid })
                    .sort({ created_at: -1 })
                    .limit(this.max)
                    .project({ id: 1 })
                    .toArray();
                const keepIds = docsToKeep.map((d) => d.id);
                await this.messagesCollection.deleteMany({
                    jid,
                    id: { $nin: keepIds }
                });
            }
            catch (error) {
                console.error('[message-store-mongodb] Failed to save message to MongoDB:', error);
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
            if (self.messagesCollection) {
                try {
                    const docs = await self.messagesCollection
                        .find({ jid })
                        .sort({ created_at: 1 })
                        .skip(offset)
                        .toArray();
                    list = docs.map((doc) => doc.data);
                }
                catch (error) {
                    console.error(`[message-store-mongodb] Failed to get messages for JID ${jid}:`, error);
                }
            }
            else if (self.fallbackStore) {
                const rawList = self.fallbackStore[jid] || [];
                list = offset > 0 ? rawList.slice(offset) : rawList;
            }
            const sliced = list;
            sliced.count = async () => {
                if (self.messagesCollection) {
                    try {
                        const total = await self.messagesCollection.countDocuments({ jid });
                        return Math.max(0, total - offset);
                    }
                    catch (error) {
                        console.error(`[message-store-mongodb] Failed to count messages for JID ${jid}:`, error);
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
                if (self.messagesCollection) {
                    try {
                        if (offset === 0) {
                            await self.messagesCollection.deleteMany({ jid });
                        }
                        else {
                            const docsToKeep = await self.messagesCollection
                                .find({ jid })
                                .sort({ created_at: 1 })
                                .limit(offset)
                                .project({ id: 1 })
                                .toArray();
                            const keepIds = docsToKeep.map((d) => d.id);
                            await self.messagesCollection.deleteMany({ jid, id: { $nin: keepIds } });
                        }
                    }
                    catch (error) {
                        console.error(`[message-store-mongodb] Failed to clear messages for JID ${jid}:`, error);
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
            if (self.messagesCollection) {
                try {
                    const total = await self.messagesCollection.countDocuments({ jid });
                    return Math.max(0, total - offset);
                }
                catch (error) {
                    console.error(`[message-store-mongodb] Failed to count messages for JID ${jid}:`, error);
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
            if (self.messagesCollection) {
                try {
                    if (offset === 0) {
                        await self.messagesCollection.deleteMany({ jid });
                    }
                    else {
                        const docsToKeep = await self.messagesCollection
                            .find({ jid })
                            .sort({ created_at: 1 })
                            .limit(offset)
                            .project({ id: 1 })
                            .toArray();
                        const keepIds = docsToKeep.map((d) => d.id);
                        await self.messagesCollection.deleteMany({ jid, id: { $nin: keepIds } });
                    }
                }
                catch (error) {
                    console.error(`[message-store-mongodb] Failed to clear messages for JID ${jid}:`, error);
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
        if (this.messagesCollection && jid && id) {
            try {
                await this.messagesCollection.updateOne({ jid, id }, { $set: { data: msg, created_at: Date.now() } }, { upsert: true });
            }
            catch (error) {
                console.error('[message-store-mongodb] Failed to update receipt in MongoDB:', error);
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
        if (this.messagesCollection && jid && id) {
            try {
                await this.messagesCollection.updateOne({ jid, id }, { $set: { data: msg, created_at: Date.now() } }, { upsert: true });
            }
            catch (error) {
                console.error('[message-store-mongodb] Failed to update reaction in MongoDB:', error);
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
//# sourceMappingURL=store-mongo.js.map