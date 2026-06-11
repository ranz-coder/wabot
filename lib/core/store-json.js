"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
const utils_js_1 = require("../utils.js");
class Store {
    constructor(dir = 'stores', max = 250) {
        this.cache = new Map();
        this.maxCachedJids = 15;
        this.writeTimeouts = new Map();
        this.fallbackStore = null;
        this.fallbackChats = null;
        this.contacts = Object.create(null);
        this.stories = Object.create(null);
        this.presences = Object.create(null);
        this.state = { connection: 'close' };
        this.messageId = new Map();
        this.chatsCache = new Map();
        this.chatsWriteTimeout = null;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.database = 'json';
        this.chatsFilePath = node_path_1.default.join(this.storeDir, 'chats.json');
        if (!node_fs_1.default.existsSync(this.storeDir)) {
            node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
        }
        this.loadChats();
        setInterval(() => this.cleanupExpiredMessages(), 120000);
    }
    loadChats() {
        if (node_fs_1.default.existsSync(this.chatsFilePath)) {
            try {
                const content = node_fs_1.default.readFileSync(this.chatsFilePath, 'utf-8');
                const list = JSON.parse(content);
                for (const chat of list) {
                    if (chat.id)
                        this.chatsCache.set(chat.id, chat);
                }
            }
            catch (error) {
                console.error('[message-store-json] Failed to load chats:', error);
            }
        }
    }
    writeChats() {
        if (this.chatsWriteTimeout) {
            clearTimeout(this.chatsWriteTimeout);
        }
        this.chatsWriteTimeout = setTimeout(async () => {
            this.chatsWriteTimeout = null;
            const list = Array.from(this.chatsCache.values());
            const tempPath = `${this.chatsFilePath}.tmp`;
            try {
                await node_fs_1.default.promises.writeFile(tempPath, JSON.stringify(list), 'utf-8');
                await node_fs_1.default.promises.rename(tempPath, this.chatsFilePath);
            }
            catch (error) {
                console.error('[message-store-json] Failed to write chats:', error);
            }
        }, 1000);
    }
    config({ dir, max }) {
        if (dir) {
            this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
            this.chatsFilePath = node_path_1.default.join(this.storeDir, 'chats.json');
            if (!node_fs_1.default.existsSync(this.storeDir)) {
                node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
            }
            this.loadChats();
        }
        if (max !== undefined) {
            this.max = max;
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
                self.writeChats();
                return true;
            },
            ownKeys: () => {
                return Array.from(self.chatsCache.keys());
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
    getFilePath(jid) {
        const safeJid = jid.replace(/[^a-zA-Z0-9.-]/g, '_');
        return node_path_1.default.join(this.storeDir, `${safeJid}.json`);
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
                if (this.writeTimeouts.has(key))
                    continue;
                this.cache.delete(key);
                if (this.cache.size <= this.maxCachedJids)
                    break;
            }
        }
    }
    readJidData(jid) {
        if (this.cache.has(jid)) {
            this.touchJid(jid);
            return this.cache.get(jid);
        }
        const filePath = this.getFilePath(jid);
        if (!node_fs_1.default.existsSync(filePath)) {
            return [];
        }
        try {
            const fileContent = node_fs_1.default.readFileSync(filePath, 'utf-8');
            const data = JSON.parse(fileContent);
            this.cache.set(jid, data);
            this.evictOldestCache();
            return data;
        }
        catch (error) {
            console.error(`[message-store-json] Failed to load JID ${jid} from JSON:`, error);
            return [];
        }
    }
    writeJidData(jid, data) {
        this.cache.set(jid, data);
        this.touchJid(jid);
        this.evictOldestCache();
        if (this.writeTimeouts.has(jid)) {
            clearTimeout(this.writeTimeouts.get(jid));
        }
        const timeout = setTimeout(async () => {
            this.writeTimeouts.delete(jid);
            const filePath = this.getFilePath(jid);
            const tempFilePath = `${filePath}.tmp`;
            try {
                const jsonStr = JSON.stringify(data);
                await node_fs_1.default.promises.writeFile(tempFilePath, jsonStr, 'utf-8');
                await node_fs_1.default.promises.rename(tempFilePath, filePath);
            }
            catch (error) {
                console.error(`[message-store-json] Failed to write JID ${jid} to JSON:`, error);
            }
        }, 1000);
        this.writeTimeouts.set(jid, timeout);
    }
    loadMessage(jid, id) {
        const list = this.readJidData(jid);
        return list.find(v => v.key?.id === id || v.id === id) || null;
    }
    loadMessages(jid, count) {
        const list = this.readJidData(jid);
        if (list.length === 0)
            return null;
        const slice = count ? list.slice(-count) : list;
        return [...slice].reverse();
    }
    addMessage(jid, msg) {
        const list = this.readJidData(jid);
        list.push(msg);
        if (list.length > this.max) {
            list.splice(0, list.length - this.max);
        }
        this.writeJidData(jid, list);
    }
    getAllMessages(jid, offset = 0) {
        const list = this.readJidData(jid);
        const sliced = (offset > 0 ? list.slice(offset) : list);
        const self = this;
        sliced.count = () => {
            const currentList = self.readJidData(jid);
            return Math.max(0, currentList.length - offset);
        };
        sliced.clear = () => {
            if (self.writeTimeouts.has(jid)) {
                clearTimeout(self.writeTimeouts.get(jid));
                self.writeTimeouts.delete(jid);
            }
            self.cache.delete(jid);
            if (offset === 0) {
                const filePath = self.getFilePath(jid);
                if (node_fs_1.default.existsSync(filePath)) {
                    try {
                        node_fs_1.default.unlinkSync(filePath);
                    }
                    catch (error) {
                        console.error(`[message-store-json] Failed to delete JSON file for JID ${jid}:`, error);
                    }
                }
            }
            else {
                const currentList = self.readJidData(jid);
                if (offset < currentList.length) {
                    const updated = currentList.slice(0, offset);
                    self.writeJidData(jid, updated);
                }
            }
        };
        return sliced;
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
        if (jid) {
            const list = this.readJidData(jid);
            const id = msg.key?.id || msg.id;
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
                this.writeJidData(jid, list);
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
        if (jid) {
            const list = this.readJidData(jid);
            const id = msg.key?.id || msg.id;
            const idx = list.findIndex(v => v.key?.id === id || v.id === id);
            if (idx !== -1) {
                list[idx] = msg;
                this.writeJidData(jid, list);
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
//# sourceMappingURL=store-json.js.map