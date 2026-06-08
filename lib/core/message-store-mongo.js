"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_path_1 = __importDefault(require("node:path"));
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
class MessageStore {
    constructor(dir = 'messages', max = 250, uri) {
        this.mongoClient = null;
        this.db = null;
        this.messagesCollection = null;
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
        const MongoClient = await loadMongo();
        if (!MongoClient) {
            console.warn('[message-store-mongodb] mongodb module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-mongodb] MongoDB URI not provided! Running in RAM-only mode.');
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
            await this.messagesCollection.createIndex({ jid: 1, id: 1 }, { unique: true });
        }
        catch (error) {
            console.error('[message-store-mongodb] Failed to initialize MongoDB:', error);
            this.mongoClient = null;
            this.db = null;
            this.messagesCollection = null;
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
        if (!this.messagesCollection)
            return;
        this.loadingJids.add(jid);
        try {
            const docs = await this.messagesCollection.find({ jid }).sort({ created_at: 1 }).toArray();
            const history = docs.map((doc) => doc.data);
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
            console.error(`[message-store-mongodb] Failed to load JID ${jid} from MongoDB:`, error);
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
        if (msgId && this.messagesCollection) {
            this.messagesCollection.updateOne({ jid, id: msgId }, { $set: { data: msg, created_at: Date.now() } }, { upsert: true }).then(async () => {
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
            }).catch((error) => {
                console.error('[message-store-mongodb] Failed to save message to MongoDB:', error);
            });
        }
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-mongo.js.map