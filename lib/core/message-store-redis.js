"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_path_1 = __importDefault(require("node:path"));
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
class MessageStore {
    constructor(dir = 'messages', max = 250, uri) {
        this.redis = null;
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.uri = uri || process.env.USE_STORE;
        this._messages = Object.create(null);
        this.loadedJids = new Set();
        this.loadingJids = new Set();
        this.maxCachedJids = 50;
        this.dirtyJids = new Set();
        this.isSaving = false;
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
        setInterval(() => this.checkAndSave(), 15000);
    }
    async initDB() {
        const RedisModule = await loadRedis();
        if (!RedisModule || !RedisModule.createClient) {
            console.warn('[message-store-redis] Redis module not installed! Running in RAM-only mode.');
            return;
        }
        if (!this.uri) {
            console.warn('[message-store-redis] Redis URI not provided! Running in RAM-only mode.');
            return;
        }
        if (this.redis) {
            try {
                await this.redis.disconnect();
            }
            catch (e) { }
        }
        try {
            this.redis = RedisModule.createClient({ url: this.uri });
            this.redis.on('error', (err) => {
                console.error('[message-store-redis] Redis Client Error:', err);
            });
            await this.redis.connect();
        }
        catch (error) {
            console.error('[message-store-redis] Failed to initialize Redis:', error);
            this.redis = null;
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
        if (!this.redis)
            return;
        this.loadingJids.add(jid);
        try {
            const raw = await this.redis.get(`msg_store:${jid}`);
            if (raw) {
                const history = JSON.parse(raw);
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
        }
        catch (error) {
            console.error(`[message-store-redis] Failed to load JID ${jid} from Redis:`, error);
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
                if (this.dirtyJids.has(oldJid) || this.loadingJids.has(oldJid))
                    continue;
                delete this._messages[oldJid];
                this.loadedJids.delete(oldJid);
                break;
            }
        }
    }
    async checkAndSave() {
        if (this.isSaving || this.dirtyJids.size === 0 || !this.redis)
            return;
        this.isSaving = true;
        const jidsToSave = Array.from(this.dirtyJids);
        this.dirtyJids.clear();
        try {
            const multi = this.redis.multi();
            jidsToSave.forEach((jid) => {
                if (this.loadingJids.has(jid)) {
                    this.dirtyJids.add(jid);
                    return;
                }
                const data = this._messages[jid];
                if (data) {
                    multi.set(`msg_store:${jid}`, JSON.stringify(data));
                }
            });
            await multi.exec();
        }
        catch (error) {
            console.error('[message-store-redis] Failed to save messages to Redis:', error);
        }
        finally {
            this.isSaving = false;
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
        if (this._messages[jid].length > this.max) {
            this._messages[jid].splice(0, this._messages[jid].length - this.max);
        }
        this.dirtyJids.add(jid);
        this.touchJid(jid);
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-redis.js.map