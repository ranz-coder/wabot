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
        this.uri = uri || process.env.REDIS_URL;
        this.messages = Object.create(null);
        this.dirtyJids = new Set();
        this.isSaving = false;
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
            const keys = await this.redis.keys('msg_store:*');
            if (keys.length > 0) {
                const values = await this.redis.mGet(keys);
                keys.forEach((key, index) => {
                    const jid = key.replace('msg_store:', '');
                    if (values[index]) {
                        try {
                            this.messages[jid] = JSON.parse(values[index]);
                        }
                        catch {
                            this.messages[jid] = [];
                        }
                    }
                });
            }
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
        if (!this.messages[jid]) {
            this.messages[jid] = [];
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
                const data = this.messages[jid];
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
        if (this.messages[jid].length > this.max) {
            this.messages[jid].splice(0, this.messages[jid].length - this.max);
        }
        this.dirtyJids.add(jid);
    }
}
const store = new MessageStore('messages');
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=message-store-redis.js.map