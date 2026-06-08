"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
class MessageStore {
    constructor(dir = 'messages', max = 250) {
        this.client = null;
        this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
        this.max = max;
        this.maxCachedJids = 50;
        if (!node_fs_1.default.existsSync(this.storeDir)) {
            node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
        }
        this._messages = Object.create(null);
        this.loadedJids = new Set();
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
    config({ dir, max }) {
        if (dir) {
            this.storeDir = node_path_1.default.join(process.cwd(), '.cache', dir);
            if (!node_fs_1.default.existsSync(this.storeDir)) {
                node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
            }
        }
        if (max !== undefined) {
            this.max = max;
        }
        return this;
    }
    bind(client) {
        this.client = client;
        client.loadMessage = this.loadMessage.bind(this);
        client.loadMessages = this.loadMessages.bind(this);
        client.addMessage = this.addMessage.bind(this);
        client.messages = this.messages;
        return client;
    }
    getFilePath(jid) {
        const safeJid = jid.replace(/[^a-zA-Z0-9.-]/g, '_');
        return node_path_1.default.join(this.storeDir, `${safeJid}.json`);
    }
    loadJidData(jid) {
        if (this._messages[jid])
            return;
        const filePath = this.getFilePath(jid);
        if (!node_fs_1.default.existsSync(filePath)) {
            this._messages[jid] = [];
            return;
        }
        try {
            const fileContent = node_fs_1.default.readFileSync(filePath, 'utf-8');
            this._messages[jid] = JSON.parse(fileContent);
        }
        catch (error) {
            console.error(`[message-store-json] Failed to load JID ${jid} from JSON, creating backup:`, error);
            const backupPath = `${filePath}.corrupt-${Date.now()}`;
            try {
                node_fs_1.default.renameSync(filePath, backupPath);
            }
            catch { }
            this._messages[jid] = [];
        }
    }
    touchJid(jid) {
        this.loadedJids.delete(jid);
        this.loadedJids.add(jid);
        if (this.loadedJids.size > this.maxCachedJids) {
            for (const oldJid of this.loadedJids) {
                if (this.dirtyJids.has(oldJid))
                    continue;
                delete this._messages[oldJid];
                this.loadedJids.delete(oldJid);
                break;
            }
        }
    }
    async checkAndSave() {
        if (this.isSaving || this.dirtyJids.size === 0)
            return;
        this.isSaving = true;
        const jidsToSave = Array.from(this.dirtyJids);
        this.dirtyJids.clear();
        try {
            const savePromises = jidsToSave.map(async (jid) => {
                const data = this._messages[jid];
                if (!data)
                    return;
                const filePath = this.getFilePath(jid);
                const tempFilePath = `${filePath}.tmp`;
                const jsonStr = JSON.stringify(data);
                await node_fs_1.default.promises.writeFile(tempFilePath, jsonStr, 'utf-8');
                await node_fs_1.default.promises.rename(tempFilePath, filePath);
            });
            await Promise.all(savePromises);
        }
        catch (error) {
            console.error('[message-store-json] Error saving messages:', error);
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
//# sourceMappingURL=message-store-json.js.map