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
        if (!node_fs_1.default.existsSync(this.storeDir)) {
            node_fs_1.default.mkdirSync(this.storeDir, { recursive: true });
        }
        this.messages = Object.create(null);
        this.dirtyJids = new Set();
        this.isSaving = false;
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
        if (this.messages[jid])
            return;
        const filePath = this.getFilePath(jid);
        if (!node_fs_1.default.existsSync(filePath)) {
            this.messages[jid] = [];
            return;
        }
        try {
            const fileContent = node_fs_1.default.readFileSync(filePath, 'utf-8');
            this.messages[jid] = JSON.parse(fileContent);
        }
        catch (error) {
            const backupPath = `${filePath}.corrupt-${Date.now()}`;
            try {
                node_fs_1.default.renameSync(filePath, backupPath);
            }
            catch { }
            this.messages[jid] = [];
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
                const data = this.messages[jid];
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
            console.error('Error saving messages:', error);
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
//# sourceMappingURL=memory-store-json.js.map