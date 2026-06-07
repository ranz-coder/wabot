"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const memory_store_json_js_1 = __importDefault(require("./core/memory-store-json.js"));
const memory_store_sqlite_js_1 = __importDefault(require("./core/memory-store-sqlite.js"));
const store = process.env?.USE_STORE?.includes('sqlite')
    ? memory_store_sqlite_js_1.default
    : memory_store_json_js_1.default;
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=index.js.map