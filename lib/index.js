"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.messages = void 0;
const message_store_json_js_1 = __importDefault(require("./core/message-store-json.js"));
const message_store_mysql_js_1 = __importDefault(require("./core/message-store-mysql.js"));
const message_store_mongo_js_1 = __importDefault(require("./core/message-store-mongo.js"));
const message_store_pgsql_js_1 = __importDefault(require("./core/message-store-pgsql.js"));
const message_store_redis_js_1 = __importDefault(require("./core/message-store-redis.js"));
const message_store_sqlite_js_1 = __importDefault(require("./core/message-store-sqlite.js"));
const store = process.env?.USE_STORE?.includes('mysql')
    ? message_store_mysql_js_1.default
    : process.env?.USE_STORE?.includes('mongo')
        ? message_store_mongo_js_1.default
        : (process.env?.USE_STORE?.includes('pgsql') || process.env?.USE_STORE?.includes('postgres'))
            ? message_store_pgsql_js_1.default
            : process.env?.USE_STORE?.includes('redis')
                ? message_store_redis_js_1.default
                : process.env?.USE_STORE?.includes('sqlite')
                    ? message_store_sqlite_js_1.default
                    : message_store_json_js_1.default;
exports.messages = store.messages;
exports.default = store;
//# sourceMappingURL=index.js.map