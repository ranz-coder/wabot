# @neoxr/message-store

> A lightweight message storage utility for WhatsApp bots built with Baileys. It maintains a RAM cache for immediate retrieval while persisting message history to a backend of your choice.

### ⌗ FEATURES

- **Multi-Backend Support**: Use standard JSON files by default, or opt-in to SQLite, Redis, MySQL, MongoDB or PostgreSQL.
- **Lazy Loading**: Messages are loaded from storage only when accessed, optimizing memory usage.
- **Optional Dependencies**: Heavy database drivers (better-sqlite3, redis, mysql2, pg, mongodb) are dynamically imported. Your project compiles successfully even if these databases are not installed.
- **Anti-Corruption Safeguards**: Uses Atomic Writes (temp files) for JSON, WAL mode for SQLite, and transaction/connection pools for relational databases.
Easy Integration: Hooks directly into the Baileys client instance with a single bind() call.

### ⌗ INSTALLATION

Install the core package:

```bash
yarn add @neoxr/message-store@github:neoxr/neoxr-bot#utils/message-store
```

Depending on the storage engine you plan to use, install the corresponding optional peer dependency:

```bash
# For SQLite
yarn add better-sqlite3

# For Redis
yarn add redis

# For MySQL
yarn add mysql2

# For MongoDB
yarn add mongodb

# For PostgreSQL
yarn add pg
```

### ⌗ CONFIGURATION

Import the specific storage engine you want to use. You can configure the cache limit (max) and directory/database settings dynamically via config().

```Typescript
export interface StoreConfig {
   /* Directory name inside '.cache' where file-based storage (JSON/SQLite) is kept */
   dir?: string
   /* Maximum number of messages to persist and cache in RAM per JID */
   max?: number
   /* Connection URI/string for network-based databases (Redis, MySQL, PostgreSQL) */
   uri?: string
}
```

Here is an example using JSON

```Javascript
import store from '@neoxr/message-store/lib/message-store-json.js'

store.config({
   dir: 'messages',
   max: 300
})
```

Or if you're using a cloud database like MongoDB, use the URI:

```Javascript
import store from '@neoxr/message-store/lib/message-store-mongo.js'

store.config({
   max: 300,
   uri: 'mongodb://localhost:27017/mydb'
})
```

### ⌗ USAGE EXAMPLE

Integrating the store into your Baileys connection:

```Javascript
import { makeWASocket } from '@whiskeysockets/baileys'
import store from '@neoxr/message-store/lib/message-store-json.js'

store.config({
   dir: 'messages',
   max: 300
})

async function connectToWA() {
   const client = makeWASocket({
      // Your Baileys configuration
   })

   // Bind store methods directly to the client instance
   store.bind(client)

   client.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return

      for (const msg of messages) {
         const jid = msg.key.remoteJid
         if (!jid) continue

         // Save incoming message
         client.addMessage(jid, msg)

         // Retrieve the single message
         const singleMsg = client.loadMessage(jid, msg.key.id)

         // Retrieve latest 10 messages from this chat
         const history = client.loadMessages(jid, 10)
      }
   })
}

connectToWA()
```

### ⌗ API REFERENCE

Once the store is bound to your `client` instance, the following methods become available:

#### `client.addMessage(jid: string, msg: WAMessage): void`
Saves a message to the RAM cache and pushes it to the persistent storage. Truncates older history once it exceeds the `max` configuration limit.

#### `client.loadMessage(jid: string, id: string): WAMessage | null`
Retrieves a specific message by its ID within a given chat JID.

#### `client.loadMessages(jid: string, count?: number): WAMessage[] | null`
Loads the latest `$count` messages from a specific JID. Returns the array in reverse order (newest message first).

#### `client.messages`
Exposes direct, read-only access to the internal RAM-cached message object: `Record<string, WAMessage[]>`.