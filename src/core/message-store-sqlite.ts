import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import fs from 'node:fs'
import path from 'node:path'

let DatabaseConstructor: any = null
const loadSqlite = async () => {
   if (DatabaseConstructor) return DatabaseConstructor
   try {
      const moduleName = String('better-sqlite3')
      const module = await import(moduleName)
      DatabaseConstructor = module.default || module
      return DatabaseConstructor
   } catch (e) {
      return null
   }
}

class MessageStore {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public messages: Record<string, WAMessage[]>

   private _messages: Record<string, WAMessage[]>
   private loadedJids: Set<string>
   private maxCachedJids: number
   private db: any = null
   private insertStmt: any = null
   private cleanupStmt: any = null
   private getAllStmt: any = null

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max

      this._messages = Object.create(null) as Record<string, WAMessage[]>
      this.loadedJids = new Set<string>()
      this.maxCachedJids = 50

      const self = this
      this.messages = new Proxy(this._messages, {
         get(target, prop, receiver) {
            if (typeof prop === 'string' && !['prototype', 'constructor', 'toJSON'].includes(prop)) {
               self.loadJidData(prop)
               self.touchJid(prop)
            }
            return Reflect.get(target, prop, receiver)
         },
         set(target, prop, value, receiver) {
            if (typeof prop === 'string' && !['prototype', 'constructor', 'toJSON'].includes(prop)) {
               self.touchJid(prop)
            }
            return Reflect.set(target, prop, value, receiver)
         },
         deleteProperty(target, prop) {
            if (typeof prop === 'string') {
               self.loadedJids.delete(prop)
            }
            return Reflect.deleteProperty(target, prop)
         }
      }) as Record<string, WAMessage[]>

      this.initDB()
   }

   private async initDB(): Promise<void> {
      const SQLite = await loadSqlite()

      if (!SQLite) {
         console.warn('[message-store-sqlite] better-sqlite3 module not installed! Running in RAM-only mode.')
         return
      }

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      const dbPath = path.join(this.storeDir, 'store.db')

      if (this.db) {
         this.db.close()
      }

      try {
         this.db = new SQLite(dbPath)
         this.db.pragma('journal_mode = WAL')

         this.db.exec(`
            CREATE TABLE IF NOT EXISTS messages (
               jid TEXT,
               id TEXT,
               data TEXT,
               created_at INTEGER,
               PRIMARY KEY (jid, id)
            )
         `)

         this.insertStmt = this.db.prepare('INSERT OR REPLACE INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?)')
         this.cleanupStmt = this.db.prepare(`
            DELETE FROM messages 
            WHERE jid = ? AND id NOT IN (
               SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?
            )
         `)
         this.getAllStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC')
      } catch (error) {
         console.error('[message-store-sqlite] Failed to initialize SQLite database:', error)
         this.db = null
      }
   }

   public config({ dir, max }: StoreConfig): this {
      let dbNeedsReinit = false

      if (dir) {
         const newDir = path.join(process.cwd(), '.cache', dir)
         if (this.storeDir !== newDir) {
            this.storeDir = newDir
            dbNeedsReinit = true
         }
      }

      if (max !== undefined) {
         this.max = max
      }

      if (dbNeedsReinit) {
         this.initDB()
      }

      return this
   }

   public bind<T extends BotClient>(client: T): T {
      this.client = client

      client.loadMessage = this.loadMessage.bind(this)
      client.loadMessages = this.loadMessages.bind(this)
      client.addMessage = this.addMessage.bind(this)
      client.messages = this.messages

      return client
   }

   private loadJidData(jid: string): void {
      if (this._messages[jid]) return

      if (!this.getAllStmt) {
         this._messages[jid] = []
         return
      }

      try {
         const rows = this.getAllStmt.all(jid) as { data: string }[]
         this._messages[jid] = rows.map(row => JSON.parse(row.data) as WAMessage)
      } catch (error) {
         console.error(`[message-store-sqlite] Failed to load JID ${jid} from SQLite:`, error)
         this._messages[jid] = []
      }
   }

   private touchJid(jid: string): void {
      this.loadedJids.delete(jid)
      this.loadedJids.add(jid)

      if (this.loadedJids.size > this.maxCachedJids) {
         for (const oldJid of this.loadedJids) {
            delete this._messages[oldJid]
            this.loadedJids.delete(oldJid)
            break
         }
      }
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      this.loadJidData(jid)
      this.touchJid(jid)
      return this._messages[jid]?.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public loadMessages(jid: string, count?: number): WAMessage[] | null {
      this.loadJidData(jid)
      this.touchJid(jid)
      const list = this._messages[jid]
      if (!list || list.length === 0) return null

      const slice = count ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public addMessage(jid: string, msg: WAMessage): void {
      this.loadJidData(jid)

      this._messages[jid].push(msg)

      const msgId = msg.key?.id || (msg as any).id

      if (this._messages[jid].length > this.max) {
         this._messages[jid].splice(0, this._messages[jid].length - this.max)
      }

      this.touchJid(jid)

      if (msgId && this.insertStmt && this.cleanupStmt) {
         try {
            this.insertStmt.run(jid, msgId, JSON.stringify(msg), Date.now())
            this.cleanupStmt.run(jid, jid, this.max)
         } catch (error) {
            console.error('[message-store-sqlite] Failed to save message to SQLite:', error)
         }
      }
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store