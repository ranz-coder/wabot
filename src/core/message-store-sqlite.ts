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
   public database: string

   private db: any = null
   private fallbackStore: Record<string, WAMessage[]> | null = null

   private insertStmt: any = null
   private cleanupStmt: any = null
   private getOneStmt: any = null
   private getLimitStmt: any = null
   private getAllDescStmt: any = null
   private getAllWithOffsetStmt: any = null
   private countStmt: any = null
   private deleteWithOffsetStmt: any = null

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.database = 'sqlite'

      if (process.env?.USE_STORE?.includes('sqlite')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
      }
   }

   private async initDB(): Promise<void> {
      const SQLite = await loadSqlite()

      if (!SQLite) {
         console.warn('[message-store-sqlite] better-sqlite3 module not installed! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
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
         this.getOneStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? AND id = ?')
         this.getLimitStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?')
         this.getAllDescStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC')
         this.getAllWithOffsetStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?')
         this.countStmt = this.db.prepare('SELECT COUNT(*) as count FROM messages WHERE jid = ?')
         this.deleteWithOffsetStmt = this.db.prepare(`
            DELETE FROM messages 
            WHERE jid = ? AND id IN (
               SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?
            )
         `)
      } catch (error) {
         console.error('[message-store-sqlite] Failed to initialize SQLite database. Falling back to RAM-only mode:', error)
         this.db = null
         this.fallbackStore = Object.create(null)
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
      client.getAllMessages = this.getAllMessages.bind(this)

      return client
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      if (this.db && this.getOneStmt) {
         try {
            const row = this.getOneStmt.get(jid, id) as { data: string } | undefined
            return row ? (JSON.parse(row.data) as WAMessage) : null
         } catch (error) {
            console.error(`[message-store-sqlite] Failed to load message ${id} for JID ${jid}:`, error)
            return null
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         return list.find(v => v.key?.id === id || (v as any).id === id) || null
      }

      return null
   }

   public loadMessages(jid: string, count?: number): WAMessage[] | null {
      if (this.db) {
         try {
            let rows: { data: string }[] = []

            if (count !== undefined && count > 0) {
               if (this.getLimitStmt) {
                  rows = this.getLimitStmt.all(jid, count) as { data: string }[]
               }
            } else {
               if (this.getAllDescStmt) {
                  rows = this.getAllDescStmt.all(jid) as { data: string }[]
               }
            }

            if (rows.length === 0) return null

            return rows.map(row => JSON.parse(row.data) as WAMessage)
         } catch (error) {
            console.error(`[message-store-sqlite] Failed to load messages for JID ${jid}:`, error)
            return null
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid]
         if (!list || list.length === 0) return null

         const slice = count ? list.slice(-count) : list
         return [...slice].reverse()
      }

      return null
   }

   public addMessage(jid: string, msg: WAMessage): void {
      if (this.db && this.insertStmt && this.cleanupStmt) {
         const msgId = msg.key?.id || (msg as any).id
         if (msgId) {
            try {
               this.insertStmt.run(jid, msgId, JSON.stringify(msg), Date.now())
               this.cleanupStmt.run(jid, jid, this.max)
            } catch (error) {
               console.error('[message-store-sqlite] Failed to save message to SQLite:', error)
            }
         }
         return
      }

      if (this.fallbackStore) {
         if (!this.fallbackStore[jid]) {
            this.fallbackStore[jid] = []
         }
         this.fallbackStore[jid].push(msg)

         if (this.fallbackStore[jid].length > this.max) {
            this.fallbackStore[jid].splice(0, this.fallbackStore[jid].length - this.max)
         }
      }
   }

   public getAllMessages(jid: string, offset: number = 0): WAMessage[] & { count(): number; clear(): void } {
      if (this.db && this.getAllWithOffsetStmt && this.countStmt && this.deleteWithOffsetStmt) {
         try {
            const rows = this.getAllWithOffsetStmt.all(jid, offset) as { data: string }[]
            const messages = rows.map(row => JSON.parse(row.data) as WAMessage) as WAMessage[] & { count(): number; clear(): void }

            messages.count = () => {
               try {
                  const result = this.countStmt.get(jid) as { count: number } | undefined
                  const total = result ? result.count : 0
                  return Math.max(0, total - offset)
               } catch (error) {
                  console.error('[message-store-sqlite] Failed to count messages:', error)
                  return 0
               }
            }

            messages.clear = () => {
               try {
                  this.deleteWithOffsetStmt.run(jid, jid, offset)
               } catch (error) {
                  console.error(`[message-store-sqlite] Failed to clear messages for JID ${jid} with offset ${offset}:`, error)
               }
            }

            return messages
         } catch (error) {
            console.error(`[message-store-sqlite] Failed to get all messages for JID ${jid}:`, error)
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): number; clear(): void }

         sliced.count = () => {
            const currentList = this.fallbackStore?.[jid] || []
            return Math.max(0, currentList.length - offset)
         }

         sliced.clear = () => {
            if (this.fallbackStore) {
               if (offset === 0) {
                  delete this.fallbackStore[jid]
               } else {
                  const currentList = this.fallbackStore[jid] || []
                  if (offset < currentList.length) {
                     this.fallbackStore[jid] = currentList.slice(0, offset)
                  }
               }
            }
         }

         return sliced
      }

      const emptyResult = [] as unknown as WAMessage[] & { count(): number; clear(): void }
      emptyResult.count = () => 0
      emptyResult.clear = () => { }
      return emptyResult
   }
}

const store = new MessageStore('messages')

export default store