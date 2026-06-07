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

   private db: any = null
   private insertStmt: any = null
   private cleanupStmt: any = null
   private getAllStmt: any = null

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.messages = Object.create(null) as Record<string, WAMessage[]>

      this.initDB()
   }

   private async initDB(): Promise<void> {
      const SQLite = await loadSqlite()

      if (!SQLite) {
         console.warn('[MessageStore] Module better-sqlite3 tidak terinstal! Berjalan di mode RAM-only.')
         return
      }

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      const dbPath = path.join(this.storeDir, 'store.db')

      if (this.db) {
         this.db.close()
      }

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
      if (this.messages[jid]) return

      if (!this.getAllStmt) {
         this.messages[jid] = []
         return
      }

      try {
         const rows = this.getAllStmt.all(jid) as { data: string }[]
         this.messages[jid] = rows.map(row => JSON.parse(row.data) as WAMessage)
      } catch (error) {
         console.error(`[MessageStore] Gagal meload JID ${jid} dari SQLite:`, error)
         this.messages[jid] = []
      }
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      this.loadJidData(jid)
      return this.messages[jid]?.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public loadMessages(jid: string, count?: number): WAMessage[] | null {
      this.loadJidData(jid)
      const list = this.messages[jid]
      if (!list || list.length === 0) return null

      const slice = count ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public addMessage(jid: string, msg: WAMessage): void {
      this.loadJidData(jid)

      this.messages[jid].push(msg)

      const msgId = msg.key?.id || (msg as any).id

      if (this.messages[jid].length > this.max) {
         this.messages[jid].splice(0, this.messages[jid].length - this.max)
      }

      if (msgId && this.insertStmt && this.cleanupStmt) {
         try {
            this.insertStmt.run(jid, msgId, JSON.stringify(msg), Date.now())
            this.cleanupStmt.run(jid, jid, this.max)
         } catch (error) {
            console.error('[MessageStore] Gagal menyimpan pesan ke SQLite:', error)
         }
      }
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store