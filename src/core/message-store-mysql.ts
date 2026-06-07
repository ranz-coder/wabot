import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'

let MySQLConstructor: any = null
const loadMySQL = async () => {
   if (MySQLConstructor) return MySQLConstructor
   try {
      const moduleName = String('mysql2/promise')
      const module = await import(moduleName)
      MySQLConstructor = module.default || module
      return MySQLConstructor
   } catch (e) {
      return null
   }
}

class MessageStore {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public uri: string | undefined
   public messages: Record<string, WAMessage[]>

   private pool: any = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.messages = Object.create(null) as Record<string, WAMessage[]>

      this.initDB()
   }

   private async initDB(): Promise<void> {
      const mysql = await loadMySQL()

      if (!mysql) {
         console.warn('[message-store-mysql] mysql2 module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[message-store-mysql] MySQL URI not provided! Running in RAM-only mode.')
         return
      }

      if (this.pool) {
         try {
            await this.pool.end()
         } catch (e) { }
      }

      try {
         this.pool = mysql.createPool(this.uri)

         await this.pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
               jid VARCHAR(255) NOT NULL,
               id VARCHAR(255) NOT NULL,
               data LONGTEXT NOT NULL,
               created_at BIGINT NOT NULL,
               PRIMARY KEY (jid, id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
         `)

         const [rows]: any = await this.pool.query('SELECT jid, data FROM messages ORDER BY created_at ASC')

         const loadedMessages = Object.create(null) as Record<string, WAMessage[]>
         for (const row of rows) {
            if (!loadedMessages[row.jid]) {
               loadedMessages[row.jid] = []
            }
            try {
               loadedMessages[row.jid].push(JSON.parse(row.data))
            } catch { }
         }

         this.messages = loadedMessages
         if (this.client) {
            this.client.messages = this.messages
         }
      } catch (error) {
         console.error('[message-store-mysql] Failed to initialize MySQL:', error)
         this.pool = null
      }
   }

   public config({ dir, max, uri }: StoreConfig): this {
      let needsReinit = false

      if (dir) {
         this.storeDir = path.join(process.cwd(), '.cache', dir)
      }

      if (max !== undefined) {
         this.max = max
      }

      if (uri && uri !== this.uri) {
         this.uri = uri
         needsReinit = true
      }

      if (needsReinit) {
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
      if (!this.messages[jid]) {
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

      if (msgId && this.pool) {
         this.pool.execute(
            'INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)',
            [jid, msgId, JSON.stringify(msg), Date.now()]
         ).then(() => {
            return this.pool.execute(
               'DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?) as tmp)',
               [jid, jid, this.max]
            )
         }).catch((error: any) => {
            console.error('[message-store-mysql] Failed to save message to MySQL:', error)
         })
      }
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store