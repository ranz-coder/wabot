import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'

let PGConstructor: any = null
const loadPG = async () => {
   if (PGConstructor) return PGConstructor
   try {
      const moduleName = String('pg')
      const module = await import(moduleName)
      PGConstructor = module.default?.Pool || module.Pool || module
      return PGConstructor
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
   }

   private async initDB(): Promise<void> {
      const Pool = await loadPG()

      if (!Pool) {
         console.warn('[message-store-pg] pg module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[message-store-pg] PostgreSQL URI not provided! Running in RAM-only mode.')
         return
      }

      if (this.pool) {
         try {
            await this.pool.end()
         } catch (e) { }
      }

      try {
         this.pool = new Pool({ connectionString: this.uri })

         await this.pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
               jid VARCHAR(255) NOT NULL,
               id VARCHAR(255) NOT NULL,
               data TEXT NOT NULL,
               created_at BIGINT NOT NULL,
               PRIMARY KEY (jid, id)
            )
         `)

         const { rows }: any = await this.pool.query('SELECT jid, data FROM messages ORDER BY created_at ASC')

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
         console.error('[message-store-pg] Failed to initialize PostgreSQL:', error)
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

      this.initDB()

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
         this.pool.query(
            'INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at',
            [jid, msgId, JSON.stringify(msg), Date.now()]
         ).then(() => {
            return this.pool.query(
               'DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at DESC LIMIT $3)',
               [jid, jid, this.max]
            )
         }).catch((error: any) => {
            console.error('[message-store-pg] Failed to save message to PG:', error)
         })
      }
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store