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

   private _messages: Record<string, WAMessage[]>
   private loadedJids: Set<string>
   private loadingJids: Set<string>
   private maxCachedJids: number
   private pool: any = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE

      this._messages = Object.create(null) as Record<string, WAMessage[]>
      this.loadedJids = new Set<string>()
      this.loadingJids = new Set<string>()
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
      if (!this._messages[jid]) {
         this._messages[jid] = []
         this.asyncLoadJid(jid)
      }
   }

   private async asyncLoadJid(jid: string): Promise<void> {
      if (!this.pool) return
      this.loadingJids.add(jid)
      try {
         const { rows }: any = await this.pool.query(
            'SELECT data FROM messages WHERE jid = $1 ORDER BY created_at ASC',
            [jid]
         )
         const history: WAMessage[] = []
         for (const row of rows) {
            try {
               history.push(JSON.parse(row.data))
            } catch { }
         }
         const current = this._messages[jid] || []
         const merged = [...history]
         for (const msg of current) {
            const id = msg.key?.id || (msg as any).id
            const exists = merged.some(v => (v.key?.id === id || (v as any).id === id))
            if (!exists) {
               merged.push(msg)
            }
         }
         if (merged.length > this.max) {
            merged.splice(0, merged.length - this.max)
         }
         this._messages[jid] = merged
      } catch (error) {
         console.error(`[message-store-pg] Failed to load JID ${jid} from PostgreSQL:`, error)
      } finally {
         this.loadingJids.delete(jid)
      }
   }

   private touchJid(jid: string): void {
      this.loadedJids.delete(jid)
      this.loadedJids.add(jid)

      if (this.loadedJids.size > this.maxCachedJids) {
         for (const oldJid of this.loadedJids) {
            if (this.loadingJids.has(oldJid)) continue

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