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
   public database: string

   private pool: any = null
   private fallbackStore: Record<string, WAMessage[]> | null = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'pg'

      if (process.env?.USE_STORE?.includes('pg')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
      }
   }

   private async initDB(): Promise<void> {
      const Pool = await loadPG()

      if (!Pool) {
         console.warn('[message-store-pg] pg module not installed! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         return
      }

      if (!this.uri) {
         console.warn('[message-store-pg] PostgreSQL URI not provided! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
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
         console.error('[message-store-pg] Failed to initialize PostgreSQL. Falling back to RAM-only mode:', error)
         this.pool = null
         this.fallbackStore = Object.create(null)
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
      client.getAllMessages = this.getAllMessages.bind(this)

      return client
   }

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      if (this.pool) {
         try {
            const { rows }: any = await this.pool.query(
               'SELECT data FROM messages WHERE jid = $1 AND id = $2',
               [jid, id]
            )
            return rows.length > 0 ? (JSON.parse(rows[0].data) as WAMessage) : null
         } catch (error) {
            console.error(`[message-store-pg] Failed to load message ${id} for JID ${jid}:`, error)
            return null
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         return list.find(v => v.key?.id === id || (v as any).id === id) || null
      }

      return null
   }

   public async loadMessages(jid: string, count?: number): Promise<WAMessage[] | null> {
      if (this.pool) {
         try {
            let query = 'SELECT data FROM messages WHERE jid = $1 ORDER BY created_at DESC'
            const params: any[] = [jid]

            if (count !== undefined && count > 0) {
               query += ' LIMIT $2'
               params.push(count)
            }

            const { rows }: any = await this.pool.query(query, params)
            if (rows.length === 0) return null

            return rows.map((row: any) => JSON.parse(row.data) as WAMessage)
         } catch (error) {
            console.error(`[message-store-pg] Failed to load messages for JID ${jid}:`, error)
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

   public async addMessage(jid: string, msg: WAMessage): Promise<void> {
      const msgId = msg.key?.id || (msg as any).id

      if (this.pool && msgId) {
         try {
            await this.pool.query(
               'INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at',
               [jid, msgId, JSON.stringify(msg), Date.now()]
            )
            await this.pool.query(
               'DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at DESC LIMIT $3)',
               [jid, jid, this.max]
            )
         } catch (error) {
            console.error('[message-store-pg] Failed to save message to PG:', error)
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

   public getAllMessages(jid: string, offset: number = 0): Promise<WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }> & { count(): Promise<number>; clear(): Promise<void> } {
      const self = this

      const promise = (async () => {
         let list: WAMessage[] = []

         if (self.pool) {
            try {
               const { rows }: any = await self.pool.query(
                  'SELECT data FROM messages WHERE jid = $1 ORDER BY created_at ASC OFFSET $2',
                  [jid, offset]
               )
               list = rows.map((row: any) => JSON.parse(row.data) as WAMessage)
            } catch (error) {
               console.error(`[message-store-pg] Failed to get messages for JID ${jid}:`, error)
            }
         } else if (self.fallbackStore) {
            const rawList = self.fallbackStore[jid] || []
            list = offset > 0 ? rawList.slice(offset) : rawList
         }

         const sliced = list as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.pool) {
               try {
                  const { rows }: any = await self.pool.query(
                     'SELECT COUNT(*) as count FROM messages WHERE jid = $1',
                     [jid]
                  )
                  const total = parseInt(rows[0]?.count || '0', 10)
                  return Math.max(0, total - offset)
               } catch (error) {
                  console.error(`[message-store-pg] Failed to count messages for JID ${jid}:`, error)
                  return 0
               }
            }
            if (self.fallbackStore) {
               const total = (self.fallbackStore[jid] || []).length
               return Math.max(0, total - offset)
            }
            return 0
         }

         sliced.clear = async () => {
            if (self.pool) {
               try {
                  if (offset === 0) {
                     await self.pool.query('DELETE FROM messages WHERE jid = $1', [jid])
                  } else {
                     await self.pool.query(
                        'DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at ASC LIMIT $3)',
                        [jid, jid, offset]
                     )
                  }
               } catch (error) {
                  console.error(`[message-store-pg] Failed to clear messages for JID ${jid}:`, error)
               }
               return
            }

            if (self.fallbackStore) {
               if (offset === 0) {
                  delete self.fallbackStore[jid]
               } else {
                  const currentList = self.fallbackStore[jid] || []
                  if (offset < currentList.length) {
                     self.fallbackStore[jid] = currentList.slice(0, offset)
                  }
               }
            }
         }

         return sliced
      })()

      const promiseWithMethods = promise as any

      promiseWithMethods.count = async () => {
         if (self.pool) {
            try {
               const { rows }: any = await self.pool.query(
                  'SELECT COUNT(*) as count FROM messages WHERE jid = $1',
                  [jid]
               )
               const total = parseInt(rows[0]?.count || '0', 10)
               return Math.max(0, total - offset)
            } catch (error) {
               console.error(`[message-store-pg] Failed to count messages for JID ${jid}:`, error)
               return 0
            }
         }
         if (self.fallbackStore) {
            const total = (self.fallbackStore[jid] || []).length
            return Math.max(0, total - offset)
         }
         return 0
      }

      promiseWithMethods.clear = async () => {
         if (self.pool) {
            try {
               if (offset === 0) {
                  await self.pool.query('DELETE FROM messages WHERE jid = $1', [jid])
               } else {
                  await self.pool.query(
                     'DELETE FROM messages WHERE jid = $1 AND id NOT IN (SELECT id FROM messages WHERE jid = $2 ORDER BY created_at ASC LIMIT $3)',
                     [jid, jid, offset]
                  )
               }
            } catch (error) {
               console.error(`[message-store-pg] Failed to clear messages for JID ${jid}:`, error)
            }
            return
         }

         if (self.fallbackStore) {
            if (offset === 0) {
               delete self.fallbackStore[jid]
            } else {
               const currentList = self.fallbackStore[jid] || []
               if (offset < currentList.length) {
                  self.fallbackStore[jid] = currentList.slice(0, offset)
               }
            }
         }
      }

      return promiseWithMethods
   }
}

const store = new MessageStore('messages')

export default store