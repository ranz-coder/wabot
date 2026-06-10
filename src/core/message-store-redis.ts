import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'

let RedisConstructor: any = null
const loadRedis = async () => {
   if (RedisConstructor) return RedisConstructor
   try {
      const moduleName = String('redis')
      const module = await import(moduleName)
      RedisConstructor = module.createClient ? module : (module.default || module)
      return RedisConstructor
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

   private redis: any = null
   private fallbackStore: Record<string, WAMessage[]> | null = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'redis'

      if (process.env?.USE_STORE?.includes('redis')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
      }
   }

   private async initDB(): Promise<void> {
      const RedisModule = await loadRedis()

      if (!RedisModule || (!RedisModule.createClient && !RedisModule.default?.createClient)) {
         console.warn('[message-store-redis] Redis module not installed! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         return
      }

      if (!this.uri) {
         console.warn('[message-store-redis] Redis URI not provided! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         return
      }

      if (this.redis) {
         try {
            await this.redis.disconnect()
         } catch (e) { }
      }

      try {
         const createClient = RedisModule.createClient || RedisModule.default?.createClient
         this.redis = createClient({ url: this.uri })

         this.redis.on('error', (err: any) => {
            console.error('[message-store-redis] Redis Client Error:', err)
         })

         await this.redis.connect()
      } catch (error) {
         console.error('[message-store-redis] Failed to initialize Redis. Falling back to RAM-only mode:', error)
         this.redis = null
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

   private async getRedisData(jid: string): Promise<WAMessage[]> {
      if (!this.redis) return []
      try {
         const raw = await this.redis.get(`msg_store:${jid}`)
         return raw ? (JSON.parse(raw) as WAMessage[]) : []
      } catch (error) {
         console.error(`[message-store-redis] Failed to load JID ${jid} from Redis:`, error)
         return []
      }
   }

   private async setRedisData(jid: string, data: WAMessage[]): Promise<void> {
      if (!this.redis) return
      try {
         await this.redis.set(`msg_store:${jid}`, JSON.stringify(data))
      } catch (error) {
         console.error(`[message-store-redis] Failed to save JID ${jid} to Redis:`, error)
      }
   }

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      if (this.redis) {
         const list = await this.getRedisData(jid)
         return list.find(v => v.key?.id === id || (v as any).id === id) || null
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         return list.find(v => v.key?.id === id || (v as any).id === id) || null
      }

      return null
   }

   public async loadMessages(jid: string, count?: number): Promise<WAMessage[] | null> {
      if (this.redis) {
         const list = await this.getRedisData(jid)
         if (list.length === 0) return null

         const slice = count ? list.slice(-count) : list
         return [...slice].reverse()
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
      if (this.redis) {
         const list = await this.getRedisData(jid)
         list.push(msg)

         if (list.length > this.max) {
            list.splice(0, list.length - this.max)
         }

         await this.setRedisData(jid, list)
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

         if (self.redis) {
            list = await self.getRedisData(jid)
         } else if (self.fallbackStore) {
            list = self.fallbackStore[jid] || []
         }

         const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.redis) {
               const currentList = await self.getRedisData(jid)
               return Math.max(0, currentList.length - offset)
            }
            if (self.fallbackStore) {
               const currentList = self.fallbackStore[jid] || []
               return Math.max(0, currentList.length - offset)
            }
            return 0
         }

         sliced.clear = async () => {
            if (self.redis) {
               if (offset === 0) {
                  try {
                     await self.redis.del(`msg_store:${jid}`)
                  } catch (error) {
                     console.error(`[message-store-redis] Failed to clear JID ${jid} from Redis:`, error)
                  }
               } else {
                  const currentList = await self.getRedisData(jid)
                  if (offset < currentList.length) {
                     const updated = currentList.slice(0, offset)
                     await self.setRedisData(jid, updated)
                  }
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
         if (self.redis) {
            const currentList = await self.getRedisData(jid)
            return Math.max(0, currentList.length - offset)
         }
         if (self.fallbackStore) {
            const currentList = self.fallbackStore[jid] || []
            return Math.max(0, currentList.length - offset)
         }
         return 0
      }

      promiseWithMethods.clear = async () => {
         if (self.redis) {
            if (offset === 0) {
               try {
                  await self.redis.del(`msg_store:${jid}`)
               } catch (error) {
                  console.error(`[message-store-redis] Failed to clear JID ${jid} from Redis:`, error)
               }
            } else {
               const currentList = await self.getRedisData(jid)
               if (offset < currentList.length) {
                  const updated = currentList.slice(0, offset)
                  await self.setRedisData(jid, updated)
               }
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