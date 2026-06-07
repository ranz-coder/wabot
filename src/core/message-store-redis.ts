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
   public messages: Record<string, WAMessage[]>

   private redis: any = null
   private dirtyJids: Set<string>
   private isSaving: boolean

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.messages = Object.create(null) as Record<string, WAMessage[]>
      this.dirtyJids = new Set<string>()
      this.isSaving = false

      setInterval(() => this.checkAndSave(), 15000)
   }

   private async initDB(): Promise<void> {
      const RedisModule = await loadRedis()

      if (!RedisModule || !RedisModule.createClient) {
         console.warn('[message-store-redis] Redis module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[message-store-redis] Redis URI not provided! Running in RAM-only mode.')
         return
      }

      if (this.redis) {
         try {
            await this.redis.disconnect()
         } catch (e) { }
      }

      try {
         this.redis = RedisModule.createClient({ url: this.uri })

         this.redis.on('error', (err: any) => {
            console.error('[message-store-redis] Redis Client Error:', err)
         })

         await this.redis.connect()

         const keys = await this.redis.keys('msg_store:*')
         if (keys.length > 0) {
            const values = await this.redis.mGet(keys)
            keys.forEach((key: string, index: number) => {
               const jid = key.replace('msg_store:', '')
               if (values[index]) {
                  try {
                     this.messages[jid] = JSON.parse(values[index])
                  } catch {
                     this.messages[jid] = []
                  }
               }
            })
         }
      } catch (error) {
         console.error('[message-store-redis] Failed to initialize Redis:', error)
         this.redis = null
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

   private async checkAndSave(): Promise<void> {
      if (this.isSaving || this.dirtyJids.size === 0 || !this.redis) return

      this.isSaving = true
      const jidsToSave = Array.from(this.dirtyJids)
      this.dirtyJids.clear()

      try {
         const multi = this.redis.multi()

         jidsToSave.forEach((jid) => {
            const data = this.messages[jid]
            if (data) {
               multi.set(`msg_store:${jid}`, JSON.stringify(data))
            }
         })

         await multi.exec()
      } catch (error) {
         console.error('[message-store-redis] Failed to save messages to Redis:', error)
      } finally {
         this.isSaving = false
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

      if (this.messages[jid].length > this.max) {
         this.messages[jid].splice(0, this.messages[jid].length - this.max)
      }

      this.dirtyJids.add(jid)
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store