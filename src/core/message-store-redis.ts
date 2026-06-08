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

   private _messages: Record<string, WAMessage[]>
   private loadedJids: Set<string>
   private loadingJids: Set<string>
   private maxCachedJids: number
   private redis: any = null
   private dirtyJids: Set<string>
   private isSaving: boolean

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      
      this._messages = Object.create(null) as Record<string, WAMessage[]>
      this.loadedJids = new Set<string>()
      this.loadingJids = new Set<string>()
      this.maxCachedJids = 50
      this.dirtyJids = new Set<string>()
      this.isSaving = false

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
      if (!this._messages[jid]) {
         this._messages[jid] = []
         this.asyncLoadJid(jid)
      }
   }

   private async asyncLoadJid(jid: string): Promise<void> {
      if (!this.redis) return
      this.loadingJids.add(jid)
      try {
         const raw = await this.redis.get(`msg_store:${jid}`)
         if (raw) {
            const history = JSON.parse(raw) as WAMessage[]
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
         }
      } catch (error) {
         console.error(`[message-store-redis] Failed to load JID ${jid} from Redis:`, error)
      } finally {
         this.loadingJids.delete(jid)
      }
   }

   private touchJid(jid: string): void {
      this.loadedJids.delete(jid)
      this.loadedJids.add(jid)

      if (this.loadedJids.size > this.maxCachedJids) {
         for (const oldJid of this.loadedJids) {
            if (this.dirtyJids.has(oldJid) || this.loadingJids.has(oldJid)) continue

            delete this._messages[oldJid]
            this.loadedJids.delete(oldJid)
            break
         }
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
            if (this.loadingJids.has(jid)) {
               this.dirtyJids.add(jid)
               return
            }
            const data = this._messages[jid]
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

      if (this._messages[jid].length > this.max) {
         this._messages[jid].splice(0, this._messages[jid].length - this.max)
      }

      this.dirtyJids.add(jid)
      this.touchJid(jid)
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store