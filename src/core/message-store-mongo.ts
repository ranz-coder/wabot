import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'

let MongoConstructor: any = null
const loadMongo = async () => {
   if (MongoConstructor) return MongoConstructor
   try {
      const moduleName = String('mongodb')
      const module = await import(moduleName)
      MongoConstructor = module.MongoClient || module.default?.MongoClient || module
      return MongoConstructor
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
   private mongoClient: any = null
   private db: any = null
   private messagesCollection: any = null

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
      const MongoClient = await loadMongo()

      if (!MongoClient) {
         console.warn('[message-store-mongodb] mongodb module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[message-store-mongodb] MongoDB URI not provided! Running in RAM-only mode.')
         return
      }

      if (this.mongoClient) {
         try {
            await this.mongoClient.close()
         } catch (e) { }
      }

      try {
         this.mongoClient = new MongoClient(this.uri)
         await this.mongoClient.connect()

         this.db = this.mongoClient.db()
         this.messagesCollection = this.db.collection('messages')

         await this.messagesCollection.createIndex({ jid: 1, id: 1 }, { unique: true })
      } catch (error) {
         console.error('[message-store-mongodb] Failed to initialize MongoDB:', error)
         this.mongoClient = null
         this.db = null
         this.messagesCollection = null
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
      if (!this.messagesCollection) return
      this.loadingJids.add(jid)
      try {
         const docs = await this.messagesCollection.find({ jid }).sort({ created_at: 1 }).toArray()
         const history = docs.map((doc: any) => doc.data) as WAMessage[]
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
         console.error(`[message-store-mongodb] Failed to load JID ${jid} from MongoDB:`, error)
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

      if (msgId && this.messagesCollection) {
         this.messagesCollection.updateOne(
            { jid, id: msgId },
            { $set: { data: msg, created_at: Date.now() } },
            { upsert: true }
         ).then(async () => {
            const docsToKeep = await this.messagesCollection
               .find({ jid })
               .sort({ created_at: -1 })
               .limit(this.max)
               .project({ id: 1 })
               .toArray()

            const keepIds = docsToKeep.map((d: any) => d.id)
            await this.messagesCollection.deleteMany({
               jid,
               id: { $nin: keepIds }
            })
         }).catch((error: any) => {
            console.error('[message-store-mongodb] Failed to save message to MongoDB:', error)
         })
      }
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store