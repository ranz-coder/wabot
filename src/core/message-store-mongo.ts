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
   public database: string

   private mongoClient: any = null
   private db: any = null
   private messagesCollection: any = null
   private fallbackStore: Record<string, WAMessage[]> | null = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'mongodb'

      if (process.env?.USE_STORE?.includes('mongodb')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
      }
   }

   private async initDB(): Promise<void> {
      const MongoClient = await loadMongo()

      if (!MongoClient) {
         console.warn('[message-store-mongodb] mongodb module not installed! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         return
      }

      if (!this.uri) {
         console.warn('[message-store-mongodb] MongoDB URI not provided! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
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
         console.error('[message-store-mongodb] Failed to initialize MongoDB. Falling back to RAM-only mode:', error)
         this.mongoClient = null
         this.db = null
         this.messagesCollection = null
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
      if (this.messagesCollection) {
         try {
            const doc = await this.messagesCollection.findOne({ jid, id })
            return doc ? (doc.data as WAMessage) : null
         } catch (error) {
            console.error(`[message-store-mongodb] Failed to load message ${id} for JID ${jid}:`, error)
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
      if (this.messagesCollection) {
         try {
            let cursor = this.messagesCollection.find({ jid }).sort({ created_at: -1 })
            if (count !== undefined && count > 0) {
               cursor = cursor.limit(count)
            }
            const docs = await cursor.toArray()
            if (docs.length === 0) return null
            return docs.map((doc: any) => doc.data as WAMessage)
         } catch (error) {
            console.error(`[message-store-mongodb] Failed to load messages for JID ${jid}:`, error)
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

      if (this.messagesCollection && msgId) {
         try {
            await this.messagesCollection.updateOne(
               { jid, id: msgId },
               { $set: { data: msg, created_at: Date.now() } },
               { upsert: true }
            )

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
         } catch (error) {
            console.error('[message-store-mongodb] Failed to save message to MongoDB:', error)
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

         if (self.messagesCollection) {
            try {
               const docs = await self.messagesCollection
                  .find({ jid })
                  .sort({ created_at: 1 })
                  .skip(offset)
                  .toArray()
               list = docs.map((doc: any) => doc.data as WAMessage)
            } catch (error) {
               console.error(`[message-store-mongodb] Failed to get messages for JID ${jid}:`, error)
            }
         } else if (self.fallbackStore) {
            const rawList = self.fallbackStore[jid] || []
            list = offset > 0 ? rawList.slice(offset) : rawList
         }

         const sliced = list as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.messagesCollection) {
               try {
                  const total = await self.messagesCollection.countDocuments({ jid })
                  return Math.max(0, total - offset)
               } catch (error) {
                  console.error(`[message-store-mongodb] Failed to count messages for JID ${jid}:`, error)
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
            if (self.messagesCollection) {
               try {
                  if (offset === 0) {
                     await self.messagesCollection.deleteMany({ jid })
                  } else {
                     const docsToKeep = await self.messagesCollection
                        .find({ jid })
                        .sort({ created_at: 1 })
                        .limit(offset)
                        .project({ id: 1 })
                        .toArray()

                     const keepIds = docsToKeep.map((d: any) => d.id)
                     await self.messagesCollection.deleteMany({ jid, id: { $nin: keepIds } })
                  }
               } catch (error) {
                  console.error(`[message-store-mongodb] Failed to clear messages for JID ${jid}:`, error)
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
         if (self.messagesCollection) {
            try {
               const total = await self.messagesCollection.countDocuments({ jid })
               return Math.max(0, total - offset)
            } catch (error) {
               console.error(`[message-store-mongodb] Failed to count messages for JID ${jid}:`, error)
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
         if (self.messagesCollection) {
            try {
               if (offset === 0) {
                  await self.messagesCollection.deleteMany({ jid })
               } else {
                  const docsToKeep = await self.messagesCollection
                     .find({ jid })
                     .sort({ created_at: 1 })
                     .limit(offset)
                     .project({ id: 1 })
                     .toArray()

                  const keepIds = docsToKeep.map((d: any) => d.id)
                  await self.messagesCollection.deleteMany({ jid, id: { $nin: keepIds } })
               }
            } catch (error) {
               console.error(`[message-store-mongodb] Failed to clear messages for JID ${jid}:`, error)
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