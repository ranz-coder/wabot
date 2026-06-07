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

   private mongoClient: any = null
   private db: any = null
   private messagesCollection: any = null

   constructor(dir: string = 'messages', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.messages = Object.create(null) as Record<string, WAMessage[]>
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

         const docs = await this.messagesCollection.find().sort({ created_at: 1 }).toArray()

         const loadedMessages = Object.create(null) as Record<string, WAMessage[]>
         for (const doc of docs) {
            if (!loadedMessages[doc.jid]) {
               loadedMessages[doc.jid] = []
            }
            loadedMessages[doc.jid].push(doc.data)
         }

         this.messages = loadedMessages
         if (this.client) {
            this.client.messages = this.messages
         }
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