import { type Contact, type ConnectionState, type PresenceData, BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'
import { noSuffix, getKeyAuthor } from '../utils.js'

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

class Store {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public uri: string | undefined
   public database: string

   private mongoClient: any = null
   private db: any = null
   private messagesCollection: any = null
   private chatsCollection: any = null

   private fallbackStore: Record<string, WAMessage[]> | null = null
   private fallbackChats: Record<string, any> | null = null

   public contacts: Record<string, Contact> = Object.create(null)
   public stories: Record<string, any[]> = Object.create(null)
   public presences: Record<string, { [participant: string]: PresenceData }> = Object.create(null)
   public state: ConnectionState = { connection: 'close' }
   public messageId: Map<string, Map<string, { at: number }>> = new Map()

   private cache = new Map<string, WAMessage[]>()
   private maxCachedJids = 10
   private writeQueues = new Map<string, Promise<any>>()

   private chatsCache = new Map<string, any>()
   private chatsProxyInstance: Record<string, any>

   constructor(dir: string = 'stores', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'mongodb'
      this.chatsProxyInstance = this.createChatsProxy()

      if (this.uri?.includes('mongodb')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
         this.fallbackChats = Object.create(null)
      }

      setInterval(() => this.cleanupExpiredMessages(), 120000)
   }

   private toPOJO(obj: any, seen = new WeakSet()): any {
      if (obj === null || typeof obj !== 'object') return obj
      if (seen.has(obj)) return null
      if (Buffer.isBuffer(obj) || obj instanceof Uint8Array) return obj
   
      seen.add(obj)
   
      if (Array.isArray(obj)) {
         return obj.map(v => this.toPOJO(v, seen))
      }
   
      const res: any = {}
      for (const key of Object.keys(obj)) {
         const val = obj[key]
         if (typeof val !== 'function') {
            res[key] = this.toPOJO(val, seen)
         }
      }
      return res
   }

   private async initDB(): Promise<void> {
      const MongoClient = await loadMongo()
      if (!MongoClient || !this.uri) return
      try {
         this.mongoClient = new MongoClient(this.uri, { maxPoolSize: 10, minPoolSize: 1 })
         await this.mongoClient.connect()
         this.db = this.mongoClient.db()
         this.messagesCollection = this.db.collection('messages')
         this.chatsCollection = this.db.collection('chats')
         await this.messagesCollection.createIndex({ jid: 1, id: 1 }, { unique: true })
         await this.messagesCollection.createIndex({ jid: 1, created_at: -1 })
         await this.chatsCollection.createIndex({ id: 1 }, { unique: true })
         await this.preloadChats()
         this.fallbackStore = null
         this.fallbackChats = null
      } catch (error) {
         if (!this.fallbackStore) {
            this.fallbackStore = Object.create(null)
            this.fallbackChats = Object.create(null)
         }
      }
   }

   private async preloadChats(): Promise<void> {
      if (!this.chatsCollection) return
      try {
         const docs = await this.chatsCollection.find({}).project({ id: 1, data: 1 }).toArray()
         for (const doc of docs) {
            this.chatsCache.set(doc.id, doc.data)
         }
      } catch (e) { }
   }

   private createChatsProxy(): Record<string, any> {
      const self = this
      return new Proxy(Object.create(null), {
         get: (target, prop) => {
            if (typeof prop !== 'string' || prop === 'toJSON') return undefined
            return self.chatsCache.get(prop) || self.fallbackChats?.[prop]
         },
         set: (target, prop, value) => {
            if (typeof prop !== 'string') return false

            const cleanedValue = self.toPOJO(value)
            self.chatsCache.set(prop, cleanedValue)

            if (self.chatsCollection) {
               self.chatsCollection.updateOne(
                  { id: prop },
                  { $set: { data: cleanedValue, updated_at: Date.now() } },
                  { upsert: true }
               ).catch(() => { })
            } else if (self.fallbackChats) {
               self.fallbackChats[prop] = cleanedValue
            }
            return true
         },
         ownKeys: () => self.chatsCollection ? Array.from(self.chatsCache.keys()) : Object.keys(self.fallbackChats || {}),
         getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
      }) as Record<string, any>
   }

   public get chats(): Record<string, any> {
      return this.chatsProxyInstance
   }

   public bind<T extends BotClient>(client: T): T {
      this.client = client

      client.loadMessage = this.loadMessage.bind(this)
      client.loadMessages = this.loadMessages.bind(this)
      client.addMessage = this.addMessage.bind(this)
      client.getAllMessages = this.getAllMessages.bind(this)

      client.chatUpdate = this.chatUpdate.bind(this)
      client.contactsUpsert = this.contactsUpsert.bind(this)
      client.contactUpdate = this.contactUpdate.bind(this)
      client.getContact = this.getContact.bind(this)
      client.getAllContacts = this.getAllContacts.bind(this)
      client.updateMessageWithReceipt = this.updateMessageWithReceipt.bind(this)
      client.updateMessageWithReaction = this.updateMessageWithReaction.bind(this)
      client.loadStories = this.loadStories.bind(this)
      client.loadStory = this.loadStory.bind(this)
      client.addStory = this.addStory.bind(this)
      client.getAllStories = this.getAllStories.bind(this)
      client.recordMessageId = this.recordMessageId.bind(this)

      client.contacts = this.contacts
      client.stories = this.stories
      client.presences = this.presences
      client.state = this.state
      client.messageId = this.messageId
      client.chats = this.chats

      return client
   }

   private async getMongoData(jid: string): Promise<WAMessage[]> {
      if (this.cache.has(jid)) return this.cache.get(jid)!
      if (!this.messagesCollection) return []
      try {
         const docs = await this.messagesCollection.find({ jid }).sort({ created_at: 1 }).limit(this.max).toArray()
         const data = docs.map((doc: any) => doc.data)
         this.cache.set(jid, data)
         if (this.cache.size > this.maxCachedJids) this.cache.delete(this.cache.keys().next().value)
         return data
      } catch { return [] }
   }

   public async addMessage(jid: string, msg: WAMessage): Promise<void> {
      const msgId = msg.key?.id || (msg as any).id
      if (!msgId) return
      const list = await this.getMongoData(jid)
      list.push(msg)
      if (list.length > this.max) list.shift()

      if (this.messagesCollection) {
         const cleanedMsg = this.toPOJO(msg)
         const previous = this.writeQueues.get(jid) || Promise.resolve()
         const current = previous.then(async () => {
            try {
               await this.messagesCollection.updateOne({ jid, id: msgId }, { $set: { data: cleanedMsg, created_at: Date.now() } }, { upsert: true })
               if (list.length >= this.max) {
                  const count = await this.messagesCollection.countDocuments({ jid })
                  if (count > this.max + 10) {
                     const toDelete = await this.messagesCollection.find({ jid }).sort({ created_at: 1 }).limit(count - this.max).project({ _id: 1 }).toArray()
                     await this.messagesCollection.deleteMany({ _id: { $in: toDelete.map((d: any) => d._id) } })
                  }
               }
            } catch (e) { }
         }).finally(() => { if (this.writeQueues.get(jid) === current) this.writeQueues.delete(jid) })
         this.writeQueues.set(jid, current)
      } else if (this.fallbackStore) {
         if (!this.fallbackStore[jid]) this.fallbackStore[jid] = []
         this.fallbackStore[jid].push(msg)
         if (this.fallbackStore[jid].length > this.max) this.fallbackStore[jid].shift()
      }
   }

   public async updateMessageWithReceipt(msg: any, receipt: any): Promise<void> {
      if (!msg) return
      msg.userReceipt = msg.userReceipt || []
      const recp = msg.userReceipt.find((m: any) => m.userJid === receipt.userJid)
      if (recp) Object.assign(recp, receipt)
      else msg.userReceipt.push(receipt)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id

      if (jid && id && this.messagesCollection) {
         const cleanedMsg = this.toPOJO(msg)
         this.messagesCollection.updateOne({ jid, id }, { $set: { data: cleanedMsg } }).catch(() => { })
      }
   }

   public async updateMessageWithReaction(msg: any, reaction: any): Promise<void> {
      if (!msg) return
      const authorID = getKeyAuthor(reaction.key)
      msg.reactions = (msg.reactions || []).filter((r: any) => getKeyAuthor(r.key) !== authorID)
      if (reaction.text) msg.reactions.push(reaction)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id

      if (jid && id && this.messagesCollection) {
         const cleanedMsg = this.toPOJO(msg)
         this.messagesCollection.updateOne({ jid, id }, { $set: { data: cleanedMsg } }).catch(() => { })
      }
   }

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      const list = await this.getMongoData(jid)
      return list.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public async loadMessages(jid: string, count?: number): Promise<WAMessage[] | null> {
      const list = await this.getMongoData(jid)
      if (list.length === 0) return null
      return [...list].reverse().slice(0, count)
   }

   public chatUpdate(updates: any[]): void {
      for (const update of updates) {
         if (update.id) this.chats[update.id] = Object.assign(this.chats[update.id] || { id: update.id }, update)
      }
   }

   public contactsUpsert(newContacts: Contact[]): Set<string> {
      const oldContacts = new Set(Object.keys(this.contacts))
      for (const contact of newContacts) {
         const id = noSuffix(contact.id)
         let jid = id
         if (this.client && jid?.endsWith('lid')) {
            // @ts-ignore
            jid = this.client?.getJidFromJSON(jid)?.jid ?? id
         }
         oldContacts.delete(jid)
         this.contacts[jid] = Object.assign(this.contacts[jid] || { jid }, contact)
      }
      return oldContacts
   }

   public contactUpdate(updates: any[]): void {
      for (const update of updates) {
         if (update.id) {
            const id = noSuffix(update.id)
            let jid = id
            if (this.client && jid?.endsWith('lid')) {
               // @ts-ignore
               jid = this.client?.getJidFromJSON(jid)?.jid ?? id
            }
            this.contacts[jid] = Object.assign(this.contacts[jid] || { jid, id: jid }, update)
         }
      }
   }

   public getContact(id: string): Contact | null {
      return this.contacts[id] || Object.values(this.contacts).find((c: any) => c.id === id || c.jid === id) || null
   }

   public getAllContacts(offset: number = 0) {
      const list = Object.values(this.contacts).slice(offset)
      return Object.assign(list, {
         count: () => Object.keys(this.contacts).length - offset,
         clear: () => { if (offset === 0) this.contacts = Object.create(null) }
      })
   }

   public recordMessageId(sock: any, msg: any): boolean {
      const id = msg.key?.id
      if (!id || msg.fromMe) return true
      const instance = noSuffix(sock.user.id)
      let instanceMap = this.messageId.get(instance)
      if (!instanceMap) {
         instanceMap = new Map()
         this.messageId.set(instance, instanceMap)
      }
      if (instanceMap.has(id) && !msg.updated) return false
      instanceMap.set(id, { at: Date.now() })
      if (instanceMap.size > 2000) instanceMap.delete(instanceMap.keys().next().value)
      return true
   }

   private cleanupExpiredMessages(): void {
      const now = Date.now()
      this.messageId.forEach((map, key) => {
         map.forEach((val, msgId) => { if (now - val.at > 600000) map.delete(msgId) })
         if (map.size === 0) this.messageId.delete(key)
      })
      Object.keys(this.stories).forEach(k => { if (this.stories[k].length > 20) this.stories[k] = this.stories[k].slice(-20) })
   }

   public async getAllMessages(jid: string, offset: number = 0) {
      const list = await this.getMongoData(jid)
      const sliced = list.slice(offset)
      return Object.assign(sliced, {
         count: async () => (await this.getMongoData(jid)).length - offset,
         clear: async () => {
            this.cache.delete(jid)
            if (this.messagesCollection) await this.messagesCollection.deleteMany({ jid })
         }
      })
   }

   public loadStories(jid: string, count?: number) {
      const list = this.stories[jid] || []
      return [...list].reverse().slice(0, count)
   }

   public loadStory(jid: string, id: string) {
      return (this.stories[jid] || []).find((v: any) => v.key?.id === id) || null
   }

   public addStory(jid: string, story: any) {
      if (!this.stories[jid]) this.stories[jid] = []
      this.stories[jid].push(story)
      if (this.stories[jid].length > 50) this.stories[jid].shift()
   }

   public getAllStories(jid: string, offset: number = 0) {
      const list = (this.stories[jid] || []).slice(offset)
      return Object.assign(list, {
         count: () => (this.stories[jid] || []).length - offset,
         clear: () => { delete this.stories[jid] }
      })
   }

   public config({ max, uri }: StoreConfig): this {
      if (max) this.max = max
      if (uri && uri !== this.uri) { this.uri = uri; this.initDB() }
      return this
   }
}

const store = new Store('stores')
export default store