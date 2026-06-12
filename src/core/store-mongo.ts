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
   private maxCachedJids = 15
   private writeQueues = new Map<string, Promise<any>>()

   private chatsCache = new Map<string, any>()
   private isChatsPreloaded = false
   private chatsProxyInstance: Record<string, any>

   constructor(dir: string = 'stores', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'mongodb'

      this.fallbackStore = Object.create(null)
      this.fallbackChats = Object.create(null)

      this.chatsProxyInstance = this.createChatsProxy()

      if (process.env?.USE_STORE?.includes('mongodb')) {
         this.initDB()
      }

      setInterval(() => this.cleanupExpiredMessages(), 120000)
   }

   private async initDB(): Promise<void> {
      const MongoClient = await loadMongo()

      if (!MongoClient) {
         console.warn('[store-mongodb] mongodb module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[store-mongodb] MongoDB URI not provided! Running in RAM-only mode.')
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
         this.chatsCollection = this.db.collection('chats')

         await this.messagesCollection.createIndex({ jid: 1, id: 1 }, { unique: true })
         await this.messagesCollection.createIndex({ jid: 1, created_at: -1 })
         await this.chatsCollection.createIndex({ id: 1 }, { unique: true })

         await this.preloadChats()
      } catch (error) {
         console.error('[store-mongodb] Failed to initialize MongoDB. Falling back to RAM-only mode:', error)
         this.mongoClient = null
         this.db = null
         this.messagesCollection = null
         this.chatsCollection = null
      }
   }

   private async preloadChats(): Promise<void> {
      if (!this.chatsCollection) return
      try {
         const docs = await this.chatsCollection.find({}).toArray()
         for (const doc of docs) {
            this.chatsCache.set(doc.id, doc.data)
         }
         this.isChatsPreloaded = true
      } catch (error) {
         console.error('[store-mongodb] Failed to preload chats:', error)
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

   private createChatsProxy(): Record<string, any> {
      const self = this
      return new Proxy(Object.create(null), {
         get: (target, prop) => {
            if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop)) return undefined
            return self.chatsCache.get(prop) || self.fallbackChats?.[prop]
         },
         set: (target, prop, value) => {
            if (typeof prop !== 'string') return false
            self.chatsCache.set(prop, value)
            if (self.chatsCollection) {
               self.chatsCollection.updateOne(
                  { id: prop },
                  { $set: { data: value } },
                  { upsert: true }
               ).catch(() => { })
            } else if (self.fallbackChats) {
               self.fallbackChats[prop] = value
            }
            return true
         },
         ownKeys: () => {
            return self.chatsCollection ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : [])
         },
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

   private touchJid(jid: string): void {
      const data = this.cache.get(jid)
      if (data) {
         this.cache.delete(jid)
         this.cache.set(jid, data)
      }
   }

   private evictOldestCache(): void {
      if (this.cache.size > this.maxCachedJids) {
         for (const [key] of this.cache) {
            if (this.writeQueues.has(key)) continue

            this.cache.delete(key)
            if (this.cache.size <= this.maxCachedJids) break
         }
      }
   }

   private async getMongoData(jid: string): Promise<WAMessage[]> {
      if (this.cache.has(jid)) {
         this.touchJid(jid)
         return this.cache.get(jid)!
      }

      if (!this.messagesCollection) return []
      try {
         const docs = await this.messagesCollection
            .find({ jid })
            .sort({ created_at: 1 })
            .toArray()
         const data = docs.map((doc: any) => doc.data as WAMessage)
         this.cache.set(jid, data)
         this.evictOldestCache()
         return data
      } catch (error) {
         console.error(`[store-mongodb] Failed to load messages for JID ${jid}:`, error)
         return []
      }
   }

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      const list = await this.getMongoData(jid)
      return list.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public async loadMessages(jid: string, count?: number): Promise<WAMessage[] | null> {
      const list = await this.getMongoData(jid)
      if (list.length === 0) return null

      const slice = count ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public async addMessage(jid: string, msg: WAMessage): Promise<void> {
      const list = await this.getMongoData(jid)
      list.push(msg)

      if (list.length > this.max) {
         list.splice(0, list.length - this.max)
      }

      const msgId = msg.key?.id || (msg as any).id
      if (this.messagesCollection && msgId) {
         const previous = this.writeQueues.get(jid) || Promise.resolve()
         const current = previous
            .then(async () => {
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
                  console.error('[store-mongodb] Failed to save message to MongoDB:', error)
               }
            })
            .finally(() => {
               if (this.writeQueues.get(jid) === current) {
                  this.writeQueues.delete(jid)
               }
            })
         this.writeQueues.set(jid, current)
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
            list = await self.getMongoData(jid)
         } else if (self.fallbackStore) {
            const rawList = self.fallbackStore[jid] || []
            list = offset > 0 ? rawList.slice(offset) : rawList
         }

         const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.messagesCollection) {
               const currentList = await self.getMongoData(jid)
               return Math.max(0, currentList.length - offset)
            }
            if (self.fallbackStore) {
               const total = (self.fallbackStore[jid] || []).length
               return Math.max(0, total - offset)
            }
            return 0
         }

         sliced.clear = async () => {
            self.cache.delete(jid)

            if (self.messagesCollection) {
               const previous = self.writeQueues.get(jid) || Promise.resolve()
               const current = previous
                  .then(async () => {
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
                        console.error(`[store-mongodb] Failed to clear messages for JID ${jid}:`, error)
                     }
                  })
                  .finally(() => {
                     if (self.writeQueues.get(jid) === current) {
                        self.writeQueues.delete(jid)
                     }
                  })
               self.writeQueues.set(jid, current)
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
            const currentList = await self.getMongoData(jid)
            return Math.max(0, currentList.length - offset)
         }
         if (self.fallbackStore) {
            const total = (self.fallbackStore[jid] || []).length
            return Math.max(0, total - offset)
         }
         return 0
      }

      promiseWithMethods.clear = async () => {
         self.cache.delete(jid)

         if (self.messagesCollection) {
            const previous = self.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
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
                     console.error(`[store-mongodb] Failed to clear messages for JID ${jid}:`, error)
                  }
               })
               .finally(() => {
                  if (self.writeQueues.get(jid) === current) {
                     self.writeQueues.delete(jid)
                  }
               })
            self.writeQueues.set(jid, current)
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

   public chatUpdate(updates: any[]): void {
      for (const update of updates) {
         if (update.id) {
            const id = update.id
            this.chats[id] = Object.assign(this.chats[id] || { id }, update)
         }
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
      if (!id) return null
      if (this.contacts[id]) return this.contacts[id]
      const found = Object.values(this.contacts).find((c: any) => c.id === id || c.jid === id || c.sender_pn === id)
      return found || null
   }

   public getAllContacts(offset: number = 0) {
      const list = Object.values(this.contacts)
      const sliced = (offset > 0 ? list.slice(offset) : list) as any[] & { count(): number; clear(): void }

      sliced.count = () => {
         const currentList = Object.values(this.contacts)
         return Math.max(0, currentList.length - offset)
      }

      sliced.clear = () => {
         if (offset === 0) {
            for (const key in this.contacts) {
               delete this.contacts[key]
            }
         } else {
            const keys = Object.keys(this.contacts)
            if (offset < keys.length) {
               for (let i = offset; i < keys.length; i++) {
                  delete this.contacts[keys[i]]
               }
            }
         }
      }

      return sliced
   }

   public async updateMessageWithReceipt(msg: any, receipt: any): Promise<void> {
      if (!msg) return
      msg.userReceipt = msg.userReceipt || []
      const recp = msg.userReceipt.find((m: any) => m.userJid === receipt.userJid)
      if (recp) Object.assign(recp, receipt)
      else msg.userReceipt.push(receipt)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id
      if (jid && id) {
         const list = await this.getMongoData(jid)
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
         }

         if (this.messagesCollection) {
            const previous = this.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
                  try {
                     await this.messagesCollection.updateOne(
                        { jid, id },
                        { $set: { data: msg, created_at: Date.now() } },
                        { upsert: true }
                     )
                  } catch (error) {
                     console.error('[store-mongodb] Failed to update receipt in MongoDB:', error)
                  }
               })
               .finally(() => {
                  if (this.writeQueues.get(jid) === current) {
                     this.writeQueues.delete(jid)
                  }
               })
            this.writeQueues.set(jid, current)
         }
      }
   }

   public async updateMessageWithReaction(msg: any, reaction: any): Promise<void> {
      if (!msg) return
      const authorID = getKeyAuthor(reaction.key)
      msg.reactions = (msg.reactions || []).filter((r: any) => getKeyAuthor(r.key) !== authorID)
      if (reaction.text) msg.reactions.push(reaction)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id
      if (jid && id) {
         const list = await this.getMongoData(jid)
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
         }

         if (this.messagesCollection) {
            const previous = this.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
                  try {
                     await this.messagesCollection.updateOne(
                        { jid, id },
                        { $set: { data: msg, created_at: Date.now() } },
                        { upsert: true }
                     )
                  } catch (error) {
                     console.error('[store-mongodb] Failed to update reaction in MongoDB:', error)
                  }
               })
               .finally(() => {
                  if (this.writeQueues.get(jid) === current) {
                     this.writeQueues.delete(jid)
                  }
               })
            this.writeQueues.set(jid, current)
         }
      }
   }

   public loadStories(jid: string, count?: number): any[] | null {
      const list = this.stories[jid]
      if (!list || list.length === 0) return null
      const slice = count && count > 0 ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public loadStory(jid: string, id: string): any | null {
      const list = this.stories[jid]
      if (!list || list.length === 0) return null
      return list.find((v: any) => v.key?.id === id || v.id === id) || null
   }

   public addStory(jid: string, story: any): void {
      if (!this.stories[jid]) {
         this.stories[jid] = []
      }
      this.stories[jid].push(story)

      if (this.stories[jid].length > this.max) {
         this.stories[jid].splice(0, this.stories[jid].length - this.max)
      }
   }

   public getAllStories(jid: string, offset: number = 0) {
      const list = this.stories[jid] || []
      const sliced = (offset > 0 ? list.slice(offset) : list) as any[] & { count(): number; clear(): void }

      sliced.count = () => {
         const currentList = this.stories[jid] || []
         return Math.max(0, currentList.length - offset)
      }

      sliced.clear = () => {
         if (offset === 0) {
            delete this.stories[jid]
         } else {
            const currentList = this.stories[jid] || []
            if (offset < currentList.length) {
               this.stories[jid] = currentList.slice(0, offset)
            }
         }
      }

      return sliced
   }

   public recordMessageId(sock: any, msg: { [key: string]: any }): boolean {
      if (msg.fromMe) return true

      const id = msg.key?.id || msg.id
      if (!id) return true

      const instance = noSuffix(sock.user.id)

      let instanceMap = this.messageId.get(instance)

      if (!instanceMap) {
         instanceMap = new Map()
         this.messageId.set(instance, instanceMap)
      }

      if (instanceMap.has(id) && !msg.updated) return false

      instanceMap.set(id, { at: Date.now() })

      if (instanceMap.size > 5000) {
         const firstKey = instanceMap.keys().next().value
         if (firstKey) instanceMap.delete(firstKey)
      }
      return true
   }

   private cleanupExpiredMessages(): void {
      if (this.fallbackStore) {
         Object.values(this.fallbackStore).forEach((msgArray) => {
            if (msgArray && msgArray.length > 100) {
               msgArray.splice(0, msgArray.length - 100)
            }
         })
      }

      const now = Date.now()
      this.messageId.forEach((instanceMap, instance) => {
         instanceMap.forEach((value, msgId) => {
            if (now - value.at > 900000) instanceMap.delete(msgId)
         })
         if (instanceMap.size === 0) this.messageId.delete(instance)
      })

      Object.values(this.stories).forEach((storyArray) => {
         if (storyArray && storyArray.length > 30) {
            storyArray.splice(0, storyArray.length - 30)
         }
      })
   }
}

const store = new Store('stores')

export default store