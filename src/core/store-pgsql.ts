import { type Contact, type ConnectionState, type PresenceData, BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'
import { noSuffix, getKeyAuthor } from '../utils.js'

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

class Store {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public uri: string | undefined
   public database: string

   private pool: any = null
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
      this.database = 'pgsql'

      this.fallbackStore = Object.create(null)
      this.fallbackChats = Object.create(null)

      this.chatsProxyInstance = this.createChatsProxy()

      if (process.env?.USE_STORE?.includes('pg')) {
         this.initDB()
      }

      setInterval(() => this.cleanupExpiredMessages(), 120000)
   }

   private async initDB(): Promise<void> {
      const Pool = await loadPG()

      if (!Pool) {
         console.warn('[store-pg] pg module not installed! Running in RAM-only mode.')
         return
      }

      if (!this.uri) {
         console.warn('[store-pg] PostgreSQL URI not provided! Running in RAM-only mode.')
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
            );
            CREATE INDEX IF NOT EXISTS idx_messages_jid_created_at ON messages (jid, created_at DESC);
            CREATE TABLE IF NOT EXISTS chats (
               id VARCHAR(255) NOT NULL,
               data TEXT NOT NULL,
               PRIMARY KEY (id)
            );
         `)

         await this.preloadChats()
      } catch (error) {
         console.error('[store-pg] Failed to initialize PostgreSQL. Falling back to RAM-only mode:', error)
         this.pool = null
      }
   }

   private async preloadChats(): Promise<void> {
      if (!this.pool) return
      try {
         const { rows }: any = await this.pool.query('SELECT id, data FROM chats')
         for (const row of rows) {
            this.chatsCache.set(row.id, JSON.parse(row.data))
         }
         this.isChatsPreloaded = true
      } catch (error) {
         console.error('[store-pg] Failed to preload chats:', error)
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
            if (self.pool) {
               self.pool.query('INSERT INTO chats (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data', [prop, JSON.stringify(value)]).catch(() => { })
            } else if (self.fallbackChats) {
               self.fallbackChats[prop] = value
            }
            return true
         },
         ownKeys: () => {
            return self.pool ? Array.from(self.chatsCache.keys()) : (self.fallbackChats ? Object.keys(self.fallbackChats) : [])
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

   private async getPGData(jid: string): Promise<WAMessage[]> {
      if (this.cache.has(jid)) {
         this.touchJid(jid)
         return this.cache.get(jid)!
      }

      if (!this.pool) return []
      try {
         const { rows }: any = await this.pool.query(
            'SELECT data FROM messages WHERE jid = $1 ORDER BY created_at ASC',
            [jid]
         )
         const data = rows.map((row: any) => JSON.parse(row.data) as WAMessage)
         this.cache.set(jid, data)
         this.evictOldestCache()
         return data
      } catch (error) {
         console.error(`[store-pg] Failed to load messages for JID ${jid}:`, error)
         return []
      }
   }

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      const list = await this.getPGData(jid)
      return list.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public async loadMessages(jid: string, count?: number): Promise<WAMessage[] | null> {
      const list = await this.getPGData(jid)
      if (list.length === 0) return null

      const slice = count ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public async addMessage(jid: string, msg: WAMessage): Promise<void> {
      const list = await this.getPGData(jid)
      list.push(msg)

      if (list.length > this.max) {
         list.splice(0, list.length - this.max)
      }

      const msgId = msg.key?.id || (msg as any).id
      if (this.pool && msgId) {
         const previous = this.writeQueues.get(jid) || Promise.resolve()
         const current = previous
            .then(async () => {
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
                  console.error('[store-pg] Failed to save message to PG:', error)
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

         if (self.pool) {
            list = await self.getPGData(jid)
         } else if (self.fallbackStore) {
            list = self.fallbackStore[jid] || []
         }

         const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.pool) {
               const currentList = await self.getPGData(jid)
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

            if (self.pool) {
               const previous = self.writeQueues.get(jid) || Promise.resolve()
               const current = previous
                  .then(async () => {
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
                        console.error(`[store-pg] Failed to clear messages for JID ${jid}:`, error)
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
         if (self.pool) {
            const currentList = await self.getPGData(jid)
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

         if (self.pool) {
            const previous = self.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
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
                     console.error(`[store-pg] Failed to clear messages for JID ${jid}:`, error)
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
         const list = await this.getPGData(jid)
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
         }

         if (this.pool) {
            const previous = this.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
                  try {
                     await this.pool.query(
                        'INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at',
                        [jid, id, JSON.stringify(msg), Date.now()]
                     )
                  } catch (error) {
                     console.error('[store-pg] Failed to update receipt in PG:', error)
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
         const list = await this.getPGData(jid)
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
         }

         if (this.pool) {
            const previous = this.writeQueues.get(jid) || Promise.resolve()
            const current = previous
               .then(async () => {
                  try {
                     await this.pool.query(
                        'INSERT INTO messages (jid, id, data, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (jid, id) DO UPDATE SET data = EXCLUDED.data, created_at = EXCLUDED.created_at',
                        [jid, id, JSON.stringify(msg), Date.now()]
                     )
                  } catch (error) {
                     console.error('[store-pg] Failed to update reaction in PG:', error)
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