import { type Contact, type ConnectionState, type PresenceData, BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'
import { noSuffix, getKeyAuthor } from '../utils.js'

let MySQLConstructor: any = null
const loadMySQL = async () => {
   if (MySQLConstructor) return MySQLConstructor
   try {
      const moduleName = String('mysql2/promise')
      const module = await import(moduleName)
      MySQLConstructor = module.default || module
      return MySQLConstructor
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

   private chatsCache = new Map<string, any>()
   private isChatsPreloaded = false

   constructor(dir: string = 'stores', max: number = 250, uri?: string) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.uri = uri || process.env.USE_STORE
      this.database = 'mysql'

      if (process.env?.USE_STORE?.includes('mysql')) {
         this.initDB()
      } else {
         this.fallbackStore = Object.create(null)
         this.fallbackChats = Object.create(null)
      }

      setInterval(() => this.cleanupExpiredMessages(), 120000)
   }

   private async initDB(): Promise<void> {
      const mysql = await loadMySQL()

      if (!mysql) {
         console.warn('[message-store-mysql] mysql2 module not installed! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         this.fallbackChats = Object.create(null)
         return
      }

      if (!this.uri) {
         console.warn('[message-store-mysql] MySQL URI not provided! Running in RAM-only mode.')
         this.fallbackStore = Object.create(null)
         this.fallbackChats = Object.create(null)
         return
      }

      if (this.pool) {
         try {
            await this.pool.end()
         } catch (e) { }
      }

      try {
         this.pool = mysql.createPool(this.uri)

         await this.pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
               jid VARCHAR(255) NOT NULL,
               id VARCHAR(255) NOT NULL,
               data LONGTEXT NOT NULL,
               created_at BIGINT NOT NULL,
               PRIMARY KEY (jid, id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
         `)

         await this.pool.query(`
            CREATE TABLE IF NOT EXISTS chats (
               id VARCHAR(255) NOT NULL,
               data LONGTEXT NOT NULL,
               PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
         `)

         await this.preloadChats()
      } catch (error) {
         console.error('[message-store-mysql] Failed to initialize MySQL. Falling back to RAM-only mode:', error)
         this.pool = null
         this.fallbackStore = Object.create(null)
         this.fallbackChats = Object.create(null)
      }
   }

   private async preloadChats(): Promise<void> {
      if (!this.pool) return
      try {
         const [rows]: any = await this.pool.query('SELECT id, data FROM chats')
         for (const row of rows) {
            this.chatsCache.set(row.id, JSON.parse(row.data))
         }
         this.isChatsPreloaded = true
      } catch (error) {
         console.error('[message-store-mysql] Failed to preload chats:', error)
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

   public get chats(): Record<string, any> {
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
               self.pool.query('INSERT INTO chats (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data)', [prop, JSON.stringify(value)]).catch(() => {})
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

   public async loadMessage(jid: string, id: string): Promise<WAMessage | null> {
      if (this.pool) {
         try {
            const [rows]: any = await this.pool.query(
               'SELECT data FROM messages WHERE jid = ? AND id = ?',
               [jid, id]
            )
            return rows.length > 0 ? (JSON.parse(rows[0].data) as WAMessage) : null
         } catch (error) {
            console.error(`[message-store-mysql] Failed to load message ${id} for JID ${jid}:`, error)
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
            let query = 'SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC'
            const params: any[] = [jid]

            if (count !== undefined && count > 0) {
               query += ' LIMIT ?'
               params.push(count)
            }

            const [rows]: any = await this.pool.query(query, params)
            if (rows.length === 0) return null

            return rows.map((row: any) => JSON.parse(row.data) as WAMessage)
         } catch (error) {
            console.error(`[message-store-mysql] Failed to load messages for JID ${jid}:`, error)
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
               'INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)',
               [jid, msgId, JSON.stringify(msg), Date.now()]
            )
            await this.pool.query(
               'DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?) as tmp)',
               [jid, jid, this.max]
            )
         } catch (error) {
            console.error('[message-store-mysql] Failed to save message to MySQL:', error)
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
               const [rows]: any = await self.pool.query(
                  'SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT 999999999 OFFSET ?',
                  [jid, offset]
               )
               list = rows.map((row: any) => JSON.parse(row.data) as WAMessage)
            } catch (error) {
               console.error(`[message-store-mysql] Failed to get messages for JID ${jid}:`, error)
            }
         } else if (self.fallbackStore) {
            const rawList = self.fallbackStore[jid] || []
            list = offset > 0 ? rawList.slice(offset) : rawList
         }

         const sliced = list as WAMessage[] & { count(): Promise<number>; clear(): Promise<void> }

         sliced.count = async () => {
            if (self.pool) {
               try {
                  const [rows]: any = await self.pool.query(
                     'SELECT COUNT(*) as count FROM messages WHERE jid = ?',
                     [jid]
                  )
                  const total = rows[0]?.count || 0
                  return Math.max(0, total - offset)
               } catch (error) {
                  console.error(`[message-store-mysql] Failed to count messages for JID ${jid}:`, error)
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
                     await self.pool.query('DELETE FROM messages WHERE jid = ?', [jid])
                  } else {
                     await self.pool.query(
                        'DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT ?) as tmp)',
                        [jid, jid, offset]
                     )
                  }
               } catch (error) {
                  console.error(`[message-store-mysql] Failed to clear messages for JID ${jid}:`, error)
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
               const [rows]: any = await self.pool.query(
                  'SELECT COUNT(*) as count FROM messages WHERE jid = ?',
                  [jid]
               )
               const total = rows[0]?.count || 0
               return Math.max(0, total - offset)
            } catch (error) {
               console.error(`[message-store-mysql] Failed to count messages for JID ${jid}:`, error)
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
                  await self.pool.query('DELETE FROM messages WHERE jid = ?', [jid])
               } else {
                  await self.pool.query(
                     'DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT ?) as tmp)',
                     [jid, jid, offset]
                  )
               }
            } catch (error) {
               console.error(`[message-store-mysql] Failed to clear messages for JID ${jid}:`, error)
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
      if (this.pool && jid && id) {
         try {
            await this.pool.query(
               'INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)',
               [jid, id, JSON.stringify(msg), Date.now()]
            )
         } catch (error) {
            console.error('[message-store-mysql] Failed to update receipt in MySQL:', error)
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
      if (this.pool && jid && id) {
         try {
            await this.pool.query(
               'INSERT INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data), created_at = VALUES(created_at)',
               [jid, id, JSON.stringify(msg), Date.now()]
            )
         } catch (error) {
            console.error('[message-store-mysql] Failed to update reaction in MySQL:', error)
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

      if (this.fallbackStore) {
         Object.values(this.fallbackStore).forEach((msgArray) => {
            if (msgArray && msgArray.length > 100) {
               msgArray.splice(0, msgArray.length - 100)
            }
         })
      }

      Object.values(this.stories).forEach((storyArray) => {
         if (storyArray && storyArray.length > 30) {
            storyArray.splice(0, storyArray.length - 30)
         }
      })
   }
}

const store = new Store('stores')

export default store