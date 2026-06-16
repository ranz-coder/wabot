import { type Contact, type ConnectionState, type PresenceData, BotClient, WAMessage, StoreConfig } from '../interface.js'
import path from 'node:path'
import fs from 'node:fs'
import { noSuffix, getKeyAuthor } from '../utils.js'

let DatabaseConstructor: any = null
const loadSqlite = async () => {
   if (DatabaseConstructor) return DatabaseConstructor
   try {
      const moduleName = String('better-sqlite3')
      const module = await import(moduleName)
      DatabaseConstructor = module.default || module
      return DatabaseConstructor
   } catch (e) {
      return null
   }
}

class Store {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public database: string

   private db: any = null
   private fallbackStore: Record<string, WAMessage[]> | null = null
   private fallbackChats: Record<string, any> | null = null

   public contacts: Record<string, Contact> = Object.create(null)
   public stories: Record<string, any[]> = Object.create(null)
   public presences: Record<string, { [participant: string]: PresenceData }> = Object.create(null)
   public state: ConnectionState = { connection: 'close' }
   public messageId: Map<string, Map<string, { at: number }>> = new Map()

   private insertStmt: any = null
   private cleanupStmt: any = null
   private getOneStmt: any = null
   private getLimitStmt: any = null
   private getAllDescStmt: any = null
   private getAllWithOffsetStmt: any = null
   private countStmt: any = null
   private deleteWithOffsetStmt: any = null

   private getChatStmt: any = null
   private insertChatStmt: any = null
   private getAllChatIdsStmt: any = null

   private chatsProxyInstance: Record<string, any>

   constructor(dir: string = 'stores', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.database = 'sqlite'

      this.fallbackStore = Object.create(null)
      this.fallbackChats = Object.create(null)

      this.chatsProxyInstance = this.createChatsProxy()

      if (process.env?.USE_STORE?.includes('sqlite')) {
         this.initDB()
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
      const SQLite = await loadSqlite()

      if (!SQLite) {
         console.warn('[store-sqlite] better-sqlite3 module not installed! Running in RAM-only mode.')
         return
      }

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      const dbPath = path.join(this.storeDir, 'store.db')

      if (this.db) {
         this.db.close()
      }

      try {
         this.db = new SQLite(dbPath)

         this.db.pragma('journal_mode = WAL')
         this.db.pragma('synchronous = NORMAL')
         this.db.pragma('temp_store = MEMORY')
         this.db.pragma('cache_size = 10000')

         this.db.exec(`
            CREATE TABLE IF NOT EXISTS messages (
               jid TEXT,
               id TEXT,
               data TEXT,
               created_at INTEGER,
               PRIMARY KEY (jid, id)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_jid_created_at ON messages (jid, created_at DESC);
            CREATE TABLE IF NOT EXISTS chats (
               id TEXT PRIMARY KEY,
               data TEXT
            );
         `)

         this.insertStmt = this.db.prepare('INSERT OR REPLACE INTO messages (jid, id, data, created_at) VALUES (?, ?, ?, ?)')
         this.cleanupStmt = this.db.prepare('DELETE FROM messages WHERE jid = ? AND id NOT IN (SELECT id FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?)')
         this.getOneStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? AND id = ?')
         this.getLimitStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC LIMIT ?')
         this.getAllDescStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at DESC')
         this.getAllWithOffsetStmt = this.db.prepare('SELECT data FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?')
         this.countStmt = this.db.prepare('SELECT COUNT(*) as count FROM messages WHERE jid = ?')
         this.deleteWithOffsetStmt = this.db.prepare('DELETE FROM messages WHERE jid = ? AND id IN (SELECT id FROM messages WHERE jid = ? ORDER BY created_at ASC LIMIT -1 OFFSET ?)')

         this.getChatStmt = this.db.prepare('SELECT data FROM chats WHERE id = ?')
         this.insertChatStmt = this.db.prepare('INSERT OR REPLACE INTO chats (id, data) VALUES (?, ?)')
         this.getAllChatIdsStmt = this.db.prepare('SELECT id FROM chats')

      } catch (error) {
         console.error('[store-sqlite] Failed to initialize SQLite database. Falling back to RAM-only mode:', error)
         this.db = null
      }
   }

   public config({ dir, max }: StoreConfig): this {
      let dbNeedsReinit = false

      if (dir) {
         const newDir = path.join(process.cwd(), '.cache', dir)
         if (this.storeDir !== newDir) {
            this.storeDir = newDir
            dbNeedsReinit = true
         }
      }

      if (max !== undefined) {
         this.max = max
      }

      if (dbNeedsReinit) {
         this.initDB()
      }

      return this
   }

   private createChatsProxy(): Record<string, any> {
      const self = this
      return new Proxy(Object.create(null), {
         get: (target, prop) => {
            if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop)) return undefined
            if (self.db && self.getChatStmt) {
               try {
                  const row = self.getChatStmt.get(prop) as { data: string } | undefined
                  return row ? JSON.parse(row.data) : undefined
               } catch { return undefined }
            }
            return self.fallbackChats?.[prop]
         },
         set: (target, prop, value) => {
            if (typeof prop !== 'string') return false
            if (self.db && self.insertChatStmt) {
               try {
                  self.insertChatStmt.run(prop, JSON.stringify(self.toPOJO(value)))
                  return true
               } catch { return false }
            }
            if (self.fallbackChats) {
               self.fallbackChats[prop] = value
               return true
            }
            return false
         },
         ownKeys: () => {
            if (self.db && self.getAllChatIdsStmt) {
               try {
                  const rows = self.getAllChatIdsStmt.all() as { id: string }[]
                  return rows.map(r => r.id)
               } catch { return [] }
            }
            return self.fallbackChats ? Object.keys(self.fallbackChats) : []
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

   public loadMessage(jid: string, id: string): WAMessage | null {
      if (this.db && this.getOneStmt) {
         try {
            const row = this.getOneStmt.get(jid, id) as { data: string } | undefined
            return row ? (JSON.parse(row.data) as WAMessage) : null
         } catch (error) {
            console.error(`[store-sqlite] Failed to load message ${id} for JID ${jid}:`, error)
            return null
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         return list.find(v => v.key?.id === id || (v as any).id === id) || null
      }

      return null
   }

   public loadMessages(jid: string, count?: number): WAMessage[] | null {
      if (this.db) {
         try {
            let rows: { data: string }[] = []

            if (count !== undefined && count > 0) {
               if (this.getLimitStmt) {
                  rows = this.getLimitStmt.all(jid, count) as { data: string }[]
               }
            } else {
               if (this.getAllDescStmt) {
                  rows = this.getAllDescStmt.all(jid) as { data: string }[]
               }
            }

            if (rows.length === 0) return null

            return rows.map(row => JSON.parse(row.data) as WAMessage)
         } catch (error) {
            console.error(`[store-sqlite] Failed to load messages for JID ${jid}:`, error)
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

   public addMessage(jid: string, msg: WAMessage): void {
      if (this.db && this.insertStmt && this.cleanupStmt) {
         const msgId = msg.key?.id || (msg as any).id
         if (msgId) {
            try {
               this.insertStmt.run(jid, msgId, JSON.stringify(this.toPOJO(msg)), Date.now())
               this.cleanupStmt.run(jid, jid, this.max)
            } catch (error) {
               console.error('[store-sqlite] Failed to save message to SQLite:', error)
            }
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

   public getAllMessages(jid: string, offset: number = 0): WAMessage[] & { count(): number; clear(): void } {
      if (this.db && this.getAllWithOffsetStmt && this.countStmt && this.deleteWithOffsetStmt) {
         try {
            const rows = this.getAllWithOffsetStmt.all(jid, offset) as { data: string }[]
            const messages = rows.map(row => JSON.parse(row.data) as WAMessage) as WAMessage[] & { count(): number; clear(): void }

            messages.count = () => {
               try {
                  const result = this.countStmt.get(jid) as { count: number } | undefined
                  const total = result ? result.count : 0
                  return Math.max(0, total - offset)
               } catch (error) {
                  console.error('[store-sqlite] Failed to count messages:', error)
                  return 0
               }
            }

            messages.clear = () => {
               try {
                  this.deleteWithOffsetStmt.run(jid, jid, offset)
               } catch (error) {
                  console.error(`[store-sqlite] Failed to clear messages for JID ${jid} with offset ${offset}:`, error)
               }
            }

            return messages
         } catch (error) {
            console.error(`[store-sqlite] Failed to get all messages for JID ${jid}:`, error)
         }
      }

      if (this.fallbackStore) {
         const list = this.fallbackStore[jid] || []
         const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): number; clear(): void }

         sliced.count = () => {
            const currentList = this.fallbackStore?.[jid] || []
            return Math.max(0, currentList.length - offset)
         }

         sliced.clear = () => {
            if (this.fallbackStore) {
               if (offset === 0) {
                  delete this.fallbackStore[jid]
               } else {
                  const currentList = this.fallbackStore[jid] || []
                  if (offset < currentList.length) {
                     this.fallbackStore[jid] = currentList.slice(0, offset)
                  }
               }
            }
         }

         return sliced
      }

      const emptyResult = [] as unknown as WAMessage[] & { count(): number; clear(): void }
      emptyResult.count = () => 0
      emptyResult.clear = () => { }
      return emptyResult
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

   public updateMessageWithReceipt(msg: any, receipt: any): void {
      if (!msg) return
      msg.userReceipt = msg.userReceipt || []
      const recp = msg.userReceipt.find((m: any) => m.userJid === receipt.userJid)
      if (recp) Object.assign(recp, receipt)
      else msg.userReceipt.push(receipt)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id
      if (this.db && this.insertStmt && jid && id) {
         try {
            this.insertStmt.run(jid, id, JSON.stringify(this.toPOJO(msg)), Date.now())
         } catch (error) {
            console.error('[store-sqlite] Failed to update receipt in SQLite:', error)
         }
      }
   }

   public updateMessageWithReaction(msg: any, reaction: any): void {
      if (!msg) return
      const authorID = getKeyAuthor(reaction.key)
      msg.reactions = (msg.reactions || []).filter((r: any) => getKeyAuthor(r.key) !== authorID)
      if (reaction.text) msg.reactions.push(reaction)

      const jid = msg.key?.remoteJid
      const id = msg.key?.id || msg.id
      if (this.db && this.insertStmt && jid && id) {
         try {
            this.insertStmt.run(jid, id, JSON.stringify(this.toPOJO(msg)), Date.now())
         } catch (error) {
            console.error('[store-sqlite] Failed to update reaction in SQLite:', error)
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