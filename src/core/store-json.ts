import { type Contact, type ConnectionState, type PresenceData, BotClient, WAMessage, StoreConfig } from '../interface.js'
import fs from 'node:fs'
import path from 'node:path'
import { noSuffix, getKeyAuthor } from '../utils.js'

class Store {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public database: string

   private cache = new Map<string, WAMessage[]>()
   private maxCachedJids = 10
   private pendingJidWrites = new Set<string>()
   private writeQueues = new Map<string, Promise<any>>()
   private fallbackStore: Record<string, WAMessage[]> | null = null
   private fallbackChats: Record<string, any> | null = null
   private fallbackContacts: Record<string, Contact> | null = null

   private contactsCache = new Map<string, Contact>()
   private contactsFilePath: string
   private contactsPendingWrite = false
   private contactsProxyInstance: Record<string, Contact>

   public stories: Record<string, any[]> = Object.create(null)
   public presences: Record<string, { [participant: string]: PresenceData }> = Object.create(null)
   public state: ConnectionState = { connection: 'close' }
   public messageId: Map<string, Map<string, { at: number }>> = new Map()

   private chatsCache = new Map<string, any>()
   private chatsFilePath: string
   private chatsPendingWrite = false
   private chatsProxyInstance: Record<string, any>

   private storiesCache = new Map<string, any[]>()
   private storiesFilePath: string
   private storiesPendingWrite = false

   constructor(dir: string = 'stores', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.database = 'json'
      this.chatsFilePath = path.join(this.storeDir, 'chats.json')
      this.contactsFilePath = path.join(this.storeDir, 'contacts.json')
      this.storiesFilePath = path.join(this.storeDir, 'stories.json')

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      this.chatsProxyInstance = this.createChatsProxy()
      this.contactsProxyInstance = this.createContactsProxy()

      this.loadChats()
      this.loadContacts()
      this.loadStoriesData()

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

   private loadChats(): void {
      try {
         if (fs.existsSync(this.chatsFilePath)) {
            const content = fs.readFileSync(this.chatsFilePath, 'utf-8')
            const list = JSON.parse(content) as any[]
            list.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0))
            const capped = list.slice(0, 500)
            for (const chat of capped) {
               if (chat.id) this.chatsCache.set(chat.id, chat)
            }
         }
      } catch (error: any) {
         if (error.code !== 'ENOENT') {
            console.error('[store-json] Failed to load chats:', error)
         }
      }
   }

   private loadContacts(): void {
      try {
         if (fs.existsSync(this.contactsFilePath)) {
            const content = fs.readFileSync(this.contactsFilePath, 'utf-8')
            const list = JSON.parse(content) as any[]
            list.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0))
            const capped = list.slice(0, 1000)
            for (const contact of capped) {
               if (contact.jid) this.contactsCache.set(contact.jid, contact)
            }
         }
      } catch (error: any) {
         if (error.code !== 'ENOENT') {
            console.error('[store-json] Failed to load contacts:', error)
         }
      }
   }

   private loadStoriesData(): void {
      try {
         if (fs.existsSync(this.storiesFilePath)) {
            const content = fs.readFileSync(this.storiesFilePath, 'utf-8')
            const parsed = JSON.parse(content) as Record<string, any[]>
            for (const [jid, list] of Object.entries(parsed)) {
               this.storiesCache.set(jid, list)
            }
         }
      } catch (error: any) {
         if (error.code !== 'ENOENT') {
            console.error('[store-json] Failed to load stories:', error)
         }
      }
   }

   private enqueueWrite(key: string, writeFn: () => Promise<void>): void {
      const previous = this.writeQueues.get(key) || Promise.resolve()
      const current = previous
         .then(writeFn)
         .catch((err) => console.error(`[store] Write error on ${key}:`, err))
         .finally(() => {
            if (this.writeQueues.get(key) === current) {
               this.writeQueues.delete(key)
            }
         })
      this.writeQueues.set(key, current)
   }

   private writeChats(): void {
      if (this.chatsPendingWrite) return
      this.chatsPendingWrite = true

      setTimeout(() => {
         this.chatsPendingWrite = false
         const list = this.toPOJO(Array.from(this.chatsCache.values()))

         this.enqueueWrite('chats', async () => {
            const tempPath = `${this.chatsFilePath}.tmp`
            try {
               await fs.promises.writeFile(tempPath, JSON.stringify(list), 'utf-8')
               await fs.promises.rename(tempPath, this.chatsFilePath)
            } catch (error) {
               console.error('[store-json] Failed to write chats to disk:', error)
            }
         })
      }, 2000)
   }

   private writeContacts(): void {
      if (this.contactsPendingWrite) return
      this.contactsPendingWrite = true

      setTimeout(() => {
         this.contactsPendingWrite = false
         const list = this.toPOJO(Array.from(this.contactsCache.values()))

         this.enqueueWrite('contacts', async () => {
            const tempPath = `${this.contactsFilePath}.tmp`
            try {
               await fs.promises.writeFile(tempPath, JSON.stringify(list), 'utf-8')
               await fs.promises.rename(tempPath, this.contactsFilePath)
            } catch (error) {
               console.error('[store-json] Failed to write contacts to disk:', error)
            }
         })
      }, 2000)
   }

   private writeStoriesData(): void {
      if (this.storiesPendingWrite) return
      this.storiesPendingWrite = true

      setTimeout(() => {
         this.storiesPendingWrite = false
         const obj: Record<string, any[]> = {}
         for (const [jid, list] of this.storiesCache.entries()) {
            obj[jid] = list
         }
         const cleanData = this.toPOJO(obj)

         this.enqueueWrite('stories', async () => {
            const tempPath = `${this.storiesFilePath}.tmp`
            try {
               await fs.promises.writeFile(tempPath, JSON.stringify(cleanData), 'utf-8')
               await fs.promises.rename(tempPath, this.storiesFilePath)
            } catch (error) {
               console.error('[store-json] Failed to write stories to disk:', error)
            }
         })
      }, 2000)
   }

   public config({ dir, max }: StoreConfig): this {
      if (dir) {
         this.storeDir = path.join(process.cwd(), '.cache', dir)
         this.chatsFilePath = path.join(this.storeDir, 'chats.json')
         this.contactsFilePath = path.join(this.storeDir, 'contacts.json')
         this.storiesFilePath = path.join(this.storeDir, 'stories.json')

         if (!fs.existsSync(this.storeDir)) {
            fs.mkdirSync(this.storeDir, { recursive: true })
         }

         this.loadChats()
         this.loadContacts()
         this.loadStoriesData()
      }
      if (max !== undefined) {
         this.max = max
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
            const cleanedValue = self.toPOJO(value)
            cleanedValue.updated_at = Date.now()
            self.chatsCache.set(prop, cleanedValue)
            self.writeChats()
            return true
         },
         ownKeys: () => {
            return Array.from(self.chatsCache.keys())
         },
         getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
      }) as Record<string, any>
   }

   private createContactsProxy(): Record<string, Contact> {
      const self = this
      return new Proxy(Object.create(null), {
         get: (target, prop) => {
            if (typeof prop !== 'string' || ['constructor', 'prototype', 'toJSON'].includes(prop)) return undefined
            return self.contactsCache.get(prop) || self.fallbackContacts?.[prop]
         },
         set: (target, prop, value) => {
            if (typeof prop !== 'string') return false
            const cleanedValue = self.toPOJO(value)
            cleanedValue.updated_at = Date.now()
            self.contactsCache.set(prop, cleanedValue)
            self.writeContacts()
            return true
         },
         ownKeys: () => {
            return Array.from(self.contactsCache.keys())
         },
         getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true })
      }) as Record<string, Contact>
   }

   public get chats(): Record<string, any> {
      return this.chatsProxyInstance
   }

   public get contacts(): Record<string, Contact> {
      return this.contactsProxyInstance
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

   private getFilePath(jid: string): string {
      const safeJid = jid.replace(/[^a-zA-Z0-9.-]/g, '_')
      return path.join(this.storeDir, `${safeJid}.json`)
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
            if (this.pendingJidWrites.has(key)) continue

            this.cache.delete(key)
            if (this.cache.size <= this.maxCachedJids) break
         }
      }
   }

   private readJidData(jid: string): WAMessage[] {
      if (this.cache.has(jid)) {
         this.touchJid(jid)
         return this.cache.get(jid)!
      }

      const filePath = this.getFilePath(jid)
      try {
         const fileContent = fs.readFileSync(filePath, 'utf-8')
         const list = JSON.parse(fileContent) as WAMessage[]
         const data = list.slice(-100)

         this.cache.set(jid, data)
         this.evictOldestCache()

         return data
      } catch (error: any) {
         if (error.code === 'ENOENT') {
            return []
         }
         console.error(`[store-json] Failed to read JID ${jid} from JSON:`, error)
         return []
      }
   }

   private writeJidData(jid: string, data: WAMessage[]): void {
      this.cache.set(jid, data)
      this.touchJid(jid)
      this.evictOldestCache()

      if (this.pendingJidWrites.has(jid)) return
      this.pendingJidWrites.add(jid)

      setTimeout(() => {
         this.pendingJidWrites.delete(jid)
         const currentData = this.cache.get(jid)
         if (!currentData) return

         this.enqueueWrite(jid, async () => {
            const filePath = this.getFilePath(jid)
            const tempFilePath = `${filePath}.tmp`
            try {
               const cleanData = this.toPOJO(currentData)
               const jsonStr = JSON.stringify(cleanData)

               await fs.promises.writeFile(tempFilePath, jsonStr, 'utf-8')
               await fs.promises.rename(tempFilePath, filePath)
            } catch (error) {
               console.error(`[store-json] Failed to write JID ${jid} to JSON:`, error)
            }
         })
      }, 1500)
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      const list = this.readJidData(jid)
      return list.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public loadMessages(jid: string, count: number = 25): WAMessage[] | null {
      const list = this.readJidData(jid)
      if (list.length === 0) return null

      const slice = count ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public addMessage(jid: string, msg: WAMessage): void {
      const list = this.readJidData(jid)
      list.push(msg)

      if (list.length > this.max) {
         list.splice(0, list.length - this.max)
      }

      this.writeJidData(jid, list)
   }

   public getAllMessages(jid: string, offset: number = 0): WAMessage[] & { count(): number; clear(): void } {
      const list = this.readJidData(jid)
      const sliced = (offset > 0 ? list.slice(offset) : list) as WAMessage[] & { count(): number; clear(): void }

      const self = this

      sliced.count = () => {
         const currentList = self.readJidData(jid)
         return Math.max(0, currentList.length - offset)
      }

      sliced.clear = () => {
         self.pendingJidWrites.delete(jid)
         self.cache.delete(jid)

         if (offset === 0) {
            const filePath = self.getFilePath(jid)
            try {
               fs.unlinkSync(filePath)
            } catch (error: any) {
               if (error.code !== 'ENOENT') {
                  console.error(`[store-json] Failed to delete JSON file for JID ${jid}:`, error)
               }
            }
         } else {
            const currentList = self.readJidData(jid)
            if (offset < currentList.length) {
               const updated = currentList.slice(0, offset)
               self.writeJidData(jid, updated)
            }
         }
      }

      return sliced
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
         this.contactsCache.clear()
         if (offset === 0) {
            try {
               if (fs.existsSync(this.contactsFilePath)) {
                  fs.unlinkSync(this.contactsFilePath)
               }
            } catch { }
            if (this.fallbackContacts) {
               this.fallbackContacts = Object.create(null)
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
      if (jid) {
         const list = this.readJidData(jid)
         const id = msg.key?.id || msg.id
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
            this.writeJidData(jid, list)
         }
      }
   }

   public updateMessageWithReaction(msg: any, reaction: any): void {
      if (!msg) return
      const authorID = getKeyAuthor(reaction.key)
      msg.reactions = (msg.reactions || []).filter((r: any) => getKeyAuthor(r.key) !== authorID)
      if (reaction.text) msg.reactions.push(reaction)

      const jid = msg.key?.remoteJid
      if (jid) {
         const list = this.readJidData(jid)
         const id = msg.key?.id || msg.id
         const idx = list.findIndex(v => v.key?.id === id || (v as any).id === id)
         if (idx !== -1) {
            list[idx] = msg
            this.writeJidData(jid, list)
         }
      }
   }

   public async loadStories(jid: string, count?: number): Promise<any[] | null> {
      const list = this.storiesCache.get(jid)
      if (!list || list.length === 0) return null
      const slice = count && count > 0 ? list.slice(-count) : list
      return [...slice].reverse()
   }

   public async loadStory(jid: string, id: string): Promise<any | null> {
      const list = this.storiesCache.get(jid)
      if (!list || list.length === 0) return null
      return list.find((v: any) => v.key?.id === id || v.id === id) || null
   }

   public async addStory(jid: string, story: any): Promise<void> {
      const storyId = story.key?.id || story.id
      if (!storyId) return

      let list = this.storiesCache.get(jid)
      if (!list) {
         list = []
         this.storiesCache.set(jid, list)
      }

      const idx = list.findIndex((s: any) => (s.key?.id || s.id) === storyId)
      if (idx !== -1) {
         list[idx] = story
      } else {
         list.push(story)
      }

      if (list.length > this.max) {
         list.splice(0, list.length - this.max)
      }

      this.writeStoriesData()
   }

   public async getAllStories(jid: string, offset: number = 0) {
      const list = this.storiesCache.get(jid) || []
      const sliced = (offset > 0 ? list.slice(offset) : list) as any[] & { count(): Promise<number>; clear(): Promise<void> }

      sliced.count = async () => {
         const currentList = this.storiesCache.get(jid) || []
         return Math.max(0, currentList.length - offset)
      }

      sliced.clear = async () => {
         this.storiesCache.delete(jid)
         this.writeStoriesData()
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
      const now = Date.now()
      this.messageId.forEach((instanceMap, instance) => {
         instanceMap.forEach((value, msgId) => {
            if (now - value.at > 900000) instanceMap.delete(msgId)
         })
         if (instanceMap.size === 0) this.messageId.delete(instance)
      })

      let storiesUpdated = false
      const twentyFourHoursAgo = now - 86400000

      for (const [jid, list] of this.storiesCache.entries()) {
         const filtered = list.filter((story: any) => (story.created_at || story.messageTimestamp || now) > twentyFourHoursAgo)
         if (filtered.length !== list.length) {
            if (filtered.length === 0) {
               this.storiesCache.delete(jid)
            } else {
               this.storiesCache.set(jid, filtered)
            }
            storiesUpdated = true
         }
      }

      if (storiesUpdated) {
         this.writeStoriesData()
      }
   }
}

const store = new Store('stores')

export default store