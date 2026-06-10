import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import fs from 'node:fs'
import path from 'node:path'

class MessageStore {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public database: string

   private cache = new Map<string, WAMessage[]>()
   private maxCachedJids = 15
   private writeTimeouts = new Map<string, NodeJS.Timeout>()

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.database = 'json'

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }
   }

   public config({ dir, max }: StoreConfig): this {
      if (dir) {
         this.storeDir = path.join(process.cwd(), '.cache', dir)
         if (!fs.existsSync(this.storeDir)) {
            fs.mkdirSync(this.storeDir, { recursive: true })
         }
      }
      if (max !== undefined) {
         this.max = max
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
            if (this.writeTimeouts.has(key)) continue

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
      if (!fs.existsSync(filePath)) {
         return []
      }

      try {
         const fileContent = fs.readFileSync(filePath, 'utf-8')
         const data = JSON.parse(fileContent) as WAMessage[]

         this.cache.set(jid, data)
         this.evictOldestCache()

         return data
      } catch (error) {
         console.error(`[message-store-json] Failed to load JID ${jid} from JSON:`, error)
         return []
      }
   }

   private writeJidData(jid: string, data: WAMessage[]): void {
      this.cache.set(jid, data)
      this.touchJid(jid)
      this.evictOldestCache()

      if (this.writeTimeouts.has(jid)) {
         clearTimeout(this.writeTimeouts.get(jid)!)
      }

      const timeout = setTimeout(async () => {
         this.writeTimeouts.delete(jid)
         const filePath = this.getFilePath(jid)
         const tempFilePath = `${filePath}.tmp`
         try {
            const jsonStr = JSON.stringify(data)
            await fs.promises.writeFile(tempFilePath, jsonStr, 'utf-8')
            await fs.promises.rename(tempFilePath, filePath)
         } catch (error) {
            console.error(`[message-store-json] Failed to write JID ${jid} to JSON:`, error)
         }
      }, 1000)

      this.writeTimeouts.set(jid, timeout)
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      const list = this.readJidData(jid)
      return list.find(v => v.key?.id === id || (v as any).id === id) || null
   }

   public loadMessages(jid: string, count?: number): WAMessage[] | null {
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
         if (self.writeTimeouts.has(jid)) {
            clearTimeout(self.writeTimeouts.get(jid)!)
            self.writeTimeouts.delete(jid)
         }

         self.cache.delete(jid)

         if (offset === 0) {
            const filePath = self.getFilePath(jid)
            if (fs.existsSync(filePath)) {
               try {
                  fs.unlinkSync(filePath)
               } catch (error) {
                  console.error(`[message-store-json] Failed to delete JSON file for JID ${jid}:`, error)
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
}

const store = new MessageStore('messages')

export default store