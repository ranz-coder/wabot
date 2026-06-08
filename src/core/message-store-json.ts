import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import fs from 'node:fs'
import path from 'node:path'

class MessageStore {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public messages: Record<string, WAMessage[]>

   private _messages: Record<string, WAMessage[]>
   private loadedJids: Set<string>
   private maxCachedJids: number
   private dirtyJids: Set<string>
   private isSaving: boolean

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max
      this.maxCachedJids = 50

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      this._messages = Object.create(null) as Record<string, WAMessage[]>
      this.loadedJids = new Set<string>()
      this.dirtyJids = new Set<string>()
      this.isSaving = false

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

      setInterval(() => this.checkAndSave(), 15000)
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
      client.messages = this.messages

      return client
   }

   private getFilePath(jid: string): string {
      const safeJid = jid.replace(/[^a-zA-Z0-9.-]/g, '_')
      return path.join(this.storeDir, `${safeJid}.json`)
   }

   private loadJidData(jid: string): void {
      if (this._messages[jid]) return

      const filePath = this.getFilePath(jid)
      if (!fs.existsSync(filePath)) {
         this._messages[jid] = []
         return
      }

      try {
         const fileContent = fs.readFileSync(filePath, 'utf-8')
         this._messages[jid] = JSON.parse(fileContent) as WAMessage[]
      } catch (error) {
         console.error(`[message-store-json] Failed to load JID ${jid} from JSON, creating backup:`, error)
         const backupPath = `${filePath}.corrupt-${Date.now()}`
         try { fs.renameSync(filePath, backupPath) } catch { }
         this._messages[jid] = []
      }
   }

   private touchJid(jid: string): void {
      this.loadedJids.delete(jid)
      this.loadedJids.add(jid)

      if (this.loadedJids.size > this.maxCachedJids) {
         for (const oldJid of this.loadedJids) {
            if (this.dirtyJids.has(oldJid)) continue

            delete this._messages[oldJid]
            this.loadedJids.delete(oldJid)
            break
         }
      }
   }

   private async checkAndSave(): Promise<void> {
      if (this.isSaving || this.dirtyJids.size === 0) return

      this.isSaving = true
      const jidsToSave = Array.from(this.dirtyJids)
      this.dirtyJids.clear()

      try {
         const savePromises = jidsToSave.map(async (jid) => {
            const data = this._messages[jid]
            if (!data) return

            const filePath = this.getFilePath(jid)
            const tempFilePath = `${filePath}.tmp`
            const jsonStr = JSON.stringify(data)

            await fs.promises.writeFile(tempFilePath, jsonStr, 'utf-8')
            await fs.promises.rename(tempFilePath, filePath)
         })

         await Promise.all(savePromises)
      } catch (error) {
         console.error('[message-store-json] Error saving messages:', error)
      } finally {
         this.isSaving = false
      }
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      this.loadJidData(jid)
      this.touchJid(jid)
      return this._messages[jid]?.find(v => v.key?.id === id || v.id === id) || null
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

      if (this._messages[jid].length > this.max) {
         this._messages[jid].splice(0, this._messages[jid].length - this.max)
      }

      this.dirtyJids.add(jid)
      this.touchJid(jid)
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store