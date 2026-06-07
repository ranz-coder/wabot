import { BotClient, WAMessage, StoreConfig } from '../interface.js'
import fs from 'node:fs'
import path from 'node:path'

class MessageStore {
   public client: BotClient | null
   public storeDir: string
   public max: number
   public messages: Record<string, WAMessage[]>
   private dirtyJids: Set<string>
   private isSaving: boolean

   constructor(dir: string = 'messages', max: number = 250) {
      this.client = null
      this.storeDir = path.join(process.cwd(), '.cache', dir)
      this.max = max

      if (!fs.existsSync(this.storeDir)) {
         fs.mkdirSync(this.storeDir, { recursive: true })
      }

      this.messages = Object.create(null) as Record<string, WAMessage[]>
      this.dirtyJids = new Set<string>()
      this.isSaving = false

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
      if (this.messages[jid]) return

      const filePath = this.getFilePath(jid)
      if (!fs.existsSync(filePath)) {
         this.messages[jid] = []
         return
      }

      try {
         const fileContent = fs.readFileSync(filePath, 'utf-8')
         this.messages[jid] = JSON.parse(fileContent) as WAMessage[]
      } catch (error) {
         const backupPath = `${filePath}.corrupt-${Date.now()}`
         try { fs.renameSync(filePath, backupPath) } catch { }
         this.messages[jid] = []
      }
   }

   private async checkAndSave(): Promise<void> {
      if (this.isSaving || this.dirtyJids.size === 0) return

      this.isSaving = true
      const jidsToSave = Array.from(this.dirtyJids)
      this.dirtyJids.clear()

      try {
         const savePromises = jidsToSave.map(async (jid) => {
            const data = this.messages[jid]
            if (!data) return

            const filePath = this.getFilePath(jid)
            const tempFilePath = `${filePath}.tmp`
            const jsonStr = JSON.stringify(data)

            await fs.promises.writeFile(tempFilePath, jsonStr, 'utf-8')
            await fs.promises.rename(tempFilePath, filePath)
         })

         await Promise.all(savePromises)
      } catch (error) {
         console.error('Error saving messages:', error)
      } finally {
         this.isSaving = false
      }
   }

   public loadMessage(jid: string, id: string): WAMessage | null {
      this.loadJidData(jid)
      return this.messages[jid]?.find(v => v.key?.id === id || v.id === id) || null
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

      if (this.messages[jid].length > this.max) {
         this.messages[jid].splice(0, this.messages[jid].length - this.max)
      }

      this.dirtyJids.add(jid)
   }
}

const store = new MessageStore('messages')

export const messages = store.messages
export default store