export interface WAMessage {
   id?: string
   key?: {
      id?: string | null
      [key: string]: any
   }
   [key: string]: any
}

export interface StoreConfig {
   dir?: string
   max?: number
   uri?: string
}

export interface BotClient {
   loadMessage?: (jid: string, id: string) => WAMessage | null
   loadMessages?: (jid: string, count?: number) => WAMessage[] | null
   addMessage?: (jid: string, msg: WAMessage) => void
   getAllMessages?: (jid: string, offset?: number) => WAMessage[] & { count(): number; clear(): void }
   [key: string]: any
}