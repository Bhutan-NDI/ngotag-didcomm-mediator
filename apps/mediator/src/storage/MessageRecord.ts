import { BaseRecord, utils } from '@credo-ts/core'
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'

export type DefaultMessageRecordTags = {
  connectionId: string
}

export interface MessageRecordStorageProps {
  id?: string
  createdAt?: Date
  connectionId: string
  message: DidCommEncryptedMessage
}

export class MessageRecord extends BaseRecord<DefaultMessageRecordTags> implements MessageRecordStorageProps {
  public sentTime!: string
  public connectionId!: string
  public message!: DidCommEncryptedMessage

  /**
   * Epoch milliseconds until which this message is claimed by an in-flight delivery. While claimed it is not
   * returned by `takeFromQueue`; if the delivery is never acknowledged the claim lapses and the message becomes
   * available again.
   */
  public claimedUntil?: number

  public static override readonly type = 'MessageRecord'
  public override readonly type = MessageRecord.type

  public constructor(props: MessageRecordStorageProps) {
    super()

    if (props) {
      this.id = props.id ?? utils.uuid()
      this.createdAt = props.createdAt ?? new Date()
      this.connectionId = props.connectionId
      this.message = props.message
    }
  }

  public isClaimed(now: number = Date.now()) {
    return this.claimedUntil !== undefined && this.claimedUntil > now
  }

  public getTags() {
    return {
      ...this._tags,
      connectionId: this.connectionId,
    }
  }
}
