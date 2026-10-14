import { BaseRecord, utils } from '@credo-ts/core'
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import type { TelemetryCarrier } from '../telemetry/api.js'
import { isClaimed } from './claim.js'

export type DefaultMessageRecordTags = {
  connectionId: string
}

export interface MessageRecordStorageProps {
  id?: string
  createdAt?: Date
  connectionId: string
  message: DidCommEncryptedMessage
  telemetry?: TelemetryCarrier
}

export class MessageRecord extends BaseRecord<DefaultMessageRecordTags> implements MessageRecordStorageProps {
  public sentTime!: string
  public connectionId!: string
  public message!: DidCommEncryptedMessage
  public telemetry?: TelemetryCarrier

  /** Epoch milliseconds until which an in-flight delivery holds this message. */
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
      this.telemetry = props.telemetry
    }
  }

  public isClaimed(now: number = Date.now()) {
    return isClaimed(this.claimedUntil, now)
  }

  public getTags() {
    return {
      ...this._tags,
      connectionId: this.connectionId,
    }
  }
}
