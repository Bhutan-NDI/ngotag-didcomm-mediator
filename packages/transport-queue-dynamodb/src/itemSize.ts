import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'

/**
 * DynamoDB refuses any item above 400 KB, counted as the UTF-8 bytes of every attribute name plus the
 * size of every attribute value.
 *
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
 */
export const DYNAMODB_MAX_ITEM_BYTES = 400 * 1024

/**
 * A number is stored in up to 21 bytes. Its exact size depends on its significant digits, which AWS only
 * documents as an approximation, so every number is counted at the documented maximum: the result can only
 * be too high, never too low.
 */
const NUMBER_MAX_BYTES = 21
/** A list or a map costs 3 bytes, plus 1 byte for each of its elements. */
const CONTAINER_BYTES = 3
const CONTAINER_ELEMENT_BYTES = 1

/**
 * The connection id is not known when a message is checked on arrival (the mediation record is only read
 * while the message is handled), so that check counts this many bytes for it. Credo's record ids are UUIDs
 * of 36 characters; the write itself is checked again with the real id.
 */
export const CONNECTION_ID_ALLOWANCE_BYTES = 64

const utf8Bytes = (value: string) => Buffer.byteLength(value, 'utf8')

/** The size DynamoDB counts for one attribute value, as the AWS SDK marshalls it. */
function valueBytes(value: unknown): number {
  if (typeof value === 'string') return utf8Bytes(value)
  if (typeof value === 'number' || typeof value === 'bigint') return NUMBER_MAX_BYTES
  if (typeof value === 'boolean' || value === null) return 1
  if (Array.isArray(value)) {
    let bytes = CONTAINER_BYTES
    for (const element of value) bytes += CONTAINER_ELEMENT_BYTES + valueBytes(element)
    return bytes
  }
  if (typeof value === 'object' && value !== null) {
    let bytes = CONTAINER_BYTES
    for (const [name, element] of Object.entries(value)) {
      // `marshall` is called without removeUndefinedValues, so an undefined value never reaches a write.
      if (element === undefined) continue
      bytes += CONTAINER_ELEMENT_BYTES + utf8Bytes(name) + valueBytes(element)
    }
    return bytes
  }
  throw new TypeError(`Cannot size a value of type ${typeof value} for DynamoDB`)
}

const attributeBytes = (name: string, value: unknown) => utf8Bytes(name) + valueBytes(value)

export type QueuedMessageItemParts = {
  /** Left out when it is not known yet: {@link CONNECTION_ID_ALLOWANCE_BYTES} is counted instead. */
  connectionId?: string
  recipientDids: string[]
  encryptedMessage: DidCommEncryptedMessage
  telemetry?: Record<string, string>
}

/**
 * The size of the item `addMessage` writes for a queued message, never lower than what DynamoDB counts:
 * `connectionId`, `messageId`, `encryptedMessage`, `recipientDids`, `receivedAt` and, when given, `telemetry`.
 * The recipient-index entries are separate, small items and are not part of it.
 */
export function queuedMessageItemBytes(parts: QueuedMessageItemParts): number {
  return (
    utf8Bytes('connectionId') +
    (parts.connectionId === undefined ? CONNECTION_ID_ALLOWANCE_BYTES : utf8Bytes(parts.connectionId)) +
    attributeBytes('messageId', 0) +
    attributeBytes('encryptedMessage', parts.encryptedMessage) +
    attributeBytes('recipientDids', parts.recipientDids) +
    attributeBytes('receivedAt', 0) +
    (parts.telemetry === undefined ? 0 : attributeBytes('telemetry', parts.telemetry))
  )
}

/** A message whose queue item would be above DynamoDB's item limit. It is refused before anything is written. */
export class QueuedMessageTooLargeError extends Error {
  public readonly itemBytes: number
  public readonly maxItemBytes: number

  public constructor(itemBytes: number, maxItemBytes: number = DYNAMODB_MAX_ITEM_BYTES) {
    super(
      `The message cannot be queued: its DynamoDB item would be ${itemBytes} bytes, above the limit of ${maxItemBytes} bytes`
    )
    this.name = 'QueuedMessageTooLargeError'
    this.itemBytes = itemBytes
    this.maxItemBytes = maxItemBytes
  }
}

/** Throws {@link QueuedMessageTooLargeError} when the message's queue item would be above DynamoDB's item limit. */
export function assertQueuedMessageFits(parts: QueuedMessageItemParts): void {
  const itemBytes = queuedMessageItemBytes(parts)
  if (itemBytes > DYNAMODB_MAX_ITEM_BYTES) throw new QueuedMessageTooLargeError(itemBytes)
}
