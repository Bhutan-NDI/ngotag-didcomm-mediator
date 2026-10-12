import { AgentContext, EventEmitter, utils } from '@credo-ts/core'
import type {
  AddMessageOptions,
  DidCommQueueTransportRepository,
  GetAvailableMessageCountOptions,
  QueuedDidCommMessage,
  RemoveMessagesOptions,
  TakeFromQueueOptions,
} from '@credo-ts/didcomm'
import { DidcommMessageQueuedEvent, MediatorEventTypes } from '../events.js'
import { MessageRecord } from './MessageRecord.js'
import { MessageRepository } from './MessageRepository.js'

/**
 * How long a message taken from the queue stays claimed before it becomes available again, if the delivery is never
 * acknowledged (e.g. the holder went offline before sending messages-received).
 */
export const DEFAULT_MESSAGE_CLAIM_TTL_MS = 30_000

export interface StorageServiceMessageQueueOptions {
  claimTtlMs?: number
}

export class StorageServiceMessageQueue implements DidCommQueueTransportRepository {
  private readonly claimTtlMs: number

  // Tail of the in-flight takeFromQueue chain per connection, so the read-then-claim below is not interleaved
  // with another take for the same connection on this instance.
  private readonly connectionTakeQueues = new Map<string, Promise<void>>()

  public constructor(options: StorageServiceMessageQueueOptions = {}) {
    this.claimTtlMs = options.claimTtlMs ?? DEFAULT_MESSAGE_CLAIM_TTL_MS
  }

  // Aggregate queue-depth stats for the gauge snapshot (debug instrumentation).
  public async getQueueStats(agentContext: AgentContext) {
    const messageRepository = agentContext.resolve(MessageRepository)
    return messageRepository.getQueueStats(agentContext)
  }

  public async getAvailableMessageCount(agentContext: AgentContext, options: GetAvailableMessageCountOptions) {
    const { connectionId } = options

    const messageRepository = agentContext.resolve(MessageRepository)
    const messageRecords = await messageRepository.findByConnectionId(agentContext, connectionId)

    // Messages claimed by an in-flight delivery are not available, matching Credo's in-memory queue which only
    // counts 'pending' messages.
    const now = Date.now()
    const availableCount = messageRecords.filter((messageRecord) => !messageRecord.isClaimed(now)).length

    agentContext.config.logger.debug(
      `Found ${availableCount} available messages for connection ${connectionId} (of total ${messageRecords.length})`
    )

    return availableCount
  }

  public async takeFromQueue(
    agentContext: AgentContext,
    options: TakeFromQueueOptions
  ): Promise<QueuedDidCommMessage[]> {
    const { connectionId } = options

    return this.serializePerConnection(`${agentContext.contextCorrelationId}:${connectionId}`, () =>
      this.claimFromQueue(agentContext, options)
    )
  }

  private async claimFromQueue(
    agentContext: AgentContext,
    options: TakeFromQueueOptions
  ): Promise<QueuedDidCommMessage[]> {
    const { connectionId, limit, deleteMessages } = options

    const messageRepository = agentContext.resolve(MessageRepository)
    const messageRecords = await messageRepository.findByConnectionId(agentContext, connectionId)

    // Skip messages another delivery has taken but not yet had acknowledged. Without this, two pickup v2
    // delivery-requests (or a delivery-request and a live-mode drain) arriving before the holder's
    // messages-received would both return the same messages, since v2 only deletes on acknowledgement.
    const now = Date.now()
    const availableRecords = messageRecords.filter((messageRecord) => !messageRecord.isClaimed(now))

    const messagesToTake = limit ?? availableRecords.length
    agentContext.config.logger.debug(
      `Taking ${messagesToTake} messages from queue for connection ${connectionId} (of ${
        availableRecords.length
      } available, ${messageRecords.length} total) with deleteMessages=${String(deleteMessages)}`
    )

    const messageRecordsToReturn = availableRecords.slice(0, messagesToTake)

    if (deleteMessages) {
      await this.removeMessages(agentContext, {
        connectionId,
        messageIds: messageRecordsToReturn.map((msg) => msg.id),
      })
    } else {
      const claimedUntil = now + this.claimTtlMs
      await Promise.all(
        messageRecordsToReturn.map((messageRecord) => {
          messageRecord.claimedUntil = claimedUntil
          return messageRepository.update(agentContext, messageRecord)
        })
      )
    }

    const queuedMessages = messageRecordsToReturn.map((messageRecord) => ({
      id: messageRecord.id,
      receivedAt: messageRecord.createdAt,
      encryptedMessage: messageRecord.message,
    }))

    return queuedMessages
  }

  private async serializePerConnection<T>(key: string, task: () => Promise<T>): Promise<T> {
    const previous = this.connectionTakeQueues.get(key) ?? Promise.resolve()
    const result = previous.then(task)
    const tail = result.then(
      () => undefined,
      () => undefined
    )
    this.connectionTakeQueues.set(key, tail)

    try {
      return await result
    } finally {
      // Drop the entry once nothing is queued behind this take, so the map does not grow per connection.
      if (this.connectionTakeQueues.get(key) === tail) {
        this.connectionTakeQueues.delete(key)
      }
    }
  }

  public async addMessage(agentContext: AgentContext, options: AddMessageOptions) {
    const { connectionId, payload } = options

    agentContext.config.logger.debug(
      `Adding message to queue for connection ${connectionId} with payload ${JSON.stringify(payload)}`
    )

    const messageRepository = agentContext.resolve(MessageRepository)

    const id = utils.uuid()
    await messageRepository.save(
      agentContext,
      new MessageRecord({
        id,
        connectionId,
        message: payload,
      })
    )

    this.emitMessageQueuedEvent(agentContext, connectionId)

    return id
  }

  public async removeMessages(agentContext: AgentContext, options: RemoveMessagesOptions) {
    const { messageIds } = options

    agentContext.config.logger.debug(`Removing message ids ${messageIds}`)
    const messageRepository = agentContext.resolve(MessageRepository)

    const deletePromises = messageIds.map((messageId) => messageRepository.deleteById(agentContext, messageId))

    await Promise.all(deletePromises)
  }

  private emitMessageQueuedEvent(agentContext: AgentContext, connectionId: string) {
    const eventEmitter = agentContext.resolve(EventEmitter)
    eventEmitter.emit<DidcommMessageQueuedEvent>(agentContext, {
      type: MediatorEventTypes.DidCommMessageQueued,
      payload: {
        connectionId,
      },
    })
  }
}
