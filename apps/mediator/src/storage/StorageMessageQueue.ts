import { AgentContext, EventEmitter, RecordNotFoundError, utils } from '@credo-ts/core'
import type {
  AddMessageOptions,
  DidCommQueueTransportRepository,
  GetAvailableMessageCountOptions,
  QueuedDidCommMessage,
  RemoveMessagesOptions,
  TakeFromQueueOptions,
} from '@credo-ts/didcomm'
import { assertValidClaimTtl, DEFAULT_MESSAGE_CLAIM_TTL_MS } from '@credo-ts/didcomm-transport-queue-dynamodb'
import { DidcommMessageQueuedEvent, MediatorEventTypes } from '../events.js'
import type { TelemetryCarrier } from '../telemetry/api.js'
import type { MessageClaimLock } from './MessageClaimLock.js'
import { MessageRecord } from './MessageRecord.js'
import { MessageRepository } from './MessageRepository.js'

export interface StorageServiceMessageQueueOptions {
  claimTtlMs?: number
  /** Cross-instance claim arbiter. Without it, claims only hold within this instance. */
  claimLock?: MessageClaimLock
}

export class StorageServiceMessageQueue implements DidCommQueueTransportRepository {
  private readonly claimTtlMs: number
  private readonly claimLock?: MessageClaimLock

  // Tail of the in-flight take per connection, so this instance's read-then-claim never interleaves.
  private readonly connectionTakeQueues = new Map<string, Promise<void>>()

  public constructor(options: StorageServiceMessageQueueOptions = {}) {
    this.claimTtlMs = options.claimTtlMs ?? DEFAULT_MESSAGE_CLAIM_TTL_MS
    assertValidClaimTtl(this.claimTtlMs)
    this.claimLock = options.claimLock
  }

  // Aggregate queue-depth stats for OpenTelemetry observable gauges.
  public async getQueueStats(agentContext: AgentContext) {
    const messageRepository = agentContext.resolve(MessageRepository)
    return messageRepository.getQueueStats(agentContext)
  }

  public async getAvailableMessageCount(agentContext: AgentContext, options: GetAvailableMessageCountOptions) {
    const { connectionId } = options

    const messageRepository = agentContext.resolve(MessageRepository)
    const messageRecords = await messageRepository.findByConnectionId(agentContext, connectionId)

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

    // Both pickup versions skip messages another delivery holds.
    const now = Date.now()
    const availableRecords = messageRecords.filter((messageRecord) => !messageRecord.isClaimed(now))

    const messagesToTake = limit ?? availableRecords.length
    agentContext.config.logger.debug(
      `Taking ${messagesToTake} messages from queue for connection ${connectionId} (of ${
        availableRecords.length
      } available, ${messageRecords.length} total) with deleteMessages=${String(deleteMessages)}`
    )

    const wonRecords = await this.lockRecords(availableRecords, messagesToTake)

    let messageRecordsToReturn: MessageRecord[]
    if (deleteMessages) {
      messageRecordsToReturn = await this.keepExisting(wonRecords, (messageRecord) =>
        messageRepository.deleteById(agentContext, messageRecord.id)
      )
    } else {
      const claimedUntil = now + this.claimTtlMs
      messageRecordsToReturn = await this.keepExisting(wonRecords, (messageRecord) => {
        messageRecord.claimedUntil = claimedUntil
        return messageRepository.update(agentContext, messageRecord)
      })
    }

    const queuedMessages = messageRecordsToReturn.map((messageRecord) => ({
      id: messageRecord.id,
      receivedAt: messageRecord.createdAt,
      encryptedMessage: messageRecord.message,
      telemetry: messageRecord.telemetry,
    }))

    return queuedMessages
  }

  /** Takes up to count records, in queue order, that the claim lock grants to this instance. */
  private async lockRecords(records: MessageRecord[], count: number): Promise<MessageRecord[]> {
    if (!this.claimLock) return records.slice(0, count)

    const won: MessageRecord[] = []
    for (let start = 0; start < records.length && won.length < count; ) {
      const batch = records.slice(start, start + count - won.length)
      start += batch.length
      const results = await this.claimLock.tryClaim(
        batch.map((record) => record.id),
        this.claimTtlMs
      )
      won.push(...batch.filter((_, index) => results[index]))
    }
    return won
  }

  /** Applies write to each record, dropping records acknowledged (deleted) since they were read. */
  private async keepExisting(
    records: MessageRecord[],
    write: (record: MessageRecord) => Promise<unknown>
  ): Promise<MessageRecord[]> {
    const results = await Promise.allSettled(records.map(write))
    const failure = results.find(
      (result) => result.status === 'rejected' && !(result.reason instanceof RecordNotFoundError)
    )
    if (failure?.status === 'rejected') throw failure.reason
    return records.filter((_, index) => results[index].status === 'fulfilled')
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
    const { connectionId, payload, telemetry } = options as AddMessageOptions & { telemetry?: TelemetryCarrier }

    agentContext.config.logger.debug('Adding message to pickup queue')

    const messageRepository = agentContext.resolve(MessageRepository)

    const id = utils.uuid()
    await messageRepository.save(
      agentContext,
      new MessageRecord({
        id,
        connectionId,
        message: payload,
        telemetry,
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
