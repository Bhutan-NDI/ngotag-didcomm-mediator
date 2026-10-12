import type { AgentContext } from '@credo-ts/core'
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MessageRecord } from '../storage/MessageRecord.js'
import { MessageRepository } from '../storage/MessageRepository.js'
import { StorageServiceMessageQueue } from '../storage/StorageMessageQueue.js'

const connectionId = 'connection-1'

const encryptedMessage = (n: number) =>
  ({ protected: `p${n}`, iv: 'iv', ciphertext: 'c', tag: 't' }) as DidCommEncryptedMessage

// Minimal stand-in for MessageRepository backed by a map. Reads yield to the event loop and return fresh copies,
// like a real storage round-trip, so concurrent takes can interleave the way they do in production.
class FakeMessageRepository {
  public records = new Map<string, MessageRecord>()

  private copy(record: MessageRecord) {
    const copy = new MessageRecord({
      id: record.id,
      createdAt: record.createdAt,
      connectionId: record.connectionId,
      message: record.message,
    })
    copy.claimedUntil = record.claimedUntil
    return copy
  }

  public async findByConnectionId(_agentContext: AgentContext, connectionId: string) {
    await new Promise((resolve) => setTimeout(resolve, 5))
    return [...this.records.values()]
      .filter((record) => record.connectionId === connectionId)
      .map((record) => this.copy(record))
  }

  public async update(_agentContext: AgentContext, record: MessageRecord) {
    await new Promise((resolve) => setTimeout(resolve, 1))
    this.records.set(record.id, this.copy(record))
  }

  public async save(_agentContext: AgentContext, record: MessageRecord) {
    this.records.set(record.id, this.copy(record))
  }

  public async deleteById(_agentContext: AgentContext, id: string) {
    this.records.delete(id)
  }
}

function createAgentContext(repository: FakeMessageRepository) {
  return {
    contextCorrelationId: 'default',
    config: { logger: { debug: () => {} } },
    resolve: (token: unknown) => {
      if (token === MessageRepository) return repository
      throw new Error(`Unexpected resolve of ${String(token)}`)
    },
  } as unknown as AgentContext
}

function seed(repository: FakeMessageRepository, count: number) {
  for (let i = 0; i < count; i++) {
    const record = new MessageRecord({
      id: `message-${i}`,
      createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, i)),
      connectionId,
      message: encryptedMessage(i),
    })
    repository.records.set(record.id, record)
  }
}

describe('StorageServiceMessageQueue', () => {
  let repository: FakeMessageRepository
  let agentContext: AgentContext

  beforeEach(() => {
    repository = new FakeMessageRepository()
    agentContext = createAgentContext(repository)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('does not return the same messages to overlapping pickup v2 delivery-requests', async () => {
    seed(repository, 3)
    const queue = new StorageServiceMessageQueue()

    // Pickup v2 takes without deleteMessages; deletion only happens on the messages-received ack.
    const [first, second] = await Promise.all([
      queue.takeFromQueue(agentContext, { connectionId, limit: 10 }),
      queue.takeFromQueue(agentContext, { connectionId, limit: 10 }),
    ])

    expect(first.map((m) => m.id)).toEqual(['message-0', 'message-1', 'message-2'])
    expect(second).toEqual([])
  })

  it('splits the queue between overlapping requests that each take a limited batch', async () => {
    seed(repository, 4)
    const queue = new StorageServiceMessageQueue()

    const [first, second] = await Promise.all([
      queue.takeFromQueue(agentContext, { connectionId, limit: 2 }),
      queue.takeFromQueue(agentContext, { connectionId, limit: 2 }),
    ])

    expect(first.map((m) => m.id)).toEqual(['message-0', 'message-1'])
    expect(second.map((m) => m.id)).toEqual(['message-2', 'message-3'])
  })

  it('excludes claimed messages from the available count', async () => {
    seed(repository, 3)
    const queue = new StorageServiceMessageQueue()

    await queue.takeFromQueue(agentContext, { connectionId, limit: 2 })

    expect(await queue.getAvailableMessageCount(agentContext, { connectionId })).toBe(1)
  })

  it('makes unacknowledged messages available again once the claim lapses', async () => {
    seed(repository, 2)
    const queue = new StorageServiceMessageQueue({ claimTtlMs: 30_000 })

    const now = Date.now()
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const first = await queue.takeFromQueue(agentContext, { connectionId })
    expect(first).toHaveLength(2)
    expect(await queue.takeFromQueue(agentContext, { connectionId })).toEqual([])

    vi.spyOn(Date, 'now').mockReturnValue(now + 30_001)
    const retried = await queue.takeFromQueue(agentContext, { connectionId })
    expect(retried.map((m) => m.id)).toEqual(['message-0', 'message-1'])
  })

  it('removes acknowledged messages so they are never redelivered', async () => {
    seed(repository, 2)
    const queue = new StorageServiceMessageQueue()

    const taken = await queue.takeFromQueue(agentContext, { connectionId })
    await queue.removeMessages(agentContext, { connectionId, messageIds: taken.map((m) => m.id) })

    expect(repository.records.size).toBe(0)
    expect(await queue.getAvailableMessageCount(agentContext, { connectionId })).toBe(0)
  })

  it('deletes before returning when deleteMessages is set (pickup v1)', async () => {
    seed(repository, 2)
    const queue = new StorageServiceMessageQueue()

    const taken = await queue.takeFromQueue(agentContext, { connectionId, deleteMessages: true })

    expect(taken).toHaveLength(2)
    expect(repository.records.size).toBe(0)
  })

  it('keeps serving later takes after one fails', async () => {
    seed(repository, 1)
    const queue = new StorageServiceMessageQueue()
    vi.spyOn(repository, 'findByConnectionId').mockRejectedValueOnce(new Error('storage down'))

    const [failed, succeeded] = await Promise.allSettled([
      queue.takeFromQueue(agentContext, { connectionId }),
      queue.takeFromQueue(agentContext, { connectionId }),
    ])

    expect(failed.status).toBe('rejected')
    expect(succeeded.status === 'fulfilled' && succeeded.value.map((m) => m.id)).toEqual(['message-0'])
  })
})
