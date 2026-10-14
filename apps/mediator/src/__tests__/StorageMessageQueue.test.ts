import { type AgentContext, RecordNotFoundError } from '@credo-ts/core'
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { MessageClaimLock } from '../storage/MessageClaimLock.js'
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
    this.assertExists(record.id)
    this.records.set(record.id, this.copy(record))
  }

  public async save(_agentContext: AgentContext, record: MessageRecord) {
    this.records.set(record.id, this.copy(record))
  }

  public async deleteById(_agentContext: AgentContext, id: string) {
    this.assertExists(id)
    this.records.delete(id)
  }

  // Like real storage, writing a record that no longer exists fails.
  private assertExists(id: string) {
    if (!this.records.has(id)) throw new RecordNotFoundError(`record ${id} not found`, { recordType: 'MessageRecord' })
  }
}

// In-memory stand-in for a lock every instance can see (e.g. RedisMessageClaimLock).
class FakeClaimLock implements MessageClaimLock {
  public held = new Map<string, number>()

  public async tryClaim(messageIds: string[], claimedUntil: number) {
    await new Promise((resolve) => setTimeout(resolve, 1))
    const now = Date.now()
    return messageIds.map((id) => {
      const heldUntil = this.held.get(id)
      if (heldUntil !== undefined && heldUntil > now) return false
      this.held.set(id, claimedUntil)
      return true
    })
  }

  public async release(messageIds: string[]) {
    for (const id of messageIds) this.held.delete(id)
  }
}

function createAgentContext(repository: FakeMessageRepository) {
  return {
    contextCorrelationId: 'default',
    config: { logger: { debug: () => {}, warn: () => {} } },
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

  it('skips a record acknowledged while the take was in flight instead of failing the take', async () => {
    seed(repository, 2)
    const queue = new StorageServiceMessageQueue()
    const update = repository.update.bind(repository)
    vi.spyOn(repository, 'update').mockImplementation(async (context, record) => {
      // A late messages-received deletes message-0 between the take's read and its claim write.
      if (record.id === 'message-0') repository.records.delete('message-0')
      return update(context, record)
    })

    const taken = await queue.takeFromQueue(agentContext, { connectionId })

    expect(taken.map((m) => m.id)).toEqual(['message-1'])
    expect(repository.records.get('message-1')?.claimedUntil).toBeGreaterThan(Date.now())
  })

  it('does not return the same messages to overlapping takes on two instances sharing a claim lock', async () => {
    seed(repository, 4)
    const claimLock = new FakeClaimLock()
    // Separate queues stand in for separate mediator instances: they share storage and the lock, not memory.
    const instanceA = new StorageServiceMessageQueue({ claimLock })
    const instanceB = new StorageServiceMessageQueue({ claimLock })

    const [first, second] = await Promise.all([
      instanceA.takeFromQueue(agentContext, { connectionId, limit: 10 }),
      instanceB.takeFromQueue(agentContext, { connectionId, limit: 10 }),
    ])

    const firstIds = first.map((m) => m.id)
    const secondIds = second.map((m) => m.id)
    expect(firstIds.filter((id) => secondIds.includes(id))).toEqual([])
    expect([...firstIds, ...secondIds].sort()).toEqual(['message-0', 'message-1', 'message-2', 'message-3'])
  })

  it('fills a limited take past messages another instance holds in the lock', async () => {
    seed(repository, 3)
    const claimLock = new FakeClaimLock()
    await claimLock.tryClaim(['message-0'], Date.now() + 30_000)
    const queue = new StorageServiceMessageQueue({ claimLock })

    const taken = await queue.takeFromQueue(agentContext, { connectionId, limit: 2 })

    expect(taken.map((m) => m.id)).toEqual(['message-1', 'message-2'])
  })

  it('pickup v1 (deleteMessages) does not delete messages another instance holds in the lock', async () => {
    seed(repository, 2)
    const claimLock = new FakeClaimLock()
    await claimLock.tryClaim(['message-0'], Date.now() + 30_000)
    const queue = new StorageServiceMessageQueue({ claimLock })

    const taken = await queue.takeFromQueue(agentContext, { connectionId, deleteMessages: true })

    expect(taken.map((m) => m.id)).toEqual(['message-1'])
    expect([...repository.records.keys()]).toEqual(['message-0'])
  })

  it('locks each message until the same instant as its record claim', async () => {
    seed(repository, 1)
    const claimLock = new FakeClaimLock()
    const queue = new StorageServiceMessageQueue({ claimLock })

    await queue.takeFromQueue(agentContext, { connectionId })

    expect(claimLock.held.get('message-0')).toBe(repository.records.get('message-0')?.claimedUntil)
  })

  it('releases the lock and record claims it won when a take fails', async () => {
    seed(repository, 2)
    const claimLock = new FakeClaimLock()
    const queue = new StorageServiceMessageQueue({ claimLock })
    const update = repository.update.bind(repository)
    vi.spyOn(repository, 'update').mockImplementation(async (context, record) => {
      if (record.id === 'message-1' && record.claimedUntil !== undefined) throw new Error('storage write failed')
      return update(context, record)
    })

    await expect(queue.takeFromQueue(agentContext, { connectionId })).rejects.toThrow('storage write failed')

    expect(claimLock.held.size).toBe(0)
    expect(repository.records.get('message-0')?.claimedUntil).toBeUndefined()
    vi.restoreAllMocks()
    const retried = await new StorageServiceMessageQueue({ claimLock }).takeFromQueue(agentContext, { connectionId })
    expect(retried.map((m) => m.id)).toEqual(['message-0', 'message-1'])
  })

  it('rejects a claim TTL that is not a positive integer', () => {
    expect(() => new StorageServiceMessageQueue({ claimTtlMs: 0 })).toThrow(/claimTtlMs must be a positive integer/)
  })
})
