import { randomUUID } from 'node:crypto'
import { ConsoleLogger, LogLevel } from '@credo-ts/core'
import { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import { beforeAll, expect, suite, test } from 'vitest'
import { DynamoDbClientRepository, DynamoDbClientRepositoryOptions } from '../src/client.js'

const recipientDid = 'did:key:claim'
const encryptedMessage: DidCommEncryptedMessage = {
  ciphertext: 'ciphertext',
  iv: 'iv',
  protected: 'protected',
  tag: 'tag',
}

// Each client stands in for a separate mediator instance: claims must hold through DynamoDB, not process memory.
const createClient = (options: Partial<DynamoDbClientRepositoryOptions> = {}) =>
  DynamoDbClientRepository.initialize({
    logger: new ConsoleLogger(LogLevel.off),
    region: 'local',
    credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    tableName: 'claim_test_queued_messages',
    ...options,
  })

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

suite('dynamodb pickup v2 claim on take', () => {
  let instanceA: DynamoDbClientRepository
  let instanceB: DynamoDbClientRepository

  beforeAll(async () => {
    instanceA = await createClient()
    instanceB = await createClient()
  })

  const queueMessages = async (connectionId: string, count: number) => {
    const ids: string[] = []
    for (let index = 0; index < count; index++) {
      ids.push(
        await instanceA.addMessage({
          connectionId,
          // Distinct, increasing timestamps keep the queue order deterministic.
          receivedAt: new Date(Date.now() - (count - index) * 1000),
          encryptedMessage,
          recipientDids: [recipientDid],
        })
      )
    }
    return ids
  }

  const ids = (messages: Array<{ id: string }>) => messages.map((message) => message.id)

  test('overlapping delivery-requests on two instances do not return the same messages', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 5)

    const [first, second] = await Promise.all([
      instanceA.getMessages({ connectionId, limit: 10 }),
      instanceB.getMessages({ connectionId, limit: 10 }),
    ])

    expect(ids(first).filter((id) => ids(second).includes(id))).toEqual([])
    expect([...ids(first), ...ids(second)].sort()).toEqual([...queued].sort())
  })

  test('overlapping limited batches split the queue between them', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 4)

    const [first, second] = await Promise.all([
      instanceA.getMessages({ connectionId, limit: 2 }),
      instanceB.getMessages({ connectionId, limit: 2 }),
    ])

    expect(first).toHaveLength(2)
    expect(second).toHaveLength(2)
    expect([...ids(first), ...ids(second)].sort()).toEqual([...queued].sort())
  })

  test('a later take skips claimed messages at the front of the queue', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 3)

    expect(ids(await instanceA.getMessages({ connectionId, limit: 1 }))).toEqual([queued[0]])
    expect(ids(await instanceB.getMessages({ connectionId, limit: 1 }))).toEqual([queued[1]])
    expect(ids(await instanceA.getMessages({ connectionId, limit: 1 }))).toEqual([queued[2]])
    expect(await instanceB.getMessages({ connectionId, limit: 1 })).toEqual([])
  })

  test('the recipient-scoped path claims the same way', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 4)

    const [first, second] = await Promise.all([
      instanceA.getMessages({ connectionId, recipientDid, limit: 10 }),
      instanceB.getMessages({ connectionId, recipientDid, limit: 10 }),
    ])

    expect(ids(first).filter((id) => ids(second).includes(id))).toEqual([])
    expect([...ids(first), ...ids(second)].sort()).toEqual([...queued].sort())
  })

  test('an unacknowledged claim lapses and the messages are delivered again', async () => {
    const shortLease = await createClient({ claimTtlMs: 300 })
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 2)

    expect(ids(await shortLease.getMessages({ connectionId, limit: 10 }))).toEqual(queued)
    expect(await shortLease.getMessages({ connectionId, limit: 10 })).toEqual([])

    await sleep(400)
    expect(ids(await shortLease.getMessages({ connectionId, limit: 10 }))).toEqual(queued)
  })

  test('acknowledged messages are not delivered again or recreated by a late claim', async () => {
    const shortLease = await createClient({ claimTtlMs: 300 })
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 2)

    await shortLease.getMessages({ connectionId, limit: 10 })
    await shortLease.removeMessages({ connectionId, messageIds: queued })

    await sleep(400)
    expect(await shortLease.getMessages({ connectionId, limit: 10 })).toEqual([])
    expect(await shortLease.getMessageCount(connectionId)).toBe(0)
  })

  test('does not expose the claim on returned messages', async () => {
    const connectionId = randomUUID()
    await queueMessages(connectionId, 1)

    const [message] = await instanceA.getMessages({ connectionId, limit: 1 })
    expect(message).not.toHaveProperty('claimedUntil')
  })

  test('pickup v1 (deleteMessages) still removes the messages it returns', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 2)

    expect(ids(await instanceA.getMessages({ connectionId, limit: 10, deleteMessages: true }))).toEqual(queued)
    expect(await instanceA.getMessageCount(connectionId)).toBe(0)
  })

  test('counts exclude claimed messages, on the connection and recipient paths', async () => {
    const connectionId = randomUUID()
    await queueMessages(connectionId, 3)

    await instanceA.getMessages({ connectionId, limit: 2 })

    expect(await instanceB.getMessageCount(connectionId)).toBe(1)
    expect(await instanceB.getMessageCount(connectionId, recipientDid)).toBe(1)
  })

  test('a recipient-scoped claim is visible to connection-scoped takes and counts', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 2)

    expect(ids(await instanceA.getMessages({ connectionId, recipientDid, limit: 1 }))).toEqual([queued[0]])
    expect(ids(await instanceB.getMessages({ connectionId, limit: 10 }))).toEqual([queued[1]])
    expect(await instanceB.getMessageCount(connectionId, recipientDid)).toBe(0)
  })

  test('pickup v1 (deleteMessages) does not return or delete messages a v2 delivery holds', async () => {
    const connectionId = randomUUID()
    const queued = await queueMessages(connectionId, 3)

    const held = await instanceA.getMessages({ connectionId, limit: 1 })
    const deleted = await instanceB.getMessages({ connectionId, limit: 10, deleteMessages: true })

    expect(ids(held)).toEqual([queued[0]])
    expect(ids(deleted)).toEqual(queued.slice(1))

    // The held message survives until its own acknowledgement.
    await instanceA.removeMessages({ connectionId, messageIds: ids(held) })
    expect(await instanceA.getMessageCount(connectionId)).toBe(0)
  })

  test('rejects a claim TTL that is not a positive integer', async () => {
    await expect(createClient({ claimTtlMs: 0 })).rejects.toThrow(/claimTtlMs must be a positive integer/)
  })
})
