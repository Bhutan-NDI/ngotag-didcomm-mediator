import { DynamoDBClient, TransactWriteItemsCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb'
import { ConsoleLogger, LogLevel } from '@credo-ts/core'
import type { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import { expect, suite, test, vi } from 'vitest'
import { DynamoDbClientRepository } from '../src/client.js'
import {
  CONNECTION_ID_ALLOWANCE_BYTES,
  DYNAMODB_MAX_ITEM_BYTES,
  QueuedMessageTooLargeError,
  queuedMessageItemBytes,
} from '../src/itemSize.js'
import { DidCommTransportQueueDynamoDb } from '../src/TransportQueueDynamoDb.js'

const connectionId = '4ffdd113-117b-4827-9af5-28aa73ec4bad'
const recipientDid = 'did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH'
const clientOptions = {
  logger: new ConsoleLogger(LogLevel.off),
  region: 'local',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
}

const message = (ciphertextBytes: number): DidCommEncryptedMessage => ({
  protected: 'p'.repeat(300),
  iv: 'i'.repeat(16),
  ciphertext: 'c'.repeat(ciphertextBytes),
  tag: 't'.repeat(22),
})

// The fixed part of the item around a message of `message(n)`, counted by hand from the AWS rules
// (names and strings by their UTF-8 bytes, a number 21 bytes at most, a list or map 3 bytes plus 1 per element):
//   connectionId 12 + 36 | messageId 9 + 21 | receivedAt 10 + 21
//   recipientDids 13 + (3 + 1 + 56)
//   encryptedMessage 16 + 3 + (1 + 9 + 300) + (1 + 2 + 16) + (1 + 10 + n) + (1 + 3 + 22)
const FIXED_BYTES = 12 + 36 + 9 + 21 + 10 + 21 + 13 + 3 + 1 + 56 + 16 + 3 + 310 + 19 + 11 + 26
// The largest ciphertext whose item is exactly at the limit.
const LARGEST_CIPHERTEXT = DYNAMODB_MAX_ITEM_BYTES - FIXED_BYTES

suite('the size of a queued message item', () => {
  test('is the names and values of every attribute, by the AWS rules', () => {
    expect(recipientDid).toHaveLength(56)
    expect(
      queuedMessageItemBytes({ connectionId, recipientDids: [recipientDid], encryptedMessage: message(1000) })
    ).toBe(FIXED_BYTES + 1000)
  })

  test('counts strings in UTF-8 bytes, not characters', () => {
    const ascii = queuedMessageItemBytes({ connectionId, recipientDids: ['aaaa'], encryptedMessage: message(10) })
    // U+20AC is one character and three bytes.
    const euro = queuedMessageItemBytes({ connectionId, recipientDids: ['aaa€'], encryptedMessage: message(10) })
    expect(euro - ascii).toBe(2)
  })

  test('counts each recipient DID and the telemetry carrier when there is one', () => {
    const one = queuedMessageItemBytes({ connectionId, recipientDids: [recipientDid], encryptedMessage: message(10) })
    const two = queuedMessageItemBytes({
      connectionId,
      recipientDids: [recipientDid, recipientDid],
      encryptedMessage: message(10),
    })
    expect(two - one).toBe(1 + 56)

    const emptyCarrier = queuedMessageItemBytes({
      connectionId,
      recipientDids: [recipientDid],
      encryptedMessage: message(10),
      telemetry: {},
    })
    // The name `telemetry` and an empty map.
    expect(emptyCarrier - one).toBe(9 + 3)
    const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
    const carrier = queuedMessageItemBytes({
      connectionId,
      recipientDids: [recipientDid],
      encryptedMessage: message(10),
      telemetry: { traceparent },
    })
    expect(carrier - emptyCarrier).toBe(1 + 'traceparent'.length + traceparent.length)
  })

  test('counts an allowance for a connection id that is not known yet, never less than a UUID', () => {
    const known = queuedMessageItemBytes({ connectionId, recipientDids: [recipientDid], encryptedMessage: message(10) })
    const unknown = queuedMessageItemBytes({ recipientDids: [recipientDid], encryptedMessage: message(10) })
    expect(unknown - known).toBe(CONNECTION_ID_ALLOWANCE_BYTES - connectionId.length)
    expect(CONNECTION_ID_ALLOWANCE_BYTES).toBeGreaterThanOrEqual(36)
  })

  test('the limit is 400 KB', () => {
    expect(DYNAMODB_MAX_ITEM_BYTES).toBe(409_600)
  })
})

suite('a message too large for a DynamoDB item', () => {
  const initialized = async () => {
    const send = vi
      .spyOn(DynamoDBClient.prototype, 'send')
      .mockImplementation(async () => ({ Table: { TableStatus: 'ACTIVE' } }) as never)
    const client = await DynamoDbClientRepository.initialize(clientOptions)
    send.mockClear()
    return { client, send }
  }

  test('is refused by addMessage before anything is sent to DynamoDB', async () => {
    const { client, send } = await initialized()
    try {
      const error = await client
        .addMessage({ connectionId, recipientDids: [recipientDid], encryptedMessage: message(LARGEST_CIPHERTEXT + 1) })
        .catch((e: unknown) => e)
      expect(error).toBeInstanceOf(QueuedMessageTooLargeError)
      expect((error as QueuedMessageTooLargeError).itemBytes).toBe(DYNAMODB_MAX_ITEM_BYTES + 1)
      expect((error as QueuedMessageTooLargeError).maxItemBytes).toBe(DYNAMODB_MAX_ITEM_BYTES)
      // Sizes only: the message is as large as it is refused for.
      expect((error as Error).message).not.toContain('cccc')
      expect(send).not.toHaveBeenCalled()
    } finally {
      send.mockRestore()
    }
  })

  test('a message exactly at the limit is written', async () => {
    const { client, send } = await initialized()
    try {
      await client.addMessage({
        connectionId,
        recipientDids: [recipientDid],
        encryptedMessage: message(LARGEST_CIPHERTEXT),
      })
      const writes = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command) => command instanceof TransactWriteItemsCommand || command instanceof UpdateItemCommand)
      expect(writes).toHaveLength(1)
    } finally {
      send.mockRestore()
    }
  })

  test('the telemetry carrier stored beside the message counts', async () => {
    const { client, send } = await initialized()
    try {
      const error = await client
        .addMessage({
          connectionId,
          recipientDids: [recipientDid],
          encryptedMessage: message(LARGEST_CIPHERTEXT),
          telemetry: {},
        })
        .catch((e: unknown) => e)
      expect(error).toBeInstanceOf(QueuedMessageTooLargeError)
      expect(send).not.toHaveBeenCalled()
    } finally {
      send.mockRestore()
    }
  })

  test('assertMessageFits gives the same answer without reading or writing, with an allowance for the connection id', async () => {
    const send = vi
      .spyOn(DynamoDBClient.prototype, 'send')
      .mockImplementation(async () => ({ Table: { TableStatus: 'ACTIVE' } }) as never)
    try {
      const queue = await DidCommTransportQueueDynamoDb.initialize(clientOptions)
      send.mockClear()
      const allowance = CONNECTION_ID_ALLOWANCE_BYTES - connectionId.length

      expect(() =>
        queue.assertMessageFits({ recipientDids: [recipientDid], payload: message(LARGEST_CIPHERTEXT - allowance) })
      ).not.toThrow()
      expect(() =>
        queue.assertMessageFits({ recipientDids: [recipientDid], payload: message(LARGEST_CIPHERTEXT - allowance + 1) })
      ).toThrow(QueuedMessageTooLargeError)
      // With the connection id it is the check of the write itself.
      expect(() =>
        queue.assertMessageFits({ connectionId, recipientDids: [recipientDid], payload: message(LARGEST_CIPHERTEXT) })
      ).not.toThrow()
      expect(() =>
        queue.assertMessageFits({
          connectionId,
          recipientDids: [recipientDid],
          payload: message(LARGEST_CIPHERTEXT + 1),
        })
      ).toThrow(QueuedMessageTooLargeError)
      expect(send).not.toHaveBeenCalled()
    } finally {
      send.mockRestore()
    }
  })
})
