import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ConsoleLogger, LogLevel } from '@credo-ts/core'
import { expect, suite, test, vi } from 'vitest'
import { DynamoDbClientRepository } from '../src/client.js'

const connectionId = '4ffdd113-117b-4827-9af5-28aa73ec4bad'
const recipientDid = 'did:key:123'
const clientOptions = {
  logger: new ConsoleLogger(LogLevel.off),
  region: 'local',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
}

suite('dynamodb recipient index', () => {
  test('uses a strongly consistent read to establish the migration cutover', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof QueryCommand) return { Items: [{ messageId: { N: '42' } }] } as never
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const getLatestMessageId = client as unknown as {
        getLatestMessageId: (connection: string) => Promise<{ N: string } | undefined>
      }

      expect(await getLatestMessageId.getLatestMessageId(connectionId)).toEqual({ N: '42' })

      const latestMessageQuery = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof QueryCommand)
      const latestMessageQueryInput = latestMessageQuery instanceof QueryCommand ? latestMessageQuery.input : undefined
      expect(latestMessageQueryInput?.ConsistentRead).toBe(true)
    } finally {
      send.mockRestore()
    }
  })

  test('creates the companion recipient index table', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      return {} as never
    })

    try {
      await DynamoDbClientRepository.initialize(clientOptions)
      const createCommands = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command): command is CreateTableCommand => command instanceof CreateTableCommand)

      expect(createCommands.map((command) => command.input.TableName)).toEqual([
        'queued_messages',
        'queued_messages_recipient_index',
      ])
      expect(createCommands[1].input.KeySchema).toEqual([
        { AttributeName: 'connectionId', KeyType: 'HASH' },
        { AttributeName: 'recipientMessageId', KeyType: 'RANGE' },
      ])
    } finally {
      send.mockRestore()
    }
  })

  test('counts recipient messages through a key condition instead of a filter', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) {
        return {
          Item: {
            connectionId: { S: connectionId },
            recipientMessageId: { S: '__recipient_index_metadata__' },
            cutoverMessageId: { N: '0' },
          },
        } as never
      }
      if (command instanceof QueryCommand && command.input.TableName === 'queued_messages_recipient_index') {
        return { Count: 3 } as never
      }
      return { Count: 0 } as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      expect(await client.getMessageCount(connectionId, recipientDid, 10)).toBe(3)

      const recipientQuery = send.mock.calls
        .map(([command]) => command as unknown)
        .find(
          (command) => command instanceof QueryCommand && command.input.TableName === 'queued_messages_recipient_index'
        )
      const recipientQueryInput = recipientQuery instanceof QueryCommand ? recipientQuery.input : undefined

      expect(recipientQueryInput?.KeyConditionExpression).toBe(
        'connectionId = :connectionId AND recipientMessageId BETWEEN :recipientIndexStart AND :recipientIndexEnd'
      )
      expect(recipientQueryInput?.FilterExpression).toBeUndefined()
      expect(recipientQueryInput?.Select).toBe('COUNT')
    } finally {
      send.mockRestore()
    }
  })

  test('retains the legacy filter path until an existing connection is indexed', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) return {} as never
      if (command instanceof QueryCommand) return { Count: 2 } as never
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      expect(await client.getMessageCount(connectionId, recipientDid, 10)).toBe(2)

      const legacyQuery = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof QueryCommand)
      const legacyQueryInput = legacyQuery instanceof QueryCommand ? legacyQuery.input : undefined
      expect(legacyQueryInput?.FilterExpression).toBe('contains(recipientDids, :recipientDid)')
    } finally {
      send.mockRestore()
    }
  })

  test('uses the recipient index to fetch only the matching canonical messages', async () => {
    const messageId = '1234567890123001'
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) {
        return {
          Item: {
            connectionId: { S: connectionId },
            recipientMessageId: { S: '__recipient_index_metadata__' },
            cutoverMessageId: { N: '0' },
          },
        } as never
      }
      if (command instanceof QueryCommand && command.input.TableName === 'queued_messages_recipient_index') {
        return { Items: [{ messageId: { N: messageId } }] } as never
      }
      if (command instanceof QueryCommand) return { Items: [] } as never
      if (command instanceof BatchGetItemCommand) {
        return {
          Responses: {
            queued_messages: [
              {
                connectionId: { S: connectionId },
                messageId: { N: messageId },
                recipientDids: { L: [{ S: recipientDid }] },
                receivedAt: { N: '0' },
                encryptedMessage: {
                  M: {
                    ciphertext: { S: 'ciphertext' },
                    iv: { S: 'iv' },
                    protected: { S: 'protected' },
                    tag: { S: 'tag' },
                  },
                },
              },
            ],
          },
        } as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const messages = await client.getMessages({ connectionId, recipientDid, limit: 1 })

      expect(messages).toHaveLength(1)
      expect(messages[0].id).toBe(messageId)
      const recipientQuery = send.mock.calls
        .map(([command]) => command as unknown)
        .find(
          (command) => command instanceof QueryCommand && command.input.TableName === 'queued_messages_recipient_index'
        )
      const recipientQueryInput = recipientQuery instanceof QueryCommand ? recipientQuery.input : undefined
      expect(recipientQueryInput?.FilterExpression).toBeUndefined()
    } finally {
      send.mockRestore()
    }
  })

  test('retries unprocessed batch writes with backoff', async () => {
    let batchWriteCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchWriteItemCommand) {
        batchWriteCount += 1
        return batchWriteCount === 1 ? { UnprocessedItems: command.input.RequestItems } : ({} as never)
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchWriteItems = client as unknown as {
        batchWriteItems: (requests: Record<string, Array<Record<string, unknown>>>) => Promise<void>
      }

      await expect(
        batchWriteItems.batchWriteItems({
          queued_messages: [{ PutRequest: { Item: { connectionId: { S: connectionId } } } }],
        })
      ).resolves.toBeUndefined()
      expect(batchWriteCount).toBe(2)
    } finally {
      random.mockRestore()
      send.mockRestore()
    }
  })

  test('fails after the capped number of unprocessed batch-write retries', async () => {
    let batchWriteCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchWriteItemCommand) {
        batchWriteCount += 1
        return { UnprocessedItems: command.input.RequestItems } as never
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)
    vi.useFakeTimers()

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchWriteItems = client as unknown as {
        batchWriteItems: (requests: Record<string, Array<Record<string, unknown>>>) => Promise<void>
      }

      const batchWrite = batchWriteItems.batchWriteItems({
        queued_messages: [{ PutRequest: { Item: { connectionId: { S: connectionId } } } }],
      })
      const expectedFailure = expect(batchWrite).rejects.toThrow('BatchWriteItem exceeded 8 retries')
      await vi.runAllTimersAsync()

      await expectedFailure
      expect(batchWriteCount).toBe(9)
    } finally {
      vi.useRealTimers()
      random.mockRestore()
      send.mockRestore()
    }
  })
})
