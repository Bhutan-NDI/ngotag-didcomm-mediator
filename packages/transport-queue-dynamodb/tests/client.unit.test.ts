import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
  UpdateItemCommand,
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
      expect(createCommands[0].input.ProvisionedThroughput).toEqual({
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5,
      })
      expect(createCommands[1].input.BillingMode).toBe('PAY_PER_REQUEST')
      expect(createCommands[1].input.ProvisionedThroughput).toBeUndefined()
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

  test('does not apply a pre-filter limit to a legacy recipient count', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) return {} as never
      if (command instanceof QueryCommand) return { Count: 1 } as never
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      expect(await client.getMessageCount(connectionId, recipientDid, 10)).toBe(1)

      const legacyQuery = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof QueryCommand)
      const legacyQueryInput = legacyQuery instanceof QueryCommand ? legacyQuery.input : undefined
      expect(legacyQueryInput?.FilterExpression).toBe('contains(recipientDids, :recipientDid)')
      expect(legacyQueryInput?.Limit).toBeUndefined()
    } finally {
      send.mockRestore()
    }
  })

  test('fails fast when a count query returns a non-advancing pagination key', async () => {
    const repeatedKey = {
      connectionId: { S: connectionId },
      messageId: { N: '1' },
    }
    let queryCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof QueryCommand) {
        queryCount += 1
        return { Count: 0, LastEvaluatedKey: repeatedKey } as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(client.getMessageCount(connectionId)).rejects.toThrow('non-advancing pagination key')
      expect(queryCount).toBe(2)
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

  test('uses strongly consistent canonical reads and preserves them on unprocessed-key retries', async () => {
    const key = {
      connectionId: { S: connectionId },
      messageId: { N: '1234567890123001' },
    }
    let batchGetCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchGetItemCommand) {
        batchGetCount += 1
        return batchGetCount === 1
          ? { Responses: { queued_messages: [] }, UnprocessedKeys: { queued_messages: { Keys: [key] } } }
          : ({ Responses: { queued_messages: [] } } as never)
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchGetMessages = client as unknown as {
        batchGetMessages: (connection: string, messageIds: string[]) => Promise<unknown[]>
      }

      await batchGetMessages.batchGetMessages(connectionId, ['1234567890123001'])

      const batchGetCommands = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command): command is BatchGetItemCommand => command instanceof BatchGetItemCommand)
      expect(batchGetCommands).toHaveLength(2)
      expect(
        batchGetCommands.every((command) => command.input.RequestItems?.queued_messages?.ConsistentRead === true)
      ).toBe(true)
      expect(batchGetCommands[1].input.RequestItems?.queued_messages?.Keys).toEqual([key])
    } finally {
      send.mockRestore()
    }
  })

  test('atomically writes a new canonical message and its recipient index entries', async () => {
    const transactionError = new Error('transaction failed')
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
      if (command instanceof TransactWriteItemsCommand) throw transactionError
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(
        client.addMessage({
          connectionId,
          recipientDids: [recipientDid, 'did:key:456'],
          encryptedMessage: { ciphertext: 'a', iv: 'b', protected: 'c', tag: 'd' },
          receivedAt: new Date(1),
        })
      ).rejects.toThrow(transactionError)

      const commands = send.mock.calls.map(([command]) => command as unknown)
      expect(commands.some((command) => command instanceof UpdateItemCommand)).toBe(false)
      const transaction = commands.find((command) => command instanceof TransactWriteItemsCommand)
      const transactionInput = transaction instanceof TransactWriteItemsCommand ? transaction.input : undefined
      expect(transactionInput?.TransactItems).toHaveLength(3)
      expect(transactionInput?.TransactItems?.[0].Update?.TableName).toBe('queued_messages')
      expect(
        transactionInput?.TransactItems?.slice(1).every(
          (item) => item.Put?.TableName === 'queued_messages_recipient_index'
        )
      ).toBe(true)
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

  test('attempts every batch-write chunk even when one chunk fails', async () => {
    const batchSizes: number[] = []
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchWriteItemCommand) {
        const size = command.input.RequestItems?.queued_messages?.length ?? 0
        batchSizes.push(size)
        if (size === 25) throw new Error('first chunk failed')
        return {} as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchWrite = client as unknown as {
        batchWrite: (requests: Array<Record<string, unknown>>, tableName: string) => Promise<void>
      }
      const requests = Array.from({ length: 26 }, (_, index) => ({
        DeleteRequest: { Key: { messageId: { N: String(index) } } },
      }))

      await expect(batchWrite.batchWrite(requests, 'queued_messages')).rejects.toThrow('first chunk failed')
      expect(batchSizes).toEqual([25, 1])
    } finally {
      send.mockRestore()
    }
  })
})
