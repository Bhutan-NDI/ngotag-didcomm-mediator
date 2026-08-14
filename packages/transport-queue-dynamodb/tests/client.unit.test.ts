import {
  BatchGetItemCommand,
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
      const metadataRead = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof GetItemCommand)
      const canonicalRead = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof BatchGetItemCommand)
      expect(metadataRead instanceof GetItemCommand ? metadataRead.input.ConsistentRead : undefined).toBeUndefined()
      expect(
        canonicalRead instanceof BatchGetItemCommand
          ? canonicalRead.input.RequestItems?.queued_messages?.ConsistentRead
          : undefined
      ).toBeUndefined()
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
        batchGetMessages: (connection: string, messageIds: string[], consistentRead: boolean) => Promise<unknown[]>
      }

      await batchGetMessages.batchGetMessages(connectionId, ['1234567890123001'], true)

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
      const metadataReads = commands.filter((command): command is GetItemCommand => command instanceof GetItemCommand)
      expect(metadataReads.every((command) => command.input.ConsistentRead === true)).toBe(true)
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

  test('atomically deletes each message with its pointers and attempts every transaction batch', async () => {
    const messages = Array.from({ length: 51 }, (_, index) => ({
      connectionId: { S: connectionId },
      messageId: { N: String(index + 1) },
      recipientDids: { L: [{ S: recipientDid }] },
      receivedAt: { N: String(index) },
    }))
    const transactionInputs: TransactWriteItemsCommand['input'][] = []
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchGetItemCommand) {
        return { Responses: { queued_messages: messages } } as never
      }
      if (command instanceof GetItemCommand) {
        return {
          Item: {
            connectionId: { S: connectionId },
            recipientMessageId: { S: '__recipient_index_metadata__' },
            cutoverMessageId: { N: '0' },
          },
        } as never
      }
      if (command instanceof TransactWriteItemsCommand) {
        transactionInputs.push(command.input)
        if (command.input.TransactItems?.length === 100) throw new Error('first transaction failed')
        return {} as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(
        client.removeMessages({ connectionId, messageIds: messages.map((message) => message.messageId.N) })
      ).rejects.toThrow('first transaction failed')

      expect(transactionInputs.map((input) => input.TransactItems?.length)).toEqual([100, 2])
      for (const input of transactionInputs) {
        const tables = input.TransactItems?.map((item) => item.Delete?.TableName)
        expect(tables?.filter((table) => table === 'queued_messages')).toHaveLength((tables?.length ?? 0) / 2)
        expect(tables?.filter((table) => table === 'queued_messages_recipient_index')).toHaveLength(
          (tables?.length ?? 0) / 2
        )
      }

      const batchGet = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof BatchGetItemCommand)
      const metadataRead = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof GetItemCommand)
      expect(
        batchGet instanceof BatchGetItemCommand
          ? batchGet.input.RequestItems?.queued_messages?.ConsistentRead
          : undefined
      ).toBe(true)
      expect(metadataRead instanceof GetItemCommand ? metadataRead.input.ConsistentRead : undefined).toBe(true)
    } finally {
      send.mockRestore()
    }
  })

  test('fails after the capped number of unprocessed batch-get retries', async () => {
    let batchGetCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchGetItemCommand) {
        batchGetCount += 1
        return { UnprocessedKeys: command.input.RequestItems } as never
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)
    vi.useFakeTimers()

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchGetMessages = client as unknown as {
        batchGetMessages: (connection: string, messageIds: string[], consistentRead: boolean) => Promise<unknown[]>
      }

      const batchGet = batchGetMessages.batchGetMessages(connectionId, ['1'], false)
      const expectedFailure = expect(batchGet).rejects.toThrow('BatchGetItem exceeded 8 retries')
      await vi.runAllTimersAsync()

      await expectedFailure
      expect(batchGetCount).toBe(9)
    } finally {
      vi.useRealTimers()
      random.mockRestore()
      send.mockRestore()
    }
  })

  test('starts every batch-get chunk before waiting for a slow chunk', async () => {
    let resolveFirstChunk: ((value: { Responses: { queued_messages: never[] } }) => void) | undefined
    const firstChunk = new Promise<{ Responses: { queued_messages: never[] } }>((resolve) => {
      resolveFirstChunk = resolve
    })
    const batchSizes: number[] = []
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchGetItemCommand) {
        const size = command.input.RequestItems?.queued_messages?.Keys?.length ?? 0
        batchSizes.push(size)
        if (size === 100) return firstChunk as never
        return { Responses: { queued_messages: [] } } as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const batchGetMessages = client as unknown as {
        batchGetMessages: (connection: string, messageIds: string[], consistentRead: boolean) => Promise<unknown[]>
      }
      const messageIds = Array.from({ length: 101 }, (_, index) => String(index + 1))

      const batchGet = batchGetMessages.batchGetMessages(connectionId, messageIds, false)
      expect(batchSizes).toEqual([100, 1])
      resolveFirstChunk?.({ Responses: { queued_messages: [] } })
      await expect(batchGet).resolves.toEqual([])
    } finally {
      send.mockRestore()
    }
  })
})
