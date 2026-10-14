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
const unclaimedFilter = '(attribute_not_exists(claimedUntil) OR claimedUntil <= :now)'
const legacyRecipientFilter = `contains(recipientDids, :recipientDid) AND ${unclaimedFilter}`
const clientOptions = {
  logger: new ConsoleLogger(LogLevel.off),
  region: 'local',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
}

const transactionCanceledError = (code?: string) => {
  const error = new Error(code ?? 'transaction cancelled')
  error.name = 'TransactionCanceledException'
  return Object.assign(error, code === undefined ? {} : { CancellationReasons: [{ Code: code }] })
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

  test('counts recipient messages through a key condition, filtering only on claims', async () => {
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
      expect(recipientQueryInput?.FilterExpression).toBe(unclaimedFilter)
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
      expect(legacyQueryInput?.FilterExpression).toBe(legacyRecipientFilter)
    } finally {
      send.mockRestore()
    }
  })

  test('falls back to the canonical table when recipient metadata cannot be read while counting', async () => {
    const metadataError = new Error('recipient index throttled')
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) throw metadataError
      if (command instanceof QueryCommand && command.input.TableName === 'queued_messages') return { Count: 2 } as never
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(client.getMessageCount(connectionId, recipientDid, 10)).resolves.toBe(2)

      const queries = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command): command is QueryCommand => command instanceof QueryCommand)
      expect(queries).toHaveLength(1)
      expect(queries[0].input.TableName).toBe('queued_messages')
      expect(queries[0].input.FilterExpression).toBe(legacyRecipientFilter)
      expect(queries[0].input.Limit).toBeUndefined()
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
      expect(legacyQueryInput?.FilterExpression).toBe(legacyRecipientFilter)
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
      expect(recipientQueryInput?.FilterExpression).toBe(unclaimedFilter)
      // Index entries are small, so the claim-filtered index query reads full pages.
      expect(recipientQueryInput?.Limit).toBeUndefined()
      const metadataReads = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command): command is GetItemCommand => command instanceof GetItemCommand)
      const canonicalRead = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command) => command instanceof BatchGetItemCommand)
      // The take reads the metadata once, strongly consistent, since it decides whether claims reach the index.
      expect(metadataReads.map((command) => command.input.ConsistentRead)).toEqual([true])
      const claim = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command): command is TransactWriteItemsCommand => command instanceof TransactWriteItemsCommand)
      expect(claim?.input.TransactItems?.map((item) => item.Update?.TableName)).toEqual([
        'queued_messages',
        'queued_messages_recipient_index',
      ])
      expect(
        canonicalRead instanceof BatchGetItemCommand
          ? canonicalRead.input.RequestItems?.queued_messages?.ConsistentRead
          : undefined
      ).toBe(true)
    } finally {
      send.mockRestore()
    }
  })

  test('fails a take closed when recipient metadata cannot be read', async () => {
    const messageId = '1234567890123001'
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) throw new Error('recipient index unavailable')
      if (command instanceof QueryCommand && command.input.TableName === 'queued_messages') {
        return {
          Items: [
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
        } as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      // Without the metadata a claim could miss the recipient index, so the take fails instead of guessing.
      await expect(client.getMessages({ connectionId, recipientDid, limit: 1 })).rejects.toThrow(
        'recipient index unavailable'
      )

      const writes = send.mock.calls.filter(
        ([command]) => command instanceof UpdateItemCommand || command instanceof TransactWriteItemsCommand
      )
      expect(writes).toHaveLength(0)
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
    const transactionError = transactionCanceledError('ConditionalCheckFailed')
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
      const transactions = commands.filter(
        (command): command is TransactWriteItemsCommand => command instanceof TransactWriteItemsCommand
      )
      expect(transactions).toHaveLength(1)
      const transaction = transactions[0]
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

  test('persists W3C trace context alongside the encrypted message', async () => {
    const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
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
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await client.addMessage({
        connectionId,
        recipientDids: [recipientDid],
        encryptedMessage: { ciphertext: 'a', iv: 'b', protected: 'c', tag: 'd' },
        receivedAt: new Date(1),
        telemetry: { traceparent },
      })

      const transaction = send.mock.calls
        .map(([command]) => command as unknown)
        .find((command): command is TransactWriteItemsCommand => command instanceof TransactWriteItemsCommand)
      const update = transaction?.input.TransactItems?.[0].Update
      expect(update?.UpdateExpression).toContain('telemetry = :tc')
      expect(update?.ExpressionAttributeValues?.[':tc']).toEqual({ M: { traceparent: { S: traceparent } } })
    } finally {
      send.mockRestore()
    }
  })

  test('retries a throttled atomic enqueue with a stable idempotency token', async () => {
    const transactionTokens: Array<string | undefined> = []
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
      if (command instanceof TransactWriteItemsCommand) {
        transactionTokens.push(command.input.ClientRequestToken)
        if (transactionTokens.length === 1) throw transactionCanceledError('ProvisionedThroughputExceeded')
        return {} as never
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(
        client.addMessage({
          connectionId,
          recipientDids: [recipientDid],
          encryptedMessage: { ciphertext: 'a', iv: 'b', protected: 'c', tag: 'd' },
          receivedAt: new Date(1),
        })
      ).resolves.toBeDefined()

      expect(transactionTokens).toHaveLength(2)
      expect(transactionTokens[1]).toBe(transactionTokens[0])
    } finally {
      random.mockRestore()
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

  test('returns every fetched message when only some delete transactions succeed', async () => {
    const messages = Array.from({ length: 51 }, (_, index) => ({
      connectionId: { S: connectionId },
      messageId: { N: String(index + 1) },
      recipientDids: { L: [{ S: recipientDid }] },
      receivedAt: { N: String(index) },
    }))
    const transactionSizes: number[] = []
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof QueryCommand) return { Items: messages } as never
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
        // Claim transactions (Updates) precede the deletes; only the delete batching is under test.
        if (!command.input.TransactItems?.every((item) => item.Delete)) return {} as never
        const transactionSize = command.input.TransactItems.length
        transactionSizes.push(transactionSize)
        if (transactionSize === 100) throw new Error('first transaction failed')
        return {} as never
      }
      return {} as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const fetchedMessages = await client.getMessages({ connectionId, deleteMessages: true })

      expect(transactionSizes).toEqual([100, 2])
      expect(fetchedMessages.map((message) => message.id)).toEqual(messages.map((message) => message.messageId.N))
    } finally {
      send.mockRestore()
    }
  })

  test('retries a reasonless transaction cancellation during atomic deletion', async () => {
    const messageId = '1234567890123001'
    const transactionTokens: Array<string | undefined> = []
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof BatchGetItemCommand) {
        return {
          Responses: {
            queued_messages: [
              {
                connectionId: { S: connectionId },
                messageId: { N: messageId },
                recipientDids: { L: [{ S: recipientDid }] },
                receivedAt: { N: '0' },
              },
            ],
          },
        } as never
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
        transactionTokens.push(command.input.ClientRequestToken)
        if (transactionTokens.length === 1) throw transactionCanceledError()
        return {} as never
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(client.removeMessages({ connectionId, messageIds: [messageId] })).resolves.toBeUndefined()
      expect(transactionTokens).toHaveLength(2)
      expect(transactionTokens[1]).toBe(transactionTokens[0])
    } finally {
      random.mockRestore()
      send.mockRestore()
    }
  })

  test('fails after the capped number of retryable transaction cancellations', async () => {
    let transactionCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof TransactWriteItemsCommand) {
        transactionCount += 1
        throw transactionCanceledError('TransactionConflict')
      }
      return {} as never
    })
    const random = vi.spyOn(Math, 'random').mockReturnValue(0)
    vi.useFakeTimers()

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      const transactWriteItemsWithRetry = client as unknown as {
        transactWriteItemsWithRetry: (input: TransactWriteItemsCommand['input']) => Promise<void>
      }
      const transaction = transactWriteItemsWithRetry.transactWriteItemsWithRetry({
        ClientRequestToken: 'retry-cap-test',
        TransactItems: [
          {
            Delete: {
              TableName: 'queued_messages',
              Key: { connectionId: { S: connectionId }, messageId: { N: '1' } },
            },
          },
        ],
      })
      const expectedFailure = expect(transaction).rejects.toThrow('TransactWriteItems exceeded 8 retries')
      await vi.runAllTimersAsync()

      await expectedFailure
      expect(transactionCount).toBe(9)
    } finally {
      vi.useRealTimers()
      random.mockRestore()
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

suite('dynamodb claims', () => {
  // Without recipient index metadata every message is legacy, so each claim is a single UpdateItem.
  const legacyItems = (count: number) =>
    Array.from({ length: count }, (_, index) => ({
      connectionId: { S: connectionId },
      messageId: { N: String(index + 1) },
      recipientDids: { L: [{ S: recipientDid }] },
      receivedAt: { N: String(index) },
    }))

  const mockQueue = (
    items: ReturnType<typeof legacyItems>,
    onUpdate: (command: UpdateItemCommand) => Promise<unknown>
  ) =>
    vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never
      if (command instanceof GetItemCommand) return {} as never
      if (command instanceof QueryCommand) return { Items: items } as never
      if (command instanceof UpdateItemCommand) return (await onUpdate(command)) as never
      return {} as never
    })

  const messageIdOf = (command: UpdateItemCommand) => command.input.Key?.messageId?.N

  test('reads large pages past claimed messages instead of limit-sized ones', async () => {
    const send = mockQueue(legacyItems(10), async () => ({}))

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await client.getMessages({ connectionId, limit: 10 })
      await client.getMessageCount(connectionId, undefined, 10)

      const queries = send.mock.calls
        .map(([command]) => command as unknown)
        .filter((command): command is QueryCommand => command instanceof QueryCommand)
      const take = queries.find((query) => query.input.Select !== 'COUNT')
      const count = queries.find((query) => query.input.Select === 'COUNT')
      expect(take?.input.FilterExpression).toBe(unclaimedFilter)
      expect(take?.input.Limit).toBe(100)
      expect(count?.input.FilterExpression).toBe(unclaimedFilter)
      expect(count?.input.Limit).toBeUndefined()
    } finally {
      send.mockRestore()
    }
  })

  test('caps concurrent claim writes', async () => {
    let inFlight = 0
    let maxInFlight = 0
    const send = mockQueue(legacyItems(25), async () => {
      inFlight += 1
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise((resolve) => setTimeout(resolve, 5))
      inFlight -= 1
      return {}
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      expect(await client.getMessages({ connectionId })).toHaveLength(25)
      expect(maxInFlight).toBe(10)
    } finally {
      send.mockRestore()
    }
  })

  test('retries a throttled claim write', async () => {
    let attempts = 0
    const send = mockQueue(legacyItems(1), async () => {
      attempts += 1
      if (attempts === 1) {
        const error = new Error('throttled')
        error.name = 'ThrottlingException'
        throw error
      }
      return {}
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      expect(await client.getMessages({ connectionId, limit: 1 })).toHaveLength(1)
      expect(attempts).toBe(2)
    } finally {
      send.mockRestore()
    }
  })

  test('releases the claims it won when a take fails', async () => {
    const updates: UpdateItemCommand[] = []
    const send = mockQueue(legacyItems(3), async (command) => {
      updates.push(command)
      if (command.input.UpdateExpression?.startsWith('SET') && messageIdOf(command) === '2') {
        throw new Error('claim write failed')
      }
      return {}
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)
      await expect(client.getMessages({ connectionId, limit: 3 })).rejects.toThrow('claim write failed')

      const releases = updates.filter((command) => command.input.UpdateExpression === 'REMOVE claimedUntil')
      expect(releases.map(messageIdOf)).toEqual(['1', '3'])
      expect(releases.every((command) => command.input.ConditionExpression === 'claimedUntil = :claimedUntil')).toBe(
        true
      )
    } finally {
      send.mockRestore()
    }
  })
})
