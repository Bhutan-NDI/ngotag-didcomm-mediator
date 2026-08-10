import { CreateTableCommand, DescribeTableCommand, DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { AgentContext, ConsoleLogger, LogLevel } from '@credo-ts/core'
import { expect, suite, test, vi } from 'vitest'
import { DynamoDbClientRepository } from '../src/client.js'
import { DidCommTransportQueueDynamoDb } from '../src/TransportQueueDynamoDb.js'

const connectionId = '4ffdd113-117b-4827-9af5-28aa73ec4bad'
const recipientDid = 'did:key:123'
const clientOptions = {
  logger: new ConsoleLogger(LogLevel.off),
  region: 'local',
  credentials: {
    accessKeyId: 'local',
    secretAccessKey: 'local',
  },
}

suite('dynamodb client count query', () => {
  test('validates the key schema of an existing table', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) {
        throw Object.assign(new Error('Table already exists'), { name: 'ResourceInUseException' })
      }

      return {
        Table: {
          KeySchema: [
            { AttributeName: 'connectionId', KeyType: 'HASH' },
            { AttributeName: 'messageId', KeyType: 'RANGE' },
          ],
        },
      } as never
    })

    try {
      await expect(DynamoDbClientRepository.initialize(clientOptions)).resolves.toBeDefined()
    } finally {
      send.mockRestore()
    }
  })

  test('rejects an existing table with an incompatible key schema', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) {
        throw Object.assign(new Error('Table already exists'), { name: 'ResourceInUseException' })
      }

      return {
        Table: {
          KeySchema: [{ AttributeName: 'messageId', KeyType: 'HASH' }],
        },
      } as never
    })

    try {
      await expect(DynamoDbClientRepository.initialize(clientOptions)).rejects.toThrow('incompatible key schema')
    } finally {
      send.mockRestore()
    }
  })

  test('paginates exact counts and caps pickup counts at the requested maximum', async () => {
    const lastEvaluatedKey = {
      connectionId: { S: connectionId },
      messageId: { N: '2' },
    }
    const queryResponses = [
      { Count: 2, LastEvaluatedKey: lastEvaluatedKey },
      { Count: 3 },
      { Count: 2, LastEvaluatedKey: lastEvaluatedKey },
      { Count: 3 },
    ]
    let queryCount = 0
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never

      return queryResponses[queryCount++] as never
    })

    try {
      const client = await DynamoDbClientRepository.initialize(clientOptions)

      expect(await client.getMessageCount(connectionId)).toStrictEqual(5)
      expect(await client.getMessageCount(connectionId, undefined, 2)).toStrictEqual(2)
      expect(await client.getMessageCount(connectionId, recipientDid, 10)).toStrictEqual(3)

      const queryCommands = send.mock.calls.filter(([command]) => command instanceof QueryCommand)
      const firstCommand = queryCommands[0][0] as QueryCommand
      expect(firstCommand.input).toMatchObject({
        TableName: 'queued_messages',
        KeyConditionExpression: 'connectionId = :connectionId',
        ExpressionAttributeValues: {
          ':connectionId': { S: connectionId },
        },
        ScanIndexForward: false,
        Select: 'COUNT',
      })
      expect(firstCommand.input).not.toHaveProperty('FilterExpression')
      expect(firstCommand.input.ExclusiveStartKey).toBeUndefined()

      const secondCommand = queryCommands[1][0] as QueryCommand
      expect(secondCommand.input.ExclusiveStartKey).toStrictEqual(lastEvaluatedKey)

      const cappedCommand = queryCommands[2][0] as QueryCommand
      expect(cappedCommand.input.Limit).toStrictEqual(2)

      const recipientCommand = queryCommands[3][0] as QueryCommand
      expect(recipientCommand.input).toMatchObject({
        FilterExpression: 'contains(recipientDids, :recipientDid)',
        ExpressionAttributeValues: {
          ':recipientDid': { S: recipientDid },
        },
        Limit: 10,
      })
    } finally {
      send.mockRestore()
    }
  })

  test('uses Credo maximumBatchSize as the pickup count ceiling', async () => {
    const send = vi.spyOn(DynamoDBClient.prototype, 'send').mockImplementation(async (command) => {
      if (command instanceof CreateTableCommand) return {} as never
      if (command instanceof DescribeTableCommand) return { Table: { TableStatus: 'ACTIVE' } } as never

      return { Count: 2, LastEvaluatedKey: { connectionId: { S: connectionId } } } as never
    })
    const agentContext = {
      dependencyManager: {
        resolve: () => ({ maximumBatchSize: 2 }),
      },
    } as unknown as AgentContext

    try {
      const repository = await DidCommTransportQueueDynamoDb.initialize(clientOptions)

      expect(await repository.getAvailableMessageCount(agentContext, { connectionId })).toStrictEqual(2)

      const queryCommand = send.mock.calls.find(([command]) => command instanceof QueryCommand)?.[0] as QueryCommand
      expect(queryCommand.input.Limit).toStrictEqual(2)
    } finally {
      send.mockRestore()
    }
  })
})
