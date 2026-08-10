import { QueryCommand } from '@aws-sdk/client-dynamodb'
import { ConsoleLogger, LogLevel } from '@credo-ts/core'
import { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import { beforeAll, expect, suite, test, vi } from 'vitest'
import { DynamoDbClientRepository } from '../src/client.js'

const connectionId = '4ffdd113-117b-4827-9af5-28aa73ec4bad'
const recipientDids = ['did:key:123', 'did:jwk:123', 'did:peer:3abba']
const encryptedMessage: DidCommEncryptedMessage = {
  ciphertext: 'ciphertext',
  iv: 'iv',
  protected: 'protected',
  tag: 'tag',
}

suite('dynamodb client count query pagination', () => {
  test('get count queries all pages for a connection', async () => {
    const lastEvaluatedKey = {
      connectionId: { S: connectionId },
      messageId: { N: '2' },
    }
    const send = vi
      .fn()
      .mockResolvedValueOnce({ Count: 2, LastEvaluatedKey: lastEvaluatedKey })
      .mockResolvedValueOnce({ Count: 3 })

    const paginatedClient = Object.create(DynamoDbClientRepository.prototype) as DynamoDbClientRepository & {
      dynamodbClient: { send: typeof send }
      tableName: string
      logger: ConsoleLogger
    }
    paginatedClient.dynamodbClient = { send }
    paginatedClient.tableName = 'queued_messages'
    paginatedClient.logger = new ConsoleLogger(LogLevel.off)

    const count = await paginatedClient.getMessageCount(connectionId)

    expect(count).toStrictEqual(5)
    expect(send).toHaveBeenCalledTimes(2)

    const firstCommand = send.mock.calls[0][0] as QueryCommand
    expect(firstCommand).toBeInstanceOf(QueryCommand)
    expect(firstCommand.input).toMatchObject({
      TableName: 'queued_messages',
      KeyConditionExpression: 'connectionId = :connectionId',
      ExpressionAttributeValues: {
        ':connectionId': { S: connectionId },
      },
      Select: 'COUNT',
    })
    expect(firstCommand.input).not.toHaveProperty('FilterExpression')
    expect(firstCommand.input.ExclusiveStartKey).toBeUndefined()

    const secondCommand = send.mock.calls[1][0] as QueryCommand
    expect(secondCommand.input.ExclusiveStartKey).toStrictEqual(lastEvaluatedKey)
  })
})

suite('dynamodb client', () => {
  let client: DynamoDbClientRepository

  beforeAll(async () => {
    client = await DynamoDbClientRepository.initialize({
      logger: new ConsoleLogger(LogLevel.off),
      region: 'local',
      credentials: {
        accessKeyId: 'local',
        secretAccessKey: 'local',
      },
    })
  })

  test('initialize', async () => {
    expect(client).toBeDefined()
  })

  test('add a message', async () => {
    const timestamp = new Date()
    const id = await client.addMessage({
      connectionId: connectionId,
      receivedAt: timestamp,
      encryptedMessage,
      recipientDids: recipientDids,
    })

    expect(id.startsWith(timestamp.getTime().toString())).toBeTruthy()
  })

  test('get count', async () => {
    const count = await client.getMessageCount(connectionId)
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('get a message', async () => {
    const messages = await client.getMessages({
      connectionId: connectionId,
      limit: 1,
    })

    expect(messages.length).toStrictEqual(1)

    const [message] = messages

    expect(message.connectionId).toStrictEqual(connectionId)
    expect(message.receivedAt).toBeInstanceOf(Date)
    expect(message.encryptedMessage).toEqual(encryptedMessage)
    expect(message.recipientDids).toEqual(recipientDids)
  })

  test('get a message and filter on recipient did', async () => {
    const messages = await client.getMessages({
      connectionId: connectionId,
      limit: 1,
      recipientDid: recipientDids[0],
    })

    expect(messages.length).toStrictEqual(1)

    const [message] = messages

    expect(message.connectionId).toStrictEqual(connectionId)
    expect(message.receivedAt).toBeInstanceOf(Date)
    expect(message.encryptedMessage).toEqual(encryptedMessage)
    expect(message.recipientDids).toEqual(recipientDids)
  })

  test('get message and remove a message', async () => {
    const count = await client.getMessageCount(connectionId)

    await client.getMessages({
      connectionId: connectionId,
      deleteMessages: true,
    })

    const countAfterDelete = await client.getMessageCount(connectionId)

    expect(count).toBeGreaterThan(countAfterDelete)
  })

  test('add and explicit delete', async () => {
    const id = await client.addMessage({
      connectionId,
      encryptedMessage,
      receivedAt: new Date(),
      recipientDids,
    })

    const count = await client.getMessageCount(connectionId)

    await client.removeMessages({ connectionId: connectionId, messageIds: [id] })

    const countAfterDelete = await client.getMessageCount(connectionId)

    expect(count - countAfterDelete).toStrictEqual(1)
  })

  test('add and delete bunch of messages', async () => {
    const newConnectionId = 'new-connection-id'
    const requests: Array<Promise<string>> = []
    for (let i = 0; i < 100; i++) {
      requests.push(
        client.addMessage({
          connectionId: newConnectionId,
          recipientDids,
          encryptedMessage,
          receivedAt: new Date(i),
        })
      )
    }

    const ids = await Promise.all(requests)

    const count = await client.getMessageCount(newConnectionId)
    expect(count).toStrictEqual(100)

    await client.removeMessages({
      connectionId: newConnectionId,
      messageIds: ids,
    })

    const countAfterDelete = await client.getMessageCount(newConnectionId)
    expect(countAfterDelete).toStrictEqual(0)
  })

  test('validate sorting', async () => {
    const now = new Date()
    const oneHourInThePast = new Date()
    oneHourInThePast.setHours(oneHourInThePast.getHours() - 1)
    const oneHourInTheFuture = new Date()
    oneHourInTheFuture.setHours(oneHourInTheFuture.getHours() + 1)

    const oneHourInThePastMessageId = await client.addMessage({
      receivedAt: oneHourInThePast,
      connectionId,
      encryptedMessage,
      recipientDids,
    })

    const nowMessageId = await client.addMessage({
      receivedAt: now,
      connectionId,
      encryptedMessage,
      recipientDids,
    })

    const oneHourInTheFutureMessageId = await client.addMessage({
      receivedAt: oneHourInTheFuture,
      connectionId,
      encryptedMessage,
      recipientDids,
    })

    const messages = await client.getMessages({ connectionId, limit: 3, deleteMessages: true })

    expect(messages[0].id).toStrictEqual(oneHourInThePastMessageId)
    expect(messages[1].id).toStrictEqual(nowMessageId)
    expect(messages[2].id).toStrictEqual(oneHourInTheFutureMessageId)
  })
})
