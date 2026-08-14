import { createHash, randomInt } from 'node:crypto'
import {
  BatchGetItemCommand,
  CreateTableCommand,
  CreateTableCommandInput,
  DescribeTableCommand,
  DescribeTableCommandInput,
  DynamoDBClient,
  DynamoDBClientConfigType,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  QueryCommandInput,
  type TransactWriteItem,
  TransactWriteItemsCommand,
  type TransactWriteItemsCommandInput,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { Logger } from '@credo-ts/core'
import { DidCommEncryptedMessage } from '@credo-ts/didcomm'
import {
  attributeDefinitions,
  keySchema,
  QueuedMessage,
  recipientIndexAttributeDefinitions,
  recipientIndexKeySchema,
} from './structure.js'

export type AddQueuedMessageOptions = {
  connectionId: string
  receivedAt?: Date
  recipientDids: string[]
  encryptedMessage: DidCommEncryptedMessage
}

export type RemoveQueuedMessageOptions = {
  connectionId: string
  messageIds: Array<string>
}

export type DynamoDbClientRepositoryOptions = DynamoDBClientConfigType & {
  /** @default queued_messages */
  tableName?: string
  /** @default <tableName>_recipient_index */
  recipientIndexTableName?: string
  logger: Logger
}

type RecipientIndexMetadata = { cutoverMessageId: number }
type RecipientIndexEntry = { messageId: string }

const RECIPIENT_INDEX_METADATA_KEY = '__recipient_index_metadata__'
const RECIPIENT_INDEX_PREFIX = 'r#'
const MAX_OPERATION_RETRIES = 8
const INITIAL_RETRY_DELAY_MS = 50
const MAX_RETRY_DELAY_MS = 2_000
const RETRYABLE_TRANSACTION_CANCELLATION_CODES = new Set([
  'ProvisionedThroughputExceeded',
  'ThrottlingError',
  'TransactionConflict',
])
const NON_RETRYABLE_TRANSACTION_CANCELLATION_CODES = new Set([
  'ConditionalCheckFailed',
  'ItemCollectionSizeLimitExceeded',
  'ValidationError',
])

export class DynamoDbClientRepository {
  private dynamodbClient: DynamoDBClient
  private tableName: string
  private recipientIndexTableName: string
  private logger: Logger

  private constructor(options: DynamoDbClientRepositoryOptions) {
    this.dynamodbClient = new DynamoDBClient(options)
    this.tableName = options.tableName ?? 'queued_messages'
    this.recipientIndexTableName = options.recipientIndexTableName ?? `${this.tableName}_recipient_index`
    this.logger = options.logger
  }

  public static async initialize(options: DynamoDbClientRepositoryOptions): Promise<DynamoDbClientRepository> {
    const dcr = new DynamoDbClientRepository(options)

    await dcr.ensureTable({
      TableName: dcr.tableName,
      AttributeDefinitions: attributeDefinitions,
      KeySchema: keySchema,
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
    })
    await dcr.ensureTable({
      TableName: dcr.recipientIndexTableName,
      AttributeDefinitions: recipientIndexAttributeDefinitions,
      KeySchema: recipientIndexKeySchema,
      // Recipient index writes fan out per recipient DID. On-demand billing
      // lets an auto-created companion table absorb that variable write rate
      // without inheriting the primary table's placeholder 5-WCU ceiling.
      BillingMode: 'PAY_PER_REQUEST',
    })

    return dcr
  }

  private async ensureTable(params: CreateTableCommandInput): Promise<void> {
    if (!params.TableName) throw new Error('DynamoDB table name is required')
    const tableName = params.TableName
    try {
      await this.dynamodbClient.send(new CreateTableCommand(params))
      await this.waitForTableToExist(tableName)
    } catch (error) {
      if (error instanceof Error && error.name === 'ResourceInUseException') {
        await this.validateTableKeySchema(tableName, params.AttributeDefinitions ?? [], params.KeySchema ?? [])
        return
      }
      throw error
    }
  }

  private async validateTableKeySchema(
    tableName: string,
    expectedAttributeDefinitions: NonNullable<CreateTableCommandInput['AttributeDefinitions']>,
    expectedKeySchema: NonNullable<CreateTableCommandInput['KeySchema']>
  ): Promise<void> {
    const response = await this.dynamodbClient.send(new DescribeTableCommand({ TableName: tableName }))
    const actualKeySchema = response.Table?.KeySchema
    const actualAttributeDefinitions = response.Table?.AttributeDefinitions
    const hasExpectedKeySchema =
      actualKeySchema?.length === expectedKeySchema.length &&
      expectedKeySchema.every((expectedKey) =>
        actualKeySchema.some(
          (actualKey) =>
            actualKey.AttributeName === expectedKey.AttributeName && actualKey.KeyType === expectedKey.KeyType
        )
      )
    const hasExpectedAttributeDefinitions = expectedAttributeDefinitions.every((expectedAttribute) =>
      actualAttributeDefinitions?.some(
        (actualAttribute) =>
          actualAttribute.AttributeName === expectedAttribute.AttributeName &&
          actualAttribute.AttributeType === expectedAttribute.AttributeType
      )
    )

    if (!hasExpectedKeySchema || !hasExpectedAttributeDefinitions) {
      throw new Error(
        `DynamoDB table ${tableName} has an incompatible schema. Expected key schema ${JSON.stringify(
          expectedKeySchema
        )} and attribute definitions ${JSON.stringify(expectedAttributeDefinitions)}, received key schema ${JSON.stringify(
          actualKeySchema
        )} and attribute definitions ${JSON.stringify(actualAttributeDefinitions)}`
      )
    }
  }

  private async getLatestMessageId(connectionId: string) {
    const response = await this.dynamodbClient.send(
      new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: 'connectionId = :connectionId',
        ExpressionAttributeValues: { ':connectionId': { S: connectionId } },
        ProjectionExpression: 'messageId',
        Select: 'SPECIFIC_ATTRIBUTES',
        Limit: 1,
        ScanIndexForward: false,
        // This result fixes the migration boundary for a connection. It must
        // include every pre-index message so none can become invisible to a
        // recipient-specific pickup after the cutover is recorded.
        ConsistentRead: true,
      })
    )
    return response.Items?.[0]?.messageId
  }

  private async waitForTableToExist(tableName: string): Promise<void> {
    const startTime = Date.now()
    const maxWaitTime = 30000
    return new Promise((resolve, reject) => {
      const checkTableStatus = async () => {
        try {
          const response = await this.dynamodbClient.send(
            new DescribeTableCommand({ TableName: tableName } as DescribeTableCommandInput)
          )
          if (response.Table?.TableStatus === 'ACTIVE') return resolve()
          if (Date.now() - startTime > maxWaitTime) {
            return reject(new Error(`Table ${tableName} did not become active within ${maxWaitTime}ms`))
          }
          setTimeout(checkTableStatus, 500)
        } catch (error) {
          reject(error)
        }
      }
      checkTableStatus()
    })
  }

  // Existing queues are kept readable without a table scan. The first writer
  // after this release records the greatest existing message id for its
  // connection. Older messages are read through the legacy path until drained;
  // newer messages use the recipient index immediately.
  private async getRecipientIndexMetadata(
    connectionId: string,
    consistentRead = false
  ): Promise<RecipientIndexMetadata | undefined> {
    const response = await this.dynamodbClient.send(
      new GetItemCommand({
        TableName: this.recipientIndexTableName,
        Key: marshall({ connectionId, recipientMessageId: RECIPIENT_INDEX_METADATA_KEY }),
        ConsistentRead: consistentRead || undefined,
      })
    )
    if (!response.Item) return undefined
    return unmarshall(response.Item) as RecipientIndexMetadata
  }

  private async prepareRecipientIndex(connectionId: string): Promise<RecipientIndexMetadata> {
    // The write path must observe an existing cutover marker immediately. Read
    // paths can use the eventually consistent default because a transient miss
    // safely falls back to the legacy query.
    const existing = await this.getRecipientIndexMetadata(connectionId, true)
    if (existing) return existing

    const latestMessageId = await this.getLatestMessageId(connectionId)
    const metadata = { cutoverMessageId: Number(latestMessageId?.N ?? 0) }
    try {
      await this.dynamodbClient.send(
        new PutItemCommand({
          TableName: this.recipientIndexTableName,
          Item: marshall({ connectionId, recipientMessageId: RECIPIENT_INDEX_METADATA_KEY, ...metadata }),
          ConditionExpression: 'attribute_not_exists(connectionId) AND attribute_not_exists(recipientMessageId)',
        })
      )
      return metadata
    } catch (error) {
      if (!(error instanceof Error) || error.name !== 'ConditionalCheckFailedException') throw error
      const concurrentMetadata = await this.getRecipientIndexMetadata(connectionId, true)
      if (!concurrentMetadata) throw error
      return concurrentMetadata
    }
  }

  private recipientIndexSortKey(recipientDid: string, messageId: string | number): string {
    return `${this.recipientIndexPrefix(recipientDid)}${String(messageId).padStart(16, '0')}`
  }

  private recipientIndexPrefix(recipientDid: string): string {
    const recipientHash = createHash('sha256').update(recipientDid).digest('hex')
    return `${RECIPIENT_INDEX_PREFIX}${recipientHash}#`
  }

  private recipientIndexRange(recipientDid: string, cutoverMessageId: number) {
    const prefix = this.recipientIndexPrefix(recipientDid)
    return {
      ':recipientIndexStart': { S: `${prefix}${String(cutoverMessageId).padStart(16, '0')}` },
      ':recipientIndexEnd': { S: `${prefix}\uffff` },
    }
  }

  async getMessageCount(connectionId: string, recipientDid?: string, maximumCount?: number): Promise<number> {
    try {
      if (recipientDid === undefined) return await this.countMessagesByConnection(connectionId, maximumCount)

      const metadata = await this.getRecipientIndexMetadata(connectionId)
      if (!metadata) return await this.countLegacyRecipientMessages(connectionId, recipientDid, undefined, maximumCount)

      const indexed = await this.countIndexedRecipientMessages(
        connectionId,
        recipientDid,
        metadata.cutoverMessageId,
        maximumCount
      )
      if (maximumCount !== undefined && indexed >= maximumCount) return maximumCount
      const legacy = await this.countLegacyRecipientMessages(
        connectionId,
        recipientDid,
        metadata.cutoverMessageId,
        maximumCount === undefined ? undefined : maximumCount - indexed
      )
      return indexed + legacy
    } catch (error) {
      this.logger.error('Error getting entries count:', { error })
      throw error
    }
  }

  private async countMessagesByConnection(connectionId: string, maximumCount?: number): Promise<number> {
    return this.countQuery(
      {
        TableName: this.tableName,
        KeyConditionExpression: 'connectionId = :connectionId',
        ExpressionAttributeValues: { ':connectionId': { S: connectionId } },
      },
      maximumCount
    )
  }

  private async countIndexedRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId: number,
    maximumCount?: number
  ): Promise<number> {
    return this.countQuery(
      {
        TableName: this.recipientIndexTableName,
        KeyConditionExpression:
          'connectionId = :connectionId AND recipientMessageId BETWEEN :recipientIndexStart AND :recipientIndexEnd',
        ExpressionAttributeValues: {
          ':connectionId': { S: connectionId },
          ...this.recipientIndexRange(recipientDid, cutoverMessageId),
        },
      },
      maximumCount
    )
  }

  private async countLegacyRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId?: number,
    maximumCount?: number
  ): Promise<number> {
    const params: QueryCommandInput = {
      TableName: this.tableName,
      KeyConditionExpression:
        cutoverMessageId === undefined
          ? 'connectionId = :connectionId'
          : 'connectionId = :connectionId AND messageId <= :cutoverMessageId',
      FilterExpression: 'contains(recipientDids, :recipientDid)',
      ExpressionAttributeValues: {
        ':connectionId': { S: connectionId },
        ':recipientDid': { S: recipientDid },
        ...(cutoverMessageId === undefined ? {} : { ':cutoverMessageId': { N: String(cutoverMessageId) } }),
      },
    }
    return this.countQuery(params, maximumCount)
  }

  private async countQuery(params: QueryCommandInput, maximumCount?: number): Promise<number> {
    let count = 0
    let lastEvaluatedKey: QueryCommandInput['ExclusiveStartKey']
    do {
      const previousLastEvaluatedKey = lastEvaluatedKey
      const response = await this.dynamodbClient.send(
        new QueryCommand({
          ...params,
          Select: 'COUNT',
          // DynamoDB applies Limit before FilterExpression. Limiting a sparse
          // legacy recipient query to (for example) 10 evaluated items would
          // create many tiny pages, so filtered queries retain full pages.
          Limit: maximumCount === undefined || params.FilterExpression !== undefined ? undefined : maximumCount - count,
          ExclusiveStartKey: lastEvaluatedKey,
        })
      )
      count += response.Count || 0
      lastEvaluatedKey = response.LastEvaluatedKey
      if (maximumCount !== undefined && count >= maximumCount) return maximumCount
      this.assertPaginationAdvanced(previousLastEvaluatedKey, lastEvaluatedKey, params.TableName)
    } while (lastEvaluatedKey)
    return count
  }

  async getMessages(options: {
    connectionId: string
    limit?: number
    recipientDid?: string
    deleteMessages?: boolean
  }) {
    const messages =
      options.recipientDid === undefined
        ? await this.getMessagesByConnection(options.connectionId, options.limit)
        : await this.getRecipientMessages(options.connectionId, options.recipientDid, options.limit)

    if (options.deleteMessages && messages.length > 0) {
      await this.removeMessages({
        connectionId: options.connectionId,
        messageIds: messages.map((message) => message.id),
      })
    }
    return messages
  }

  private async getMessagesByConnection(connectionId: string, limit?: number): Promise<QueuedMessage[]> {
    const response = await this.dynamodbClient.send(
      new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: 'connectionId = :connectionId',
        ExpressionAttributeValues: { ':connectionId': { S: connectionId } },
        Limit: limit,
      })
    )
    return this.toQueuedMessages(response.Items)
  }

  private async getRecipientMessages(
    connectionId: string,
    recipientDid: string,
    limit?: number
  ): Promise<QueuedMessage[]> {
    const metadata = await this.getRecipientIndexMetadata(connectionId)
    if (!metadata) return await this.getLegacyRecipientMessages(connectionId, recipientDid, undefined, limit)

    // Old messages sort before the post-cutover indexed entries, so read them
    // first to retain the queue's FIFO ordering while they drain.
    const legacy = await this.getLegacyRecipientMessages(connectionId, recipientDid, metadata.cutoverMessageId, limit)
    if (limit !== undefined && legacy.length >= limit) return legacy

    const indexed = await this.getIndexedRecipientMessages(
      connectionId,
      recipientDid,
      metadata.cutoverMessageId,
      limit === undefined ? undefined : limit - legacy.length
    )
    return [...legacy, ...indexed]
  }

  private async getLegacyRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId?: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    const queryParams: QueryCommandInput = {
      TableName: this.tableName,
      KeyConditionExpression:
        cutoverMessageId === undefined
          ? 'connectionId = :connectionId'
          : 'connectionId = :connectionId AND messageId <= :cutoverMessageId',
      FilterExpression: 'contains(recipientDids, :recipientDid)',
      ExpressionAttributeValues: {
        ':connectionId': { S: connectionId },
        ':recipientDid': { S: recipientDid },
        ...(cutoverMessageId === undefined ? {} : { ':cutoverMessageId': { N: String(cutoverMessageId) } }),
      },
    }
    return this.queryMessages(queryParams, limit)
  }

  private async getIndexedRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    const indexEntries = await this.queryRecipientIndex(connectionId, recipientDid, cutoverMessageId, limit)
    if (indexEntries.length === 0) return []
    const messages = await this.batchGetMessages(
      connectionId,
      indexEntries.map((entry) => entry.messageId),
      // The index and canonical tables replicate independently. Once an index
      // pointer is visible, use a strong read so its payload cannot be missed
      // transiently with no fallback and disagree with the indexed count.
      true
    )
    const byId = new Map(messages.map((message) => [message.id, message]))
    return indexEntries.flatMap((entry) => {
      const message = byId.get(entry.messageId)
      return message ? [message] : []
    })
  }

  private async queryRecipientIndex(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId: number,
    limit?: number
  ): Promise<RecipientIndexEntry[]> {
    const entries: RecipientIndexEntry[] = []
    let lastEvaluatedKey: QueryCommandInput['ExclusiveStartKey']
    do {
      const previousLastEvaluatedKey = lastEvaluatedKey
      const response = await this.dynamodbClient.send(
        new QueryCommand({
          TableName: this.recipientIndexTableName,
          KeyConditionExpression:
            'connectionId = :connectionId AND recipientMessageId BETWEEN :recipientIndexStart AND :recipientIndexEnd',
          ExpressionAttributeValues: {
            ':connectionId': { S: connectionId },
            ...this.recipientIndexRange(recipientDid, cutoverMessageId),
          },
          ProjectionExpression: 'messageId',
          Limit: limit === undefined ? undefined : limit - entries.length,
          ExclusiveStartKey: lastEvaluatedKey,
        })
      )
      entries.push(...(response.Items ?? []).map((item) => ({ messageId: unmarshall(item).messageId.toString() })))
      lastEvaluatedKey = response.LastEvaluatedKey
      if (lastEvaluatedKey && (limit === undefined || entries.length < limit)) {
        this.assertPaginationAdvanced(previousLastEvaluatedKey, lastEvaluatedKey, this.recipientIndexTableName)
      }
    } while (lastEvaluatedKey && (limit === undefined || entries.length < limit))
    return entries
  }

  private async queryMessages(params: QueryCommandInput, limit?: number): Promise<QueuedMessage[]> {
    const messages: QueuedMessage[] = []
    let lastEvaluatedKey: QueryCommandInput['ExclusiveStartKey']
    do {
      const previousLastEvaluatedKey = lastEvaluatedKey
      const response = await this.dynamodbClient.send(
        new QueryCommand({ ...params, ExclusiveStartKey: lastEvaluatedKey })
      )
      messages.push(...this.toQueuedMessages(response.Items))
      lastEvaluatedKey = response.LastEvaluatedKey
      if (lastEvaluatedKey && (limit === undefined || messages.length < limit)) {
        this.assertPaginationAdvanced(previousLastEvaluatedKey, lastEvaluatedKey, params.TableName)
      }
    } while (lastEvaluatedKey && (limit === undefined || messages.length < limit))
    return limit === undefined ? messages : messages.slice(0, limit)
  }

  private toQueuedMessages(items: Record<string, unknown>[] | undefined): QueuedMessage[] {
    return (items ?? []).map((item) => {
      const message = unmarshall(item as never) as unknown as Record<string, unknown>
      return {
        ...message,
        receivedAt: new Date(message.receivedAt as number),
        id: String(message.messageId),
      } as QueuedMessage
    })
  }

  private async batchGetMessages(
    connectionId: string,
    messageIds: string[],
    consistentRead = false
  ): Promise<QueuedMessage[]> {
    const batches: Array<Promise<QueuedMessage[]>> = []
    for (let index = 0; index < messageIds.length; index += 100) {
      batches.push(this.batchGetMessageChunk(connectionId, messageIds.slice(index, index + 100), consistentRead))
    }

    // Start every chunk before waiting so a throttled chunk cannot serialize
    // unrelated reads. Await all of them before propagating the first failure.
    const results = await Promise.allSettled(batches)
    const failedResult = results.find((result) => result.status === 'rejected')
    if (failedResult) throw failedResult.reason
    return results.flatMap((result) => (result.status === 'fulfilled' ? result.value : []))
  }

  private async batchGetMessageChunk(
    connectionId: string,
    messageIds: string[],
    consistentRead: boolean
  ): Promise<QueuedMessage[]> {
    const messages: QueuedMessage[] = []
    const keys = messageIds.map((messageId) => marshall({ connectionId, messageId: Number(messageId) }))
    let requestItems = { [this.tableName]: { Keys: keys, ConsistentRead: consistentRead || undefined } }
    let retryAttempt = 0
    while (true) {
      const response = await this.dynamodbClient.send(new BatchGetItemCommand({ RequestItems: requestItems }))
      messages.push(...this.toQueuedMessages(response.Responses?.[this.tableName]))
      const unprocessedKeys = response.UnprocessedKeys?.[this.tableName]?.Keys
      if (!unprocessedKeys || unprocessedKeys.length === 0) return messages
      await this.waitBeforeRetry('BatchGetItem', retryAttempt++)
      requestItems = {
        [this.tableName]: { Keys: unprocessedKeys, ConsistentRead: consistentRead || undefined },
      }
    }
  }

  async addMessage(options: AddQueuedMessageOptions): Promise<string> {
    const metadata = await this.prepareRecipientIndex(options.connectionId)
    const randomizer = randomInt(0, 999).toString().padStart(3, '0')
    const receivedAt = options.receivedAt ?? new Date()
    const messageId = `${receivedAt.getTime()}${randomizer}`
    const messageUpdate = {
      TableName: this.tableName,
      Key: marshall({ connectionId: options.connectionId, messageId: Number(messageId) }),
      UpdateExpression: 'set encryptedMessage = :em, recipientDids = :rd, receivedAt = :ra',
      ExpressionAttributeValues: marshall({
        ':em': options.encryptedMessage,
        ':rd': options.recipientDids,
        ':ra': receivedAt.getTime(),
      }),
    }

    // Index entries only cover messages after the cutover. A caller may supply
    // an old receivedAt in tests or a replay, in which case the legacy path
    // remains authoritative and preserves correctness.
    if (Number(messageId) > metadata.cutoverMessageId) {
      const recipientDids = [...new Set(options.recipientDids)]
      // DynamoDB transactions support at most 100 actions. Reserve one for the
      // canonical message and fail before writing anything if this is exceeded.
      if (recipientDids.length > 99) {
        throw new Error('A queued message cannot have more than 99 unique recipient DIDs')
      }

      // Commit the canonical item and all recipient pointers atomically. A
      // failed index write can therefore never leave a durable message that is
      // newer than the cutover but invisible to recipient-scoped reads.
      await this.transactWriteItemsWithRetry({
        ClientRequestToken: createHash('sha256')
          .update(`${options.connectionId}:${messageId}`)
          .digest('hex')
          .slice(0, 36),
        TransactItems: [
          { Update: messageUpdate },
          ...recipientDids.map((recipientDid) => ({
            Put: {
              TableName: this.recipientIndexTableName,
              Item: marshall({
                connectionId: options.connectionId,
                recipientMessageId: this.recipientIndexSortKey(recipientDid, messageId),
                messageId: Number(messageId),
              }),
            },
          })),
        ],
      })
    } else {
      await this.dynamodbClient.send(new UpdateItemCommand(messageUpdate))
    }
    return messageId
  }

  async removeMessages(options: RemoveQueuedMessageOptions): Promise<void> {
    const messageIds = [...new Set(options.messageIds)]
    if (messageIds.length === 0) return

    // Removal needs current recipient DIDs and the immutable cutover boundary;
    // both reads are correctness-sensitive, unlike ordinary count/pickup reads.
    const [messages, metadata] = await Promise.all([
      this.batchGetMessages(options.connectionId, messageIds, true),
      this.getRecipientIndexMetadata(options.connectionId, true),
    ])

    const transactionBatches: TransactWriteItem[][] = []
    let currentBatch: TransactWriteItem[] = []
    for (const message of messages) {
      const isIndexedMessage = metadata !== undefined && Number(message.id) > metadata.cutoverMessageId
      const messageDeletes: TransactWriteItem[] = [
        ...(isIndexedMessage
          ? [...new Set(message.recipientDids)].map((recipientDid) => ({
              Delete: {
                TableName: this.recipientIndexTableName,
                Key: marshall({
                  connectionId: options.connectionId,
                  recipientMessageId: this.recipientIndexSortKey(recipientDid, message.id),
                }),
              },
            }))
          : []),
        {
          Delete: {
            TableName: this.tableName,
            Key: marshall({ connectionId: options.connectionId, messageId: Number(message.id) }),
          },
        },
      ]

      // New indexed messages are limited to 99 unique recipients at enqueue,
      // leaving one transaction action for the canonical row.
      if (messageDeletes.length > 100) {
        throw new Error(`Queued message ${message.id} cannot be deleted atomically because it has too many recipients`)
      }
      if (currentBatch.length > 0 && currentBatch.length + messageDeletes.length > 100) {
        transactionBatches.push(currentBatch)
        currentBatch = []
      }
      currentBatch.push(...messageDeletes)
    }
    if (currentBatch.length > 0) transactionBatches.push(currentBatch)

    // Each message's canonical row and all recipient pointers share a DynamoDB
    // transaction, so a failed chunk leaves every message in that chunk fully
    // visible. Independent chunks are all attempted before an error propagates.
    const results = await Promise.allSettled(
      transactionBatches.map((transactItems) =>
        this.transactWriteItemsWithRetry({
          ClientRequestToken: createHash('sha256')
            .update(`remove:${JSON.stringify(transactItems)}`)
            .digest('hex')
            .slice(0, 36),
          TransactItems: transactItems,
        })
      )
    )
    const failedResult = results.find((result) => result.status === 'rejected')
    if (failedResult) throw failedResult.reason
  }

  private async transactWriteItemsWithRetry(input: TransactWriteItemsCommandInput): Promise<void> {
    let retryAttempt = 0
    while (true) {
      try {
        await this.dynamodbClient.send(new TransactWriteItemsCommand(input))
        return
      } catch (error) {
        if (!this.isRetryableTransactionError(error)) throw error
        await this.waitBeforeRetry('TransactWriteItems', retryAttempt++, error)
      }
    }
  }

  private isRetryableTransactionError(error: unknown): boolean {
    if (!(error instanceof Error)) return false
    if (
      error.name === 'ProvisionedThroughputExceededException' ||
      error.name === 'ThrottlingException' ||
      error.name === 'RequestLimitExceeded' ||
      error.name === 'TransactionConflictException' ||
      error.name === 'TransactionInProgressException' ||
      error.name === 'InternalServerError'
    ) {
      return true
    }
    if (error.name !== 'TransactionCanceledException') return false

    const cancellationReasons = (error as Error & { CancellationReasons?: Array<{ Code?: string }> })
      .CancellationReasons
    const reasonCodes = (cancellationReasons ?? [])
      .map((reason) => reason.Code)
      .filter((code): code is string => code !== undefined && code !== 'None')

    // AWS documents that cancellation reasons may be absent outside Java. Use
    // codes when available (or embedded in the message), but treat a reasonless
    // cancellation as transient for these bounded, idempotent transactions.
    const reportedCodes =
      reasonCodes.length > 0
        ? reasonCodes
        : [...RETRYABLE_TRANSACTION_CANCELLATION_CODES, ...NON_RETRYABLE_TRANSACTION_CANCELLATION_CODES].filter(
            (code) => error.message.includes(code)
          )
    if (reportedCodes.some((code) => NON_RETRYABLE_TRANSACTION_CANCELLATION_CODES.has(code))) return false
    return (
      reportedCodes.length === 0 || reportedCodes.every((code) => RETRYABLE_TRANSACTION_CANCELLATION_CODES.has(code))
    )
  }

  private async waitBeforeRetry(operation: string, retryAttempt: number, cause?: unknown): Promise<void> {
    if (retryAttempt >= MAX_OPERATION_RETRIES) {
      this.logger.error(`${operation} still failed after retries`, {
        retryAttempts: retryAttempt + 1,
        error: cause,
      })
      const retryError = new Error(`${operation} exceeded ${MAX_OPERATION_RETRIES} retries`) as Error & {
        cause?: unknown
      }
      retryError.cause = cause
      throw retryError
    }

    const exponentialDelay = Math.min(INITIAL_RETRY_DELAY_MS * 2 ** retryAttempt, MAX_RETRY_DELAY_MS)
    // Equal jitter provides a growing delay while avoiding synchronized retry
    // storms from mediators that were throttled at the same time.
    const delayMs = Math.floor(exponentialDelay / 2 + Math.random() * (exponentialDelay / 2))
    await new Promise<void>((resolve) => setTimeout(resolve, delayMs))
  }

  private assertPaginationAdvanced(
    previousKey: QueryCommandInput['ExclusiveStartKey'],
    currentKey: QueryCommandInput['ExclusiveStartKey'],
    tableName: string | undefined
  ): void {
    if (previousKey && currentKey && JSON.stringify(previousKey) === JSON.stringify(currentKey)) {
      throw new Error(`DynamoDB returned a non-advancing pagination key while querying ${tableName ?? 'a table'}`)
    }
  }
}
