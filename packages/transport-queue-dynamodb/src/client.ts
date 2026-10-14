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
import { assertValidClaimTtl, DEFAULT_MESSAGE_CLAIM_TTL_MS, isClaimed } from './claim.js'
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
  telemetry?: Record<string, string>
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
  /** Lease for a taken but unacknowledged message. @default 30000 */
  claimTtlMs?: number
  logger: Logger
}

type RecipientIndexMetadata = { cutoverMessageId: number }
type RecipientIndexEntry = { messageId: string }

// Matches messages no delivery currently holds. Needs ':now' in the expression values.
const UNCLAIMED_FILTER = '(attribute_not_exists(claimedUntil) OR claimedUntil <= :now)'
// Claim writes in flight per take, so a large take cannot exhaust the table's write capacity at once.
const CLAIM_CONCURRENCY = 10
// Minimum page size for take queries on the canonical table.
const TAKE_PAGE_SIZE = 100

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
  private claimTtlMs: number
  private logger: Logger

  private constructor(options: DynamoDbClientRepositoryOptions) {
    const { claimTtlMs = DEFAULT_MESSAGE_CLAIM_TTL_MS, ...clientOptions } = options
    assertValidClaimTtl(claimTtlMs)
    this.dynamodbClient = new DynamoDBClient(clientOptions)
    this.tableName = options.tableName ?? 'queued_messages'
    this.recipientIndexTableName = options.recipientIndexTableName ?? `${this.tableName}_recipient_index`
    this.claimTtlMs = claimTtlMs
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

  public shutdown(): void {
    this.dynamodbClient.destroy()
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

  private async getRecipientIndexMetadataForRead(connectionId: string): Promise<RecipientIndexMetadata | undefined> {
    try {
      return await this.getRecipientIndexMetadata(connectionId)
    } catch (error) {
      // The canonical queue table contains both legacy and indexed messages,
      // so recipient-scoped reads can safely use its filtered query when the
      // optional acceleration table is temporarily unavailable. Keep writes
      // and deletes fail-closed because their metadata reads protect the
      // cutover boundary and atomic index maintenance.
      this.logger.warn('Unable to read recipient index metadata; falling back to the canonical queue table', {
        connectionId,
        error,
      })
      return undefined
    }
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

  /** Counts messages that are not claimed by an in-flight delivery. */
  async getMessageCount(connectionId: string, recipientDid?: string, maximumCount?: number): Promise<number> {
    const now = Date.now()
    try {
      if (recipientDid === undefined) return await this.countMessagesByConnection(connectionId, now, maximumCount)

      const metadata = await this.getRecipientIndexMetadataForRead(connectionId)
      if (!metadata) {
        return await this.countLegacyRecipientMessages(connectionId, recipientDid, now, undefined, maximumCount)
      }

      const indexed = await this.countIndexedRecipientMessages(
        connectionId,
        recipientDid,
        metadata.cutoverMessageId,
        now,
        maximumCount
      )
      if (maximumCount !== undefined && indexed >= maximumCount) return maximumCount
      const legacy = await this.countLegacyRecipientMessages(
        connectionId,
        recipientDid,
        now,
        metadata.cutoverMessageId,
        maximumCount === undefined ? undefined : maximumCount - indexed
      )
      return indexed + legacy
    } catch (error) {
      this.logger.error('Error getting entries count:', { error })
      throw error
    }
  }

  private async countMessagesByConnection(connectionId: string, now: number, maximumCount?: number): Promise<number> {
    return this.countQuery(
      {
        TableName: this.tableName,
        KeyConditionExpression: 'connectionId = :connectionId',
        FilterExpression: UNCLAIMED_FILTER,
        ExpressionAttributeValues: { ':connectionId': { S: connectionId }, ':now': { N: String(now) } },
      },
      maximumCount
    )
  }

  private async countIndexedRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId: number,
    now: number,
    maximumCount?: number
  ): Promise<number> {
    return this.countQuery(
      {
        TableName: this.recipientIndexTableName,
        KeyConditionExpression:
          'connectionId = :connectionId AND recipientMessageId BETWEEN :recipientIndexStart AND :recipientIndexEnd',
        FilterExpression: UNCLAIMED_FILTER,
        ExpressionAttributeValues: {
          ':connectionId': { S: connectionId },
          ':now': { N: String(now) },
          ...this.recipientIndexRange(recipientDid, cutoverMessageId),
        },
      },
      maximumCount
    )
  }

  private async countLegacyRecipientMessages(
    connectionId: string,
    recipientDid: string,
    now: number,
    cutoverMessageId?: number,
    maximumCount?: number
  ): Promise<number> {
    return this.countQuery(this.legacyRecipientQuery(connectionId, recipientDid, now, cutoverMessageId), maximumCount)
  }

  private legacyRecipientQuery(
    connectionId: string,
    recipientDid: string,
    now: number,
    cutoverMessageId?: number
  ): QueryCommandInput {
    return {
      TableName: this.tableName,
      KeyConditionExpression:
        cutoverMessageId === undefined
          ? 'connectionId = :connectionId'
          : 'connectionId = :connectionId AND messageId <= :cutoverMessageId',
      FilterExpression: `contains(recipientDids, :recipientDid) AND ${UNCLAIMED_FILTER}`,
      ExpressionAttributeValues: {
        ':connectionId': { S: connectionId },
        ':recipientDid': { S: recipientDid },
        ':now': { N: String(now) },
        ...(cutoverMessageId === undefined ? {} : { ':cutoverMessageId': { N: String(cutoverMessageId) } }),
      },
    }
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
          // No Limit: DynamoDB applies it before the filter, so claimed or non-matching items would shrink each
          // page. COUNT returns no items, so full pages are cheap; the loop stops once maximumCount is reached.
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
    // Pickup v1 claims too, so it cannot return (and delete) messages a v2 delivery currently holds.
    const messages = await this.claimMessages(options)
    if (!options.deleteMessages) return messages

    if (messages.length > 0) {
      try {
        await this.removeMessages({
          connectionId: options.connectionId,
          messageIds: messages.map((message) => message.id),
        })
      } catch (error) {
        // Some independent transactions may already have committed by the
        // time another transaction fails. Returning every fetched message
        // preserves at-least-once delivery for both known and ambiguous
        // transaction outcomes; any messages that remain queued may be
        // delivered again, but committed deletes cannot cause message loss.
        this.logger.error('Error removing fetched messages; returning them to preserve at-least-once delivery:', {
          error,
          connectionId: options.connectionId,
          messageIds: messages.map((message) => message.id),
        })
      }
    }
    return messages
  }

  /**
   * Claims each returned message with a conditional write, so overlapping takes on any instance never return the
   * same message. An unacknowledged claim lapses after claimTtlMs and the message is delivered again.
   */
  private async claimMessages(options: {
    connectionId: string
    limit?: number
    recipientDid?: string
  }): Promise<QueuedMessage[]> {
    const { connectionId, recipientDid, limit } = options
    // Strongly consistent and fail-closed: whether a claim is mirrored onto the recipient index depends on it.
    const metadata = await this.getRecipientIndexMetadata(connectionId, true)
    const claimed: Array<{ message: QueuedMessage; claimedUntil: number }> = []
    // Guards against re-attempting a message, so a lost race cannot loop.
    const attempted = new Set<string>()

    while (limit === undefined || claimed.length < limit) {
      const now = Date.now()
      const wanted = limit === undefined ? undefined : limit - claimed.length
      const found = await this.findUnclaimedMessages(connectionId, recipientDid, metadata, now, wanted)
      const candidates = found.filter((message) => !attempted.has(message.id) && !isClaimed(message.claimedUntil, now))
      if (candidates.length === 0) break

      const claimedUntil = now + this.claimTtlMs
      for (const message of candidates) attempted.add(message.id)
      const results = await this.settleWithConcurrency(candidates, (message) =>
        this.claimMessage(connectionId, message, metadata, now, claimedUntil)
      )
      candidates.forEach((message, index) => {
        const result = results[index]
        if (result.status === 'fulfilled' && result.value) claimed.push({ message, claimedUntil })
      })

      const failure = results.find((result) => result.status === 'rejected')
      if (failure) {
        // Release what this take won, so a failed take does not hide those messages for a whole lease.
        await this.releaseClaims(connectionId, claimed, metadata)
        throw failure.reason
      }

      // Fewer than requested means the read reached the end of the queue.
      if (wanted === undefined || found.length < wanted) break
    }

    return claimed.map(({ message: { claimedUntil: _claimedUntil, ...message } }) => message)
  }

  /** Resolves false when another delivery holds the message or it was acknowledged since it was read. */
  private async claimMessage(
    connectionId: string,
    message: QueuedMessage,
    metadata: RecipientIndexMetadata | undefined,
    now: number,
    claimedUntil: number
  ): Promise<boolean> {
    try {
      await this.writeClaim(connectionId, message, metadata, {
        UpdateExpression: 'SET claimedUntil = :claimedUntil',
        // attribute_exists stops the update from recreating an acknowledged message.
        ConditionExpression: `attribute_exists(messageId) AND ${UNCLAIMED_FILTER}`,
        values: { ':claimedUntil': claimedUntil, ':now': now },
      })
      return true
    } catch (error) {
      if (this.isConditionalCheckFailure(error)) return false
      throw error
    }
  }

  private async releaseClaims(
    connectionId: string,
    claims: Array<{ message: QueuedMessage; claimedUntil: number }>,
    metadata: RecipientIndexMetadata | undefined
  ): Promise<void> {
    const results = await this.settleWithConcurrency(claims, ({ message, claimedUntil }) =>
      this.writeClaim(connectionId, message, metadata, {
        UpdateExpression: 'REMOVE claimedUntil',
        // Only release a claim this take still holds.
        ConditionExpression: 'claimedUntil = :claimedUntil',
        values: { ':claimedUntil': claimedUntil },
      })
    )
    for (const result of results) {
      if (result.status === 'rejected' && !this.isConditionalCheckFailure(result.reason)) {
        this.logger.warn('Unable to release a message claim; it lapses after the claim TTL', {
          connectionId,
          error: result.reason,
        })
      }
    }
  }

  /** Updates the canonical item, mirroring the update onto its recipient index entries when it is indexed. */
  private async writeClaim(
    connectionId: string,
    message: QueuedMessage,
    metadata: RecipientIndexMetadata | undefined,
    update: { UpdateExpression: string; ConditionExpression: string; values: Record<string, number> }
  ): Promise<void> {
    const canonicalUpdate = {
      TableName: this.tableName,
      Key: marshall({ connectionId, messageId: Number(message.id) }),
      UpdateExpression: update.UpdateExpression,
      ConditionExpression: update.ConditionExpression,
      ExpressionAttributeValues: marshall(update.values),
    }
    if (!this.isIndexedMessage(message.id, metadata)) {
      await this.sendWithRetry('UpdateItem', () => this.dynamodbClient.send(new UpdateItemCommand(canonicalUpdate)))
      return
    }

    const isRelease = update.UpdateExpression.startsWith('REMOVE')
    await this.transactWriteItemsWithRetry({
      TransactItems: [
        { Update: canonicalUpdate },
        ...[...new Set(message.recipientDids)].map((recipientDid) => ({
          Update: {
            TableName: this.recipientIndexTableName,
            Key: marshall({ connectionId, recipientMessageId: this.recipientIndexSortKey(recipientDid, message.id) }),
            UpdateExpression: update.UpdateExpression,
            ConditionExpression: 'attribute_exists(recipientMessageId)',
            ExpressionAttributeValues: isRelease
              ? undefined
              : marshall({ ':claimedUntil': update.values[':claimedUntil'] }),
          },
        })),
      ],
    })
  }

  private isIndexedMessage(messageId: string, metadata: RecipientIndexMetadata | undefined): boolean {
    return metadata !== undefined && Number(messageId) > metadata.cutoverMessageId
  }

  private isConditionalCheckFailure(error: unknown): boolean {
    if (!(error instanceof Error)) return false
    if (error.name === 'ConditionalCheckFailedException') return true
    if (error.name !== 'TransactionCanceledException') return false
    const reasons = (error as Error & { CancellationReasons?: Array<{ Code?: string }> }).CancellationReasons
    return reasons
      ? reasons.some((reason) => reason.Code === 'ConditionalCheckFailed')
      : error.message.includes('ConditionalCheckFailed')
  }

  /** Runs task for every item with at most CLAIM_CONCURRENCY in flight, settling all of them. */
  private async settleWithConcurrency<T, R>(
    items: T[],
    task: (item: T) => Promise<R>
  ): Promise<PromiseSettledResult<R>[]> {
    const results: PromiseSettledResult<R>[] = new Array(items.length)
    let next = 0
    const worker = async () => {
      while (next < items.length) {
        const index = next++
        try {
          results[index] = { status: 'fulfilled', value: await task(items[index]) }
        } catch (reason) {
          results[index] = { status: 'rejected', reason }
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(CLAIM_CONCURRENCY, items.length) }, worker))
    return results
  }

  // Strongly consistent reads, so a message claimed by another take is never offered again.
  private async findUnclaimedMessages(
    connectionId: string,
    recipientDid: string | undefined,
    metadata: RecipientIndexMetadata | undefined,
    now: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    return recipientDid === undefined
      ? await this.getMessagesByConnection(connectionId, now, limit)
      : await this.getRecipientMessages(connectionId, recipientDid, metadata, now, limit)
  }

  private async getMessagesByConnection(connectionId: string, now: number, limit?: number): Promise<QueuedMessage[]> {
    return this.queryMessages(
      {
        TableName: this.tableName,
        KeyConditionExpression: 'connectionId = :connectionId',
        FilterExpression: UNCLAIMED_FILTER,
        ExpressionAttributeValues: { ':connectionId': { S: connectionId }, ':now': { N: String(now) } },
        ConsistentRead: true,
        // Limit applies before the claim filter, so a page stays large enough to get past claimed messages
        // quickly, while still bounding how many payloads one page transfers.
        Limit: limit === undefined ? undefined : Math.max(limit, TAKE_PAGE_SIZE),
      },
      limit
    )
  }

  private async getRecipientMessages(
    connectionId: string,
    recipientDid: string,
    metadata: RecipientIndexMetadata | undefined,
    now: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    if (!metadata) return await this.getLegacyRecipientMessages(connectionId, recipientDid, now, undefined, limit)

    // Old messages sort before the post-cutover indexed entries, so read them
    // first to retain the queue's FIFO ordering while they drain.
    const legacy = await this.getLegacyRecipientMessages(
      connectionId,
      recipientDid,
      now,
      metadata.cutoverMessageId,
      limit
    )
    if (limit !== undefined && legacy.length >= limit) return legacy

    const indexed = await this.getIndexedRecipientMessages(
      connectionId,
      recipientDid,
      metadata.cutoverMessageId,
      now,
      limit === undefined ? undefined : limit - legacy.length
    )
    return [...legacy, ...indexed]
  }

  private async getLegacyRecipientMessages(
    connectionId: string,
    recipientDid: string,
    now: number,
    cutoverMessageId?: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    return this.queryMessages(
      { ...this.legacyRecipientQuery(connectionId, recipientDid, now, cutoverMessageId), ConsistentRead: true },
      limit
    )
  }

  private async getIndexedRecipientMessages(
    connectionId: string,
    recipientDid: string,
    cutoverMessageId: number,
    now: number,
    limit?: number
  ): Promise<QueuedMessage[]> {
    const indexEntries = await this.queryRecipientIndex(connectionId, recipientDid, cutoverMessageId, now, limit)
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
    now: number,
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
          FilterExpression: UNCLAIMED_FILTER,
          ExpressionAttributeValues: {
            ':connectionId': { S: connectionId },
            ':now': { N: String(now) },
            ...this.recipientIndexRange(recipientDid, cutoverMessageId),
          },
          ProjectionExpression: 'messageId',
          ConsistentRead: true,
          // No Limit: it applies before the claim filter, and index entries are small, so full pages are cheap.
          ExclusiveStartKey: lastEvaluatedKey,
        })
      )
      entries.push(...(response.Items ?? []).map((item) => ({ messageId: unmarshall(item).messageId.toString() })))
      lastEvaluatedKey = response.LastEvaluatedKey
      if (lastEvaluatedKey && (limit === undefined || entries.length < limit)) {
        this.assertPaginationAdvanced(previousLastEvaluatedKey, lastEvaluatedKey, this.recipientIndexTableName)
      }
    } while (lastEvaluatedKey && (limit === undefined || entries.length < limit))
    return limit === undefined ? entries : entries.slice(0, limit)
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
      UpdateExpression: `set encryptedMessage = :em, recipientDids = :rd, receivedAt = :ra${
        options.telemetry ? ', telemetry = :tc' : ''
      }`,
      ExpressionAttributeValues: marshall({
        ':em': options.encryptedMessage,
        ':rd': options.recipientDids,
        ':ra': receivedAt.getTime(),
        ...(options.telemetry ? { ':tc': options.telemetry } : {}),
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
      const messageDeletes: TransactWriteItem[] = [
        ...(this.isIndexedMessage(message.id, metadata)
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
    await this.sendWithRetry('TransactWriteItems', () => this.dynamodbClient.send(new TransactWriteItemsCommand(input)))
  }

  /** Retries throttling and transient transaction failures with capped, jittered backoff. */
  private async sendWithRetry<T>(operation: string, send: () => Promise<T>): Promise<T> {
    let retryAttempt = 0
    while (true) {
      try {
        return await send()
      } catch (error) {
        if (!this.isRetryableTransactionError(error)) throw error
        await this.waitBeforeRetry(operation, retryAttempt++, error)
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
