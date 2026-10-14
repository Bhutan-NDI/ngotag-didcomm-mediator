import { Agent, AgentContext } from '@credo-ts/core'
import { DidCommQueueTransportRepository } from '@credo-ts/didcomm'
import { DidCommTransportQueueDynamoDb } from '@credo-ts/didcomm-transport-queue-dynamodb'
import { DidCommTransportQueuePostgres } from '@credo-ts/didcomm-transport-queue-postgres'
import type Redis from 'ioredis'
import { config, logger } from '../config.js'
import { RedisMessageClaimLock } from '../storage/MessageClaimLock.js'
import { StorageServiceMessageQueue } from '../storage/StorageMessageQueue.js'

export interface ExtendedQueueTransportRepository extends DidCommQueueTransportRepository {
  initialize?: (agent: Agent) => Promise<void>
  getPoolStats?: () => { total: number; idle: number; waiting: number } | null
  shutdown?: (agentContext: AgentContext) => Promise<void>
}

export async function loadMessagePickupStorage({
  redisClient,
}: {
  redisClient?: Redis.default
} = {}): Promise<ExtendedQueueTransportRepository> {
  const { storage, claimTtlMs, multiInstanceDelivery } = config.messagePickup

  if (storage.type === 'dynamodb') {
    logger.info('Using dynamodb message pickup storage')
    return await DidCommTransportQueueDynamoDb.initialize({
      // Endpoint is not needed when deploying to AWS, but for local development it can be useful
      endpoint: storage.endpoint,
      logger,
      region: storage.region,
      tableName: storage.tableName,
      claimTtlMs,
      recipientIndexTableName: storage.recipientIndexTableName,
      credentials: {
        accessKeyId: storage.accessKeyId,
        secretAccessKey: storage.secretAccessKey,
      },
    })
  }

  if (storage.type === 'postgres') {
    logger.info('Using postgres message pickup storage')
    return new DidCommTransportQueuePostgres({
      postgresHost: storage.host,
      postgresUser: storage.user,
      postgresPassword: storage.password,
      postgresDatabaseName: storage.database,
      logger,
    })
  }

  logger.info('Using credo message pickup storage')
  // Several instances share this storage, so their claims need a lock they can all see.
  const claimLock =
    multiInstanceDelivery.type === 'redis' && redisClient ? new RedisMessageClaimLock(redisClient) : undefined
  return new StorageServiceMessageQueue({ claimTtlMs, claimLock })
}
