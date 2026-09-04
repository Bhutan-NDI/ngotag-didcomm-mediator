import { randomUUID } from 'node:crypto'
import { DidCommMessageForwardingStrategy, DidCommMessagePickupSessionRole } from '@credo-ts/didcomm'
import Redis from 'ioredis'
import type { MediatorAgent } from '../agent.js'
import { config, configuredMessageForwardingStrategy } from '../config.js'
import { DidcommMessageQueuedEvent, MediatorEventTypes } from '../events.js'
import {
  type DeliveryFallbackReason,
  QueuedMessageDeliveryCoordinator,
} from '../message-delivery/QueuedMessageDeliveryCoordinator.js'
import { routeQueuedMessageFallback } from '../message-delivery/routeQueuedMessageFallback.js'
import { RedisStreamMessagePublishing } from '../multi-instance/redis-stream-message-publishing/redisStreamMessagePublishing.js'
import { sendNotification } from '../push-notifications/sendNotification.js'

// Bound local work from event arrival and leave time for routing/notification
// plus stream acknowledgement before another server may reclaim the entry.
const LOCAL_DELIVERY_TIMEOUT_MS = 45_000
const MESSAGE_HANDLING_TIMEOUT_MS = 55_000
const PENDING_MESSAGE_FAILOVER_MS = 60_000

/**
 * Initialize redis message publishing for queued mediator messages. This message publishing implementation is not
 * tied to a specific transport queue implementation, and can work with any implementation as long as the transport queue
 * implementation does not handle message sending and push notification itself.
 *
 * Currently the following queue transport implementations are supported:
 * - `DynamoDbMessagePickupRepository` (message pickup type=dynamodb)
 * - `StorageServiceMessageQueue` (message pickup type=credo)
 *
 * Currently the following queue transport implementations are not supported:
 * - `PostgresMessagePickupRepository` (message pickup type=postgres). Due to it handling the
 *   multi-instance message delivery in the transport implementation.
 *
 * This will handle:
 * - publishing and handling of queued message delivery between multi-instance deployments
 * - sending of push notifications for queued messages that could not be delivered
 * - automatic failover of messages sent to another server that have never been claimed and acknowledged.
 *    This means that after a minute a push notification will be sent if an error occurred.
 */
export async function loadRedisMessageDelivery({
  abortSignal,
  agent,
  redisClient,
}: {
  abortSignal?: AbortSignal
  agent: MediatorAgent
  redisClient?: Redis.default
}) {
  if (config.cache.type !== 'redis' || config.messagePickup.multiInstanceDelivery.type !== 'redis') return

  agent.config.logger.info('Loading redis multi instance message delivery')

  const client = redisClient ?? new Redis.default(config.cache.redisUrl)

  // We generate a random server instance, it does not really matter as long as it's unique between active servers
  // if a server crashes we lose the active socket connections.
  const streamPublishing = new RedisStreamMessagePublishing(agent, client, randomUUID())

  const routeOrNotify = (connectionId: string, reason?: DeliveryFallbackReason) =>
    routeQueuedMessageFallback({
      connectionId,
      logger: agent.config.logger,
      notify: (fallbackConnectionId) => sendNotification(agent.context, fallbackConnectionId),
      reason,
      routing: streamPublishing,
    })

  const deliveryCoordinator = new QueuedMessageDeliveryCoordinator(
    async (connectionId: string): Promise<boolean> => {
      try {
        const session = await agent.didcomm.messagePickup.getLiveModeSession({
          connectionId,
          role: DidCommMessagePickupSessionRole.MessageHolder,
        })

        if (!session) return false

        agent.config.logger.debug('Found a local session. Delivering messages from queue.', { connectionId })
        await agent.didcomm.messagePickup.deliverMessagesFromQueue({
          pickupSessionId: session.id,
        })
        agent.config.logger.debug('Successfully delivered queued messages to a local session.', { connectionId })
        return true
      } catch (error) {
        agent.config.logger.debug('Unable to deliver queued messages to a local session.', { connectionId, error })
        return false
      }
    },
    routeOrNotify,
    LOCAL_DELIVERY_TIMEOUT_MS,
    MESSAGE_HANDLING_TIMEOUT_MS
  )

  agent.events.on<DidcommMessageQueuedEvent>(MediatorEventTypes.DidCommMessageQueued, async (event) => {
    const connectionId = event.payload.connectionId

    agent.config.logger.debug(
      `Server ${streamPublishing.serverId} received DidCommMessageQuedEvent for connection ${connectionId}`
    )

    if (configuredMessageForwardingStrategy !== DidCommMessageForwardingStrategy.DirectDelivery) {
      await deliveryCoordinator.schedule(connectionId)
      return
    }

    await routeOrNotify(connectionId)
  })

  // We want to send a push notification for all messages that were emitted on the stream but not handled
  // it probably means the socket was closed and thus not correctly handled.
  void streamPublishing.claimPendingMessages(
    async (serverId, message) => {
      agent.config.logger.debug(
        `Server '${streamPublishing.serverId}' claimed pending message ${message.id} from server ${serverId}. Trying to send push notification.`
      )

      await sendNotification(agent.context, message.payload.connectionId)
    },
    { minIdleTimeMs: PENDING_MESSAGE_FAILOVER_MS, signal: abortSignal }
  )

  // First we want to try to send the message to an open socket connection. If that's not possible, we will emit a push notification.
  void streamPublishing.listenForMessages(
    async (message) => {
      agent.config.logger.debug(
        `Server '${streamPublishing.serverId}' received message ${message.id} for connection '${message.payload.connectionId}'. Attempting to deliver to local session.`
      )

      await deliveryCoordinator.schedule(message.payload.connectionId, message.createdAt)
    },
    { signal: abortSignal }
  )
}
