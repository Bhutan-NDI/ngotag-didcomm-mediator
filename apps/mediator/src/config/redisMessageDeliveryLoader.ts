import { randomUUID } from 'node:crypto'
import { DidCommMessageForwardingStrategy, DidCommMessagePickupSessionRole } from '@credo-ts/didcomm'
import Redis from 'ioredis'
import type { MediatorAgent } from '../agent.js'
import { config } from '../config.js'
import { DidcommMessageQueuedEvent, MediatorEventTypes } from '../events.js'
import { KeyedSingleFlight } from '../message-delivery/KeyedSingleFlight.js'
import { settleWithin } from '../message-delivery/settleWithin.js'
import { RedisStreamMessagePublishing } from '../multi-instance/redis-stream-message-publishing/redisStreamMessagePublishing.js'
import { sendNotification } from '../push-notifications/sendNotification.js'

// Match the Redis pending-message failover window. A timed-out delivery remains
// serialized by KeyedSingleFlight; only the push fallback proceeds.
const LOCAL_DELIVERY_TIMEOUT_MS = 60_000

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

  const localDelivery = new KeyedSingleFlight(async (connectionId: string): Promise<boolean> => {
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
  })

  const scheduleLocalDelivery = async (connectionId: string) => {
    const delivery = localDelivery.schedule(connectionId)

    // One owner handles the result and fallback for each coalesced run. Other
    // callers can return because the owner drains every queued message for the
    // connection and performs the single required fallback side effect.
    if (!delivery.isOwner) return

    // A follow-up run can wait behind an active delivery. Start its timeout only
    // after the run begins, not while it is waiting for the per-key lock.
    await delivery.started
    return await settleWithin(delivery.result, LOCAL_DELIVERY_TIMEOUT_MS)
  }

  agent.events.on<DidcommMessageQueuedEvent>(MediatorEventTypes.DidCommMessageQueued, async (event) => {
    const connectionId = event.payload.connectionId

    agent.config.logger.debug(
      `Server ${streamPublishing.serverId} received DidCommMessageQuedEvent for connection ${connectionId}`
    )

    if (config.messagePickup.forwardingStrategy !== DidCommMessageForwardingStrategy.DirectDelivery) {
      const delivery = await scheduleLocalDelivery(connectionId)

      if (!delivery) return

      if (delivery.status === 'completed' && delivery.value) {
        return
      }

      if (delivery.status === 'timed-out') {
        agent.config.logger.debug('Local queued-message delivery timed out. Falling back to push notification.', {
          connectionId,
        })
        await sendNotification(agent.context, connectionId)
        return
      }
    }

    // Try finding another server to send the message to
    const serverId = await streamPublishing.getConnectionServer(connectionId)

    if (serverId) {
      // Special case. Usually this won't happen because then the above code would already have
      // handled it. So it means the session was closed, but not removed from redis. We remove it
      // and will send a push notification
      if (serverId === streamPublishing.serverId) {
        agent.config.logger.debug(
          `Found own server '${serverId}' in redis for connection '${connectionId}'. Unregistering connection from redis, since we already tried sending to local session.`
        )

        await streamPublishing.unregisterConnection(connectionId).catch(() => {})
      } else {
        try {
          agent.config.logger.debug(
            `Found server '${serverId}' in redis for connection '${connectionId}'. Sending message to server over redis stream.`
          )

          await streamPublishing.sendMessageToServer(serverId, {
            connectionId,
          })
          return
        } catch {
          // If it fails, we will just send a push notification
          agent.config.logger.debug(
            `Error sending message to server '${serverId}' for connection '${connectionId}'. Falling back to push notification sending`
          )
        }
      }
    } else {
      agent.config.logger.debug(`No server with active delivery session found for connection ${connectionId}`)
    }

    await sendNotification(agent.context, connectionId)
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
    { signal: abortSignal }
  )

  // First we want to try to send the message to an open socket connection. If that's not possible, we will emit a push notification.
  void streamPublishing.listenForMessages(
    async (message) => {
      agent.config.logger.debug(
        `Server '${streamPublishing.serverId}' received message ${message.id} for connection '${message.payload.connectionId}'. Attempting to deliver to local session.`
      )

      const delivery = await scheduleLocalDelivery(message.payload.connectionId)

      if (!delivery) return

      if (delivery.status === 'completed' && delivery.value) {
        // We delivered the messages, so no push notification is needed. If
        // several stream entries target this connection, they share this
        // flight and at most one follow-up queue drain is scheduled.
        return
      }

      if (delivery.status === 'timed-out') {
        agent.config.logger.debug(
          `Local delivery timed out for connection '${message.payload.connectionId}'. Falling back to push notification without starting another delivery attempt.`
        )
        await sendNotification(agent.context, message.payload.connectionId)
        return
      }

      agent.config.logger.debug(
        `No usable local session found for connection '${message.payload.connectionId}'. Falling back to push notification.`
      )
      await sendNotification(agent.context, message.payload.connectionId)
    },
    { signal: abortSignal }
  )
}
