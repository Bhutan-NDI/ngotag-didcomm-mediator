import type { DeliveryFallbackReason } from './QueuedMessageDeliveryCoordinator.js'

interface FallbackLogger {
  debug(message: string, data?: Record<string, unknown>): void
}

interface MessageRouting {
  getConnectionServer(connectionId: string): Promise<string | null>
  sendMessageToServer(targetServerId: string, data: { connectionId: string }): Promise<void>
  serverId: string
  unregisterConnection(connectionId: string): Promise<void>
}

export async function routeQueuedMessageFallback({
  connectionId,
  logger,
  notify,
  reason,
  routing,
}: {
  connectionId: string
  logger: FallbackLogger
  notify: (connectionId: string) => Promise<void>
  reason?: DeliveryFallbackReason
  routing: MessageRouting
}): Promise<void> {
  if (reason?.status === 'timed-out') {
    logger.debug('Local queued-message delivery timed out. Trying failover before notification.', { connectionId })
  } else if (reason?.status === 'errored') {
    logger.debug('Local queued-message delivery failed. Trying failover before notification.', {
      connectionId,
      error: reason.error,
    })
  }

  const serverId = await routing.getConnectionServer(connectionId)

  if (serverId === routing.serverId) {
    logger.debug(
      `Found own server '${serverId}' in redis for connection '${connectionId}'. Unregistering the stale local connection before sending a push notification.`
    )
    await routing.unregisterConnection(connectionId).catch(() => {})
  } else if (serverId) {
    try {
      logger.debug(
        `Found server '${serverId}' in redis for connection '${connectionId}'. Sending message to server over redis stream.`
      )
      await routing.sendMessageToServer(serverId, { connectionId })
      return
    } catch (error) {
      logger.debug(
        `Error sending message to server '${serverId}' for connection '${connectionId}'. Falling back to push notification.`,
        { error }
      )
    }
  } else {
    logger.debug(`No server with active delivery session found for connection ${connectionId}`)
  }

  await notify(connectionId)
}
