interface QueuedMessageEventLogger {
  error(message: string, data?: Record<string, unknown>): void
}

/**
 * Handle a DidCommMessageQueued event without ever rejecting. Credo's event
 * emitter ignores the promise a listener returns, so a rejection here would be
 * unhandled and stop the process. The message is already persisted, so a
 * failed delivery or notification leaves it queued for the next pickup.
 */
export async function handleQueuedMessageEvent({
  connectionId,
  deliver,
  logger,
}: {
  connectionId: string
  deliver: (connectionId: string) => Promise<void>
  logger: QueuedMessageEventLogger
}): Promise<void> {
  try {
    await deliver(connectionId)
  } catch (error) {
    logger.error('Unable to deliver or notify for a queued message. It remains queued for pickup.', {
      connectionId,
      error,
    })
  }
}
