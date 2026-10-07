import { describe, expect, test, vi } from 'vitest'
import { routeQueuedMessageFallback } from './routeQueuedMessageFallback.js'

function dependencies(serverId: string | null) {
  return {
    logger: { debug: vi.fn() },
    notify: vi.fn().mockResolvedValue(undefined),
    routing: {
      getConnectionServer: vi.fn().mockResolvedValue(serverId),
      sendMessageToServer: vi.fn().mockResolvedValue(undefined),
      serverId: 'server-1',
      unregisterConnection: vi.fn().mockResolvedValue(undefined),
    },
  }
}

describe('routeQueuedMessageFallback', () => {
  test('routes to a different registered server before sending a notification', async () => {
    const { logger, notify, routing } = dependencies('server-2')

    await routeQueuedMessageFallback({ connectionId: 'connection-1', logger, notify, routing })

    expect(routing.sendMessageToServer).toHaveBeenCalledWith('server-2', { connectionId: 'connection-1' })
    expect(routing.unregisterConnection).not.toHaveBeenCalled()
    expect(notify).not.toHaveBeenCalled()
  })

  test.each([
    { status: 'unavailable' } as const,
    { status: 'errored', error: new Error('send failed') } as const,
  ])('clears a stale local registration before sending a notification ($status)', async (reason) => {
    const { logger, notify, routing } = dependencies('server-1')

    await routeQueuedMessageFallback({ connectionId: 'connection-1', logger, notify, reason, routing })

    expect(routing.unregisterConnection).toHaveBeenCalledWith('connection-1')
    expect(routing.sendMessageToServer).not.toHaveBeenCalled()
    expect(notify).toHaveBeenCalledWith('connection-1')
  })

  test('keeps the local registration when local delivery only timed out', async () => {
    const { logger, notify, routing } = dependencies('server-1')

    await routeQueuedMessageFallback({
      connectionId: 'connection-1',
      logger,
      notify,
      reason: { status: 'timed-out' },
      routing,
    })

    // A slow drain may still be running on an open socket; removing the key
    // would leave other servers sending only push notifications for it.
    expect(routing.unregisterConnection).not.toHaveBeenCalled()
    expect(routing.sendMessageToServer).not.toHaveBeenCalled()
    expect(notify).toHaveBeenCalledWith('connection-1')
  })

  test('sends a notification when no server is registered', async () => {
    const { logger, notify, routing } = dependencies(null)

    await routeQueuedMessageFallback({ connectionId: 'connection-1', logger, notify, routing })

    expect(routing.sendMessageToServer).not.toHaveBeenCalled()
    expect(notify).toHaveBeenCalledWith('connection-1')
  })

  test('sends a notification when routing to another server fails', async () => {
    const { logger, notify, routing } = dependencies('server-2')
    routing.sendMessageToServer.mockRejectedValue(new Error('redis unavailable'))

    await routeQueuedMessageFallback({ connectionId: 'connection-1', logger, notify, routing })

    expect(notify).toHaveBeenCalledWith('connection-1')
  })
})
