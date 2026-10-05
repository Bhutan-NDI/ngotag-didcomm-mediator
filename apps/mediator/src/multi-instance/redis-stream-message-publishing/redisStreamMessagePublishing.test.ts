import { DidCommMessagePickupEventTypes } from '@credo-ts/didcomm'
import { describe, expect, test, vi } from 'vitest'
import { RedisStreamMessagePublishing } from './redisStreamMessagePublishing.js'

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })

  return { promise, resolve }
}

describe('RedisStreamMessagePublishing', () => {
  test('waits for live-session registration before the event handler completes', async () => {
    const registration = deferred()
    const handlers = new Map<string, (event: never) => Promise<void>>()
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    }
    const agent = {
      config: { logger },
      context: { config: { logger } },
      events: {
        on: vi.fn((eventType: string, handler: (event: never) => Promise<void>) => {
          handlers.set(eventType, handler)
        }),
      },
    }
    const client = {
      setex: vi.fn().mockImplementation(() => registration.promise),
    }
    new RedisStreamMessagePublishing(agent as never, client as never, 'server-1')

    const savedHandler = handlers.get(DidCommMessagePickupEventTypes.LiveSessionSaved)
    expect(savedHandler).toBeDefined()

    let handlerCompleted = false
    const handling = savedHandler?.({ payload: { session: { connectionId: 'connection-1' } } } as never).then(() => {
      handlerCompleted = true
    })
    await Promise.resolve()

    expect(handlerCompleted).toBe(false)
    expect(client.setex).toHaveBeenCalledWith('connection:connection-1', 3600, 'server-1')

    registration.resolve()
    await handling
    expect(handlerCompleted).toBe(true)
  })

  test('catches a failed live-session registration in the event handler', async () => {
    const registrationError = new Error('redis unavailable')
    const handlers = new Map<string, (event: never) => Promise<void>>()
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    }
    const agent = {
      config: { logger },
      context: { config: { logger } },
      events: {
        on: vi.fn((eventType: string, handler: (event: never) => Promise<void>) => {
          handlers.set(eventType, handler)
        }),
      },
    }
    const client = {
      setex: vi.fn().mockRejectedValue(registrationError),
    }
    new RedisStreamMessagePublishing(agent as never, client as never, 'server-1')

    const savedHandler = handlers.get(DidCommMessagePickupEventTypes.LiveSessionSaved)
    expect(savedHandler).toBeDefined()

    await expect(
      savedHandler?.({ payload: { session: { connectionId: 'connection-1' } } } as never)
    ).resolves.toBeUndefined()
    expect(logger.error).toHaveBeenCalledWith(`Error handling LiveSessionSaved: ${registrationError}`)
  })

  test('processes a read batch concurrently and acknowledges every successful entry', async () => {
    const abortController = new AbortController()
    const bothHandlersStarted = deferred()
    const releaseHandlers = deferred()
    let activeHandlers = 0
    let maximumActiveHandlers = 0

    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    }
    const agent = {
      config: { logger },
      context: { config: { logger } },
      events: { on: vi.fn() },
    }
    const client = {
      xack: vi.fn().mockResolvedValue(1),
      xgroup: vi.fn().mockResolvedValue('OK'),
      xreadgroup: vi.fn().mockResolvedValue([
        [
          'server:server-1:message-publishing',
          [
            ['1-0', ['message', JSON.stringify({ connectionId: 'connection-1' })]],
            ['2-0', ['message', JSON.stringify({ connectionId: 'connection-2' })]],
          ],
        ],
      ]),
    }
    const publishing = new RedisStreamMessagePublishing(agent as never, client as never, 'server-1')

    const listening = publishing.listenForMessages(
      async () => {
        activeHandlers += 1
        maximumActiveHandlers = Math.max(maximumActiveHandlers, activeHandlers)
        if (activeHandlers === 2) {
          abortController.abort()
          bothHandlersStarted.resolve()
        }

        await releaseHandlers.promise
        activeHandlers -= 1
      },
      { signal: abortController.signal }
    )

    await bothHandlersStarted.promise
    expect(maximumActiveHandlers).toBe(2)

    releaseHandlers.resolve()
    await listening

    expect(client.xack).toHaveBeenCalledTimes(2)
    expect(client.xack).toHaveBeenCalledWith('server:server-1:message-publishing', 'default', '1-0')
    expect(client.xack).toHaveBeenCalledWith('server:server-1:message-publishing', 'default', '2-0')
  })

  test('does not let one failed entry block acknowledgement of another entry', async () => {
    const abortController = new AbortController()
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    }
    const agent = {
      config: { logger },
      context: { config: { logger } },
      events: { on: vi.fn() },
    }
    const client = {
      xack: vi.fn().mockResolvedValue(1),
      xgroup: vi.fn().mockResolvedValue('OK'),
      xreadgroup: vi.fn().mockResolvedValue([
        [
          'server:server-1:message-publishing',
          [
            ['1-0', ['message', JSON.stringify({ connectionId: 'connection-1' })]],
            ['2-0', ['message', JSON.stringify({ connectionId: 'connection-2' })]],
          ],
        ],
      ]),
    }
    const publishing = new RedisStreamMessagePublishing(agent as never, client as never, 'server-1')

    await publishing.listenForMessages(
      async (message) => {
        if (message.id === '1-0') throw new Error('delivery failed')
        abortController.abort()
      },
      { signal: abortController.signal }
    )

    expect(client.xack).toHaveBeenCalledOnce()
    expect(client.xack).toHaveBeenCalledWith('server:server-1:message-publishing', 'default', '2-0')
    expect(logger.error).toHaveBeenCalledWith('Error processing message 1-0:', {
      error: expect.objectContaining({ message: 'delivery failed' }),
    })
  })
})
