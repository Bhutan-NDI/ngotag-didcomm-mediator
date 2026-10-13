import { describe, expect, test, vi } from 'vitest'
import { handleQueuedMessageEvent } from './handleQueuedMessageEvent.js'

describe('handleQueuedMessageEvent', () => {
  test('logs instead of rejecting when delivery or fallback fails', async () => {
    const logger = { error: vi.fn() }
    const error = new Error('Queued message delivery fallback timed out')

    await expect(
      handleQueuedMessageEvent({ connectionId: 'connection-1', deliver: () => Promise.reject(error), logger })
    ).resolves.toBeUndefined()

    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('remains queued'), {
      connectionId: 'connection-1',
      error,
    })
  })

  test('delivers for the queued connection and logs nothing on success', async () => {
    const logger = { error: vi.fn() }
    const deliver = vi.fn().mockResolvedValue(undefined)

    await handleQueuedMessageEvent({ connectionId: 'connection-1', deliver, logger })

    expect(deliver).toHaveBeenCalledWith('connection-1')
    expect(logger.error).not.toHaveBeenCalled()
  })
})
