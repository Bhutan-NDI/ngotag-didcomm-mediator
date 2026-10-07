import { DidCommMessageForwardingStrategy } from '@credo-ts/didcomm'
import { describe, expect, test } from 'vitest'
import { resolveCredoMessageForwardingStrategy } from './resolveCredoMessageForwardingStrategy.js'

describe('resolveCredoMessageForwardingStrategy', () => {
  test('assigns Redis as the only live-delivery owner', () => {
    expect(
      resolveCredoMessageForwardingStrategy({
        configuredStrategy: DidCommMessageForwardingStrategy.QueueAndLiveModeDelivery,
        multiInstanceDeliveryType: 'redis',
      })
    ).toBe(DidCommMessageForwardingStrategy.QueueOnly)
  })

  test.each([
    [DidCommMessageForwardingStrategy.QueueAndLiveModeDelivery, 'none'],
    [DidCommMessageForwardingStrategy.QueueOnly, 'none'],
    [DidCommMessageForwardingStrategy.QueueOnly, 'redis'],
    [DidCommMessageForwardingStrategy.DirectDelivery, 'none'],
    [DidCommMessageForwardingStrategy.DirectDelivery, 'redis'],
  ] as const)('preserves %s with %s multi-instance delivery', (configuredStrategy, multiInstanceDeliveryType) => {
    expect(resolveCredoMessageForwardingStrategy({ configuredStrategy, multiInstanceDeliveryType })).toBe(
      configuredStrategy
    )
  })
})
