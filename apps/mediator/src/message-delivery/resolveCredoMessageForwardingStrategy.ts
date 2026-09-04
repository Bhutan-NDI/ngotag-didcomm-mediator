import { DidCommMessageForwardingStrategy } from '@credo-ts/didcomm'

/**
 * Redis message delivery owns the live queue drain and cross-instance fallback.
 * Keep Credo on QueueOnly in that topology so one queue event has one delivery
 * owner. Other topologies continue to use the configured Credo strategy.
 */
export function resolveCredoMessageForwardingStrategy({
  configuredStrategy,
  multiInstanceDeliveryType,
}: {
  configuredStrategy: DidCommMessageForwardingStrategy
  multiInstanceDeliveryType: 'none' | 'redis'
}): DidCommMessageForwardingStrategy {
  if (
    configuredStrategy === DidCommMessageForwardingStrategy.QueueAndLiveModeDelivery &&
    multiInstanceDeliveryType === 'redis'
  ) {
    return DidCommMessageForwardingStrategy.QueueOnly
  }

  return configuredStrategy
}
