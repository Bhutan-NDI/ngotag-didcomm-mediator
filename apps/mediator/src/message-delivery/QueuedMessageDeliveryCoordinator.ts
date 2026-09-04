import { KeyedSingleFlight, type ScheduledFlight } from './KeyedSingleFlight.js'
import { settleWithin } from './settleWithin.js'

export type DeliveryFallbackReason =
  | { status: 'errored'; error: unknown }
  | { status: 'timed-out' }
  | { status: 'unavailable' }

interface FallbackState {
  promise: Promise<void>
  succeeded: boolean
}

export class QueuedMessageDeliveryCoordinator<Key> {
  private readonly delivery: KeyedSingleFlight<Key, boolean>
  private readonly fallbackByFlight = new WeakMap<object, FallbackState>()
  private readonly fallbackInProgressByKey = new Map<Key, FallbackState>()

  public constructor(
    deliver: (key: Key) => Promise<boolean>,
    private readonly fallback: (key: Key, reason: DeliveryFallbackReason) => Promise<void>,
    private readonly deliveryTimeoutMs: number,
    private readonly completionTimeoutMs = deliveryTimeoutMs
  ) {
    this.delivery = new KeyedSingleFlight(deliver)
  }

  /**
   * Schedule one serialized delivery run for a key. Local delivery is bounded
   * from this call, including time queued behind a predecessor, and fallback is
   * bounded by the overall completion deadline. Non-owners share the owner's
   * delivery and fallback side effects.
   */
  public async schedule(key: Key): Promise<void> {
    const delivery = this.delivery.schedule(key)
    if (!delivery.isOwner) return

    const startedAt = Date.now()
    const deliveryDeadline = startedAt + this.deliveryTimeoutMs
    const completionDeadline = startedAt + this.completionTimeoutMs
    const started = await settleWithin(delivery.started, this.remaining(deliveryDeadline))

    // A predecessor still owns the active delivery when a queued run cannot
    // start by its deadline. Join the flight's shared fallback so stream callers
    // keep acknowledgement coupled to its outcome without duplicating effects.
    if (started.status === 'timed-out') {
      await this.completeFallbackWithin(delivery, key, started, completionDeadline)
      return
    }
    if (started.status === 'errored') {
      await this.completeFallbackWithin(delivery, key, started, completionDeadline)
      return
    }

    const result = await settleWithin(delivery.result, this.remaining(deliveryDeadline))
    if (result.status === 'completed' && result.value) {
      // A successful follow-up makes a fallback from an earlier run irrelevant
      // for later triggers in the same continuously active flight. Do not clear
      // one that is still running, or a later run could duplicate its effects.
      const fallback = this.fallbackByFlight.get(delivery.flightId)
      if (fallback?.succeeded) this.fallbackByFlight.delete(delivery.flightId)
      return
    }

    const reason: DeliveryFallbackReason = result.status === 'completed' ? { status: 'unavailable' } : result
    await this.completeFallbackWithin(delivery, key, reason, completionDeadline)
  }

  private remaining(deadline: number): number {
    return Math.max(0, deadline - Date.now())
  }

  private async completeFallbackWithin(
    delivery: ScheduledFlight<boolean>,
    key: Key,
    reason: DeliveryFallbackReason,
    deadline: number
  ): Promise<void> {
    const result = await settleWithin(this.fallbackOnce(delivery, key, reason), this.remaining(deadline))
    if (result.status === 'completed') return
    if (result.status === 'errored') throw result.error

    throw new Error('Queued message delivery fallback timed out')
  }

  private async fallbackOnce(
    delivery: ScheduledFlight<boolean>,
    key: Key,
    reason: DeliveryFallbackReason
  ): Promise<void> {
    let fallback = this.fallbackByFlight.get(delivery.flightId) ?? this.fallbackInProgressByKey.get(key)
    if (!fallback) {
      fallback = { promise: Promise.resolve(), succeeded: false }
      const fallbackState = fallback
      fallback.promise = this.fallback(key, reason)
        .then(() => {
          fallbackState.succeeded = true
        })
        .finally(() => {
          if (this.fallbackInProgressByKey.get(key) === fallbackState) {
            this.fallbackInProgressByKey.delete(key)
          }
        })
      this.fallbackInProgressByKey.set(key, fallback)
    }
    this.fallbackByFlight.set(delivery.flightId, fallback)

    try {
      await fallback.promise
    } catch (error) {
      // Allow another owner in this flight to retry a failed fallback. Redis
      // stream handlers still preserve the entry when this rejection propagates.
      if (this.fallbackByFlight.get(delivery.flightId) === fallback) {
        this.fallbackByFlight.delete(delivery.flightId)
      }
      throw error
    }
  }
}
