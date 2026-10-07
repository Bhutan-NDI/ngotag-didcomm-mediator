import { KeyedSingleFlight, type ScheduledFlight } from './KeyedSingleFlight.js'
import { settleWithin } from './settleWithin.js'

export type DeliveryFallbackReason =
  | { status: 'errored'; error: unknown }
  | { status: 'timed-out' }
  | { status: 'unavailable' }

export class QueuedMessageDeliveryCoordinator<Key> {
  private readonly delivery: KeyedSingleFlight<Key, boolean>
  private readonly fallbackInProgressByKey = new Map<Key, Promise<void>>()
  private readonly outcomeByRun = new WeakMap<Promise<boolean>, Promise<void>>()

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
   * bounded by the overall completion deadline. Callers coalesced onto a run
   * while its owner is active share the owner's delivery, fallback and outcome,
   * so a stream entry is not acknowledged when that run's fallback fails. Once
   * the owner settles, a later caller of a run that still has not started (its
   * predecessor hangs) becomes its new owner, so it re-routes or notifies for
   * its own message instead of inheriting an earlier outcome.
   *
   * Stream callers schedule as soon as an entry is read: Redis measures pending
   * idle time from delivery to the consumer, so that is the clock the 60-second
   * claim races against, not the entry's creation time.
   */
  public schedule(key: Key): Promise<void> {
    const delivery = this.delivery.schedule(key)
    const activeOutcome = this.outcomeByRun.get(delivery.result)
    if (activeOutcome) return activeOutcome

    const outcome = this.runAsOwner(delivery, key).finally(() => {
      if (this.outcomeByRun.get(delivery.result) === outcome) this.outcomeByRun.delete(delivery.result)
    })
    this.outcomeByRun.set(delivery.result, outcome)
    return outcome
  }

  private async runAsOwner(delivery: ScheduledFlight<boolean>, key: Key): Promise<void> {
    const startedAt = Date.now()
    const deliveryDeadline = startedAt + this.deliveryTimeoutMs
    const completionDeadline = startedAt + this.completionTimeoutMs
    const started = await settleWithin(delivery.started, this.remaining(deliveryDeadline))

    // A predecessor still owns the active delivery when a queued run cannot
    // start by its deadline, so fall back for this run's messages.
    if (started.status === 'timed-out') {
      await this.completeFallbackWithin(key, started, completionDeadline)
      return
    }
    if (started.status === 'errored') {
      await this.completeFallbackWithin(key, started, completionDeadline)
      return
    }

    const result = await settleWithin(delivery.result, this.remaining(deliveryDeadline))
    if (result.status === 'completed' && result.value) return

    const reason: DeliveryFallbackReason = result.status === 'completed' ? { status: 'unavailable' } : result
    await this.completeFallbackWithin(key, reason, completionDeadline)
  }

  private remaining(deadline: number): number {
    return Math.max(0, deadline - Date.now())
  }

  private async completeFallbackWithin(key: Key, reason: DeliveryFallbackReason, deadline: number): Promise<void> {
    const result = await settleWithin(this.fallbackOnce(key, reason), this.remaining(deadline))
    if (result.status === 'completed') return
    if (result.status === 'errored') throw result.error

    throw new Error('Queued message delivery fallback timed out')
  }

  /**
   * Share only a fallback that is still running. Once it settles, the next run
   * that needs one routes again, because the session may have moved and its
   * new messages were not covered by the earlier forward or push.
   */
  private fallbackOnce(key: Key, reason: DeliveryFallbackReason): Promise<void> {
    const inProgress = this.fallbackInProgressByKey.get(key)
    if (inProgress) return inProgress

    const fallback = this.fallback(key, reason).finally(() => {
      if (this.fallbackInProgressByKey.get(key) === fallback) this.fallbackInProgressByKey.delete(key)
    })
    this.fallbackInProgressByKey.set(key, fallback)
    return fallback
  }
}
