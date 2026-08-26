interface Deferred<Result> {
  promise: Promise<Result>
  reject: (reason?: unknown) => void
  resolve: (result: Result | PromiseLike<Result>) => void
}

interface FlightState<Result> {
  current: Deferred<Result>
  next?: Deferred<Result>
  started: boolean
}

function deferred<Result>(): Deferred<Result> {
  let resolve!: (result: Result | PromiseLike<Result>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<Result>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })

  return { promise, reject, resolve }
}

/**
 * Runs at most one task per key at a time. Triggers received while a task is
 * running are coalesced into one follow-up run, preventing both concurrent
 * delivery and a lost wake-up when a new queue item arrives mid-delivery.
 */
export class KeyedSingleFlight<Key, Result> {
  private readonly flights = new Map<Key, FlightState<Result>>()

  public constructor(private readonly task: (key: Key) => Promise<Result>) {}

  public schedule(key: Key): Promise<Result> {
    const existingFlight = this.flights.get(key)
    if (existingFlight) {
      // Calls made before the task starts are part of the same burst and need
      // only one queue drain. Calls made during a drain share one follow-up and
      // receive that follow-up's result rather than the current run's result.
      if (!existingFlight.started) return existingFlight.current.promise

      existingFlight.next ??= deferred<Result>()
      return existingFlight.next.promise
    }

    const current = deferred<Result>()
    const flight: FlightState<Result> = { current, started: false }
    this.flights.set(key, flight)

    // Start in the next microtask so a synchronous burst for one key collapses
    // into the initial run. The fully initialized flight is visible before
    // task() can schedule more work for the same key.
    queueMicrotask(() => {
      void this.drain(key, flight)
    })
    return current.promise
  }

  private async drain(key: Key, flight: FlightState<Result>): Promise<void> {
    try {
      flight.started = true
      while (true) {
        try {
          flight.current.resolve(await this.task(key))
        } catch (error) {
          flight.current.reject(error)
        }

        if (!flight.next) return

        flight.current = flight.next
        flight.next = undefined
      }
    } finally {
      if (this.flights.get(key) === flight) {
        this.flights.delete(key)
      }
    }
  }
}
