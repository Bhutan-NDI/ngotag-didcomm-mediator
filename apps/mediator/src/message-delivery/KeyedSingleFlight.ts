interface Deferred<Result> {
  promise: Promise<Result>
  reject: (reason?: unknown) => void
  resolve: (result: Result | PromiseLike<Result>) => void
}

interface FlightRun<Result> {
  result: Deferred<Result>
  started: Deferred<void>
}

interface FlightState<Result> {
  current: FlightRun<Result>
  id: object
  next?: FlightRun<Result>
  started: boolean
}

export interface ScheduledFlight<Result> {
  flightId: object
  isOwner: boolean
  result: Promise<Result>
  started: Promise<void>
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

function createRun<Result>(): FlightRun<Result> {
  return { result: deferred<Result>(), started: deferred<void>() }
}

function scheduledFlight<Result>(
  flight: FlightState<Result>,
  run: FlightRun<Result>,
  isOwner: boolean
): ScheduledFlight<Result> {
  return { flightId: flight.id, isOwner, result: run.result.promise, started: run.started.promise }
}

/**
 * Runs at most one task per key at a time. Triggers received while a task is
 * running are coalesced into one follow-up run, preventing both concurrent
 * delivery and a lost wake-up when a new queue item arrives mid-delivery. Each
 * run elects one owner for result side effects and exposes when that run starts
 * so queued follow-up deadlines do not begin prematurely.
 */
export class KeyedSingleFlight<Key, Result> {
  private readonly flights = new Map<Key, FlightState<Result>>()

  public constructor(private readonly task: (key: Key) => Promise<Result>) {}

  public schedule(key: Key): ScheduledFlight<Result> {
    const existingFlight = this.flights.get(key)
    if (existingFlight) {
      // Calls made before the task starts are part of the same burst and need
      // only one queue drain. Calls made during a drain share one follow-up and
      // receive that follow-up's result rather than the current run's result.
      if (!existingFlight.started) return scheduledFlight(existingFlight, existingFlight.current, false)

      const isOwner = existingFlight.next === undefined
      existingFlight.next ??= createRun<Result>()
      return scheduledFlight(existingFlight, existingFlight.next, isOwner)
    }

    const current = createRun<Result>()
    const flight: FlightState<Result> = { current, id: {}, started: false }
    this.flights.set(key, flight)

    // Start in the next microtask so a synchronous burst for one key collapses
    // into the initial run. The fully initialized flight is visible before
    // task() can schedule more work for the same key.
    queueMicrotask(() => {
      void this.drain(key, flight)
    })
    return scheduledFlight(flight, current, true)
  }

  private async drain(key: Key, flight: FlightState<Result>): Promise<void> {
    try {
      flight.started = true
      while (true) {
        flight.current.started.resolve()
        try {
          flight.current.result.resolve(await this.task(key))
        } catch (error) {
          flight.current.result.reject(error)
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
