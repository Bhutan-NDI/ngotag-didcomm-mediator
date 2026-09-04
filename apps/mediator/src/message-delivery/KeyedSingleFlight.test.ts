import { describe, expect, test } from 'vitest'
import { KeyedSingleFlight } from './KeyedSingleFlight.js'

function deferred() {
  let resolve!: () => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })

  return { promise, reject, resolve }
}

describe('KeyedSingleFlight', () => {
  test('coalesces a synchronous burst into one run with one owner', async () => {
    let taskCalls = 0
    const singleFlight = new KeyedSingleFlight(async () => {
      taskCalls += 1
      return taskCalls
    })

    const first = singleFlight.schedule('connection-1')
    const second = singleFlight.schedule('connection-1')
    const third = singleFlight.schedule('connection-1')

    expect(first.isOwner).toBe(true)
    expect(second.isOwner).toBe(false)
    expect(third.isOwner).toBe(false)
    expect(second.result).toBe(first.result)
    expect(third.result).toBe(first.result)
    expect(second.flightId).toBe(first.flightId)
    expect(third.flightId).toBe(first.flightId)
    await expect(first.result).resolves.toBe(1)
    expect(taskCalls).toBe(1)
  })

  test('serializes a connection and coalesces concurrent triggers into one follow-up', async () => {
    const firstRunStarted = deferred()
    const releaseFirstRun = deferred()
    let activeTasks = 0
    let maximumActiveTasks = 0
    let taskCalls = 0

    const singleFlight = new KeyedSingleFlight(async () => {
      taskCalls += 1
      activeTasks += 1
      maximumActiveTasks = Math.max(maximumActiveTasks, activeTasks)

      if (taskCalls === 1) {
        firstRunStarted.resolve()
        await releaseFirstRun.promise
      }

      activeTasks -= 1
      return taskCalls
    })

    const first = singleFlight.schedule('connection-1')
    await firstRunStarted.promise
    const second = singleFlight.schedule('connection-1')
    const third = singleFlight.schedule('connection-1')

    expect(first.isOwner).toBe(true)
    expect(second.isOwner).toBe(true)
    expect(third.isOwner).toBe(false)
    expect(second.result).not.toBe(first.result)
    expect(third.result).toBe(second.result)
    expect(second.flightId).toBe(first.flightId)
    expect(third.flightId).toBe(first.flightId)

    releaseFirstRun.resolve()
    await expect(first.result).resolves.toBe(1)
    await expect(second.result).resolves.toBe(2)
    expect(taskCalls).toBe(2)
    expect(maximumActiveTasks).toBe(1)
  })

  test('reports each run result to the triggers coalesced into that run', async () => {
    const firstRunStarted = deferred()
    const releaseFirstRun = deferred()
    let taskCalls = 0

    const singleFlight = new KeyedSingleFlight(async () => {
      taskCalls += 1
      if (taskCalls === 1) {
        firstRunStarted.resolve()
        await releaseFirstRun.promise
        return true
      }

      return false
    })

    const first = singleFlight.schedule('connection-1')
    await firstRunStarted.promise
    const followUp = singleFlight.schedule('connection-1')

    releaseFirstRun.resolve()
    await expect(first.result).resolves.toBe(true)
    await expect(followUp.result).resolves.toBe(false)
  })

  test('runs different connections concurrently', async () => {
    const bothRunsStarted = deferred()
    const releaseRuns = deferred()
    let activeTasks = 0
    let maximumActiveTasks = 0

    const singleFlight = new KeyedSingleFlight(async (connectionId: string) => {
      activeTasks += 1
      maximumActiveTasks = Math.max(maximumActiveTasks, activeTasks)
      if (activeTasks === 2) bothRunsStarted.resolve()

      await releaseRuns.promise
      activeTasks -= 1
      return connectionId
    })

    const first = singleFlight.schedule('connection-1')
    const second = singleFlight.schedule('connection-2')
    await bothRunsStarted.promise

    expect(first.isOwner).toBe(true)
    expect(second.isOwner).toBe(true)
    expect(maximumActiveTasks).toBe(2)
    releaseRuns.resolve()
    await expect(Promise.all([first.result, second.result])).resolves.toEqual(['connection-1', 'connection-2'])
  })

  test('clears a failed flight so a later trigger can retry', async () => {
    let shouldFail = true
    const singleFlight = new KeyedSingleFlight(async () => {
      if (shouldFail) throw new Error('delivery failed')
      return 'delivered'
    })

    const failed = singleFlight.schedule('connection-1')
    await expect(failed.result).rejects.toThrow('delivery failed')

    shouldFail = false
    const retry = singleFlight.schedule('connection-1')
    expect(retry.flightId).not.toBe(failed.flightId)
    await expect(retry.result).resolves.toBe('delivered')
  })

  test('does not start a follow-up deadline before the follow-up run starts', async () => {
    const firstRunStarted = deferred()
    const releaseFirstRun = deferred()
    let taskCalls = 0

    const singleFlight = new KeyedSingleFlight(async () => {
      taskCalls += 1
      if (taskCalls === 1) {
        firstRunStarted.resolve()
        await releaseFirstRun.promise
      }
      return taskCalls
    })

    singleFlight.schedule('connection-1')
    await firstRunStarted.promise
    const followUp = singleFlight.schedule('connection-1')

    let followUpStarted = false
    void followUp.started.then(() => {
      followUpStarted = true
    })
    await Promise.resolve()
    expect(followUpStarted).toBe(false)

    releaseFirstRun.resolve()
    await followUp.started
    expect(followUpStarted).toBe(true)
  })
})
