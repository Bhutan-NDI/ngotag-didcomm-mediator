import { afterEach, describe, expect, test, vi } from 'vitest'
import { settleWithin } from './settleWithin.js'

function deferred<Result>() {
  let resolve!: (result: Result) => void
  const promise = new Promise<Result>((resolvePromise) => {
    resolve = resolvePromise
  })

  return { promise, resolve }
}

describe('settleWithin', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  test('returns the operation result when it completes within the deadline', async () => {
    await expect(settleWithin(Promise.resolve(false), 60_000)).resolves.toEqual({
      status: 'completed',
      value: false,
    })
  })

  test('returns a timeout while leaving the original operation active', async () => {
    vi.useFakeTimers()
    const operation = deferred<boolean>()

    const result = settleWithin(operation.promise, 60_000)
    await vi.advanceTimersByTimeAsync(60_000)

    await expect(result).resolves.toEqual({ status: 'timed-out' })

    operation.resolve(true)
    await expect(operation.promise).resolves.toBe(true)
  })

  test('returns an error outcome instead of rejecting', async () => {
    const error = new Error('delivery failed')

    await expect(settleWithin(Promise.reject(error), 60_000)).resolves.toEqual({
      status: 'errored',
      error,
    })
  })
})
