import { afterEach, describe, expect, test, vi } from 'vitest'
import { QueuedMessageDeliveryCoordinator } from './QueuedMessageDeliveryCoordinator.js'

function deferred<Result>() {
  let resolve!: (result: Result) => void
  const promise = new Promise<Result>((resolvePromise) => {
    resolve = resolvePromise
  })

  return { promise, resolve }
}

describe('QueuedMessageDeliveryCoordinator', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  test('coalesces a synchronous burst and performs one fallback', async () => {
    const deliver = vi.fn().mockResolvedValue(false)
    const fallback = vi.fn().mockResolvedValue(undefined)
    const coordinator = new QueuedMessageDeliveryCoordinator(deliver, fallback, 60_000)

    await Promise.all([coordinator.schedule('connection-1'), coordinator.schedule('connection-1')])

    expect(deliver).toHaveBeenCalledOnce()
    expect(fallback).toHaveBeenCalledOnce()
    expect(fallback).toHaveBeenCalledWith('connection-1', { status: 'unavailable' })
  })

  test('deduplicates the centralized fallback when event sources overlap', async () => {
    const firstRun = deferred<boolean>()
    const firstRunStarted = deferred<void>()
    const deliver = vi.fn().mockImplementationOnce(async () => {
      firstRunStarted.resolve()
      return await firstRun.promise
    })
    deliver.mockResolvedValueOnce(false)
    const fallback = vi.fn().mockResolvedValue(undefined)
    const coordinator = new QueuedMessageDeliveryCoordinator(deliver, fallback, 60_000)

    const first = coordinator.schedule('connection-1')
    await firstRunStarted.promise
    const followUp = coordinator.schedule('connection-1')
    firstRun.resolve(false)

    await Promise.all([first, followUp])
    expect(deliver).toHaveBeenCalledTimes(2)
    expect(fallback).toHaveBeenCalledOnce()
  })

  test('bounds a follow-up queued behind a hung delivery without duplicating fallback', async () => {
    vi.useFakeTimers()
    const firstRun = deferred<boolean>()
    const firstRunStarted = deferred<void>()
    const deliver = vi.fn().mockImplementation(async () => {
      firstRunStarted.resolve()
      return await firstRun.promise
    })
    const fallbackResult = deferred<void>()
    const fallback = vi.fn().mockImplementation(() => fallbackResult.promise)
    const coordinator = new QueuedMessageDeliveryCoordinator(deliver, fallback, 60_000, 70_000)

    const first = coordinator.schedule('connection-1')
    await firstRunStarted.promise
    const followUp = coordinator.schedule('connection-1')
    await vi.advanceTimersByTimeAsync(60_000)

    expect(deliver).toHaveBeenCalledOnce()
    expect(fallback).toHaveBeenCalledOnce()
    expect(fallback).toHaveBeenCalledWith('connection-1', { status: 'timed-out' })

    fallbackResult.resolve()
    await Promise.all([first, followUp])
    firstRun.resolve(true)
  })

  test('does not repeat fallback when a timed-out delivery completes late', async () => {
    vi.useFakeTimers()
    const delivery = deferred<boolean>()
    const fallback = vi.fn().mockResolvedValue(undefined)
    const coordinator = new QueuedMessageDeliveryCoordinator(() => delivery.promise, fallback, 60_000)

    const scheduled = coordinator.schedule('connection-1')
    await vi.advanceTimersByTimeAsync(60_000)
    await scheduled
    expect(fallback).toHaveBeenCalledOnce()

    delivery.resolve(false)
    await delivery.promise
    await Promise.resolve()
    expect(fallback).toHaveBeenCalledOnce()
  })

  test('shares an in-progress fallback with a new flight for the same connection', async () => {
    const fallbackResult = deferred<void>()
    const fallbackStarted = deferred<void>()
    const deliver = vi.fn().mockResolvedValue(false)
    const fallback = vi.fn().mockImplementation(async () => {
      fallbackStarted.resolve()
      await fallbackResult.promise
    })
    const coordinator = new QueuedMessageDeliveryCoordinator(deliver, fallback, 60_000)

    const first = coordinator.schedule('connection-1')
    await fallbackStarted.promise
    const second = coordinator.schedule('connection-1')
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(deliver).toHaveBeenCalledTimes(2)
    expect(fallback).toHaveBeenCalledOnce()

    fallbackResult.resolve()
    await Promise.all([first, second])
    expect(fallback).toHaveBeenCalledOnce()
  })

  test('turns a delivery rejection into one fallback outcome', async () => {
    const error = new Error('delivery failed')
    const fallback = vi.fn().mockResolvedValue(undefined)
    const coordinator = new QueuedMessageDeliveryCoordinator(() => Promise.reject(error), fallback, 60_000)

    await expect(coordinator.schedule('connection-1')).resolves.toBeUndefined()
    expect(fallback).toHaveBeenCalledWith('connection-1', { status: 'errored', error })
  })

  test('bounds a hung fallback by the overall completion deadline', async () => {
    vi.useFakeTimers()
    const fallbackResult = deferred<void>()
    const coordinator = new QueuedMessageDeliveryCoordinator(
      () => Promise.resolve(false),
      () => fallbackResult.promise,
      45_000,
      55_000
    )

    const scheduled = expect(coordinator.schedule('connection-1')).rejects.toThrow(
      'Queued message delivery fallback timed out'
    )
    await vi.advanceTimersByTimeAsync(55_000)

    await scheduled
    fallbackResult.resolve()
  })
})
