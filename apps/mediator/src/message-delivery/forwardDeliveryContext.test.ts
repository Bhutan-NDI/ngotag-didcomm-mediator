import { describe, expect, test } from 'vitest'
import {
  getForwardDeliveryCompletion,
  isInForwardDeliveryContext,
  runInForwardDeliveryContext,
} from './forwardDeliveryContext.js'

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })

  return { promise, resolve }
}

describe('forwardDeliveryContext', () => {
  test('is scoped to the forward-delivery asynchronous call chain', async () => {
    const contextStarted = deferred()
    const releaseContext = deferred()
    const completionObserved = deferred()

    expect(isInForwardDeliveryContext()).toBe(false)
    expect(getForwardDeliveryCompletion()).toBeUndefined()

    const inContext = runInForwardDeliveryContext(async () => {
      expect(isInForwardDeliveryContext()).toBe(true)
      const completion = getForwardDeliveryCompletion()
      expect(completion).toBeDefined()
      void completion?.then(() => completionObserved.resolve())
      contextStarted.resolve()
      await releaseContext.promise
      expect(isInForwardDeliveryContext()).toBe(true)
    })

    await contextStarted.promise
    expect(isInForwardDeliveryContext()).toBe(false)
    expect(getForwardDeliveryCompletion()).toBeUndefined()

    let completed = false
    void completionObserved.promise.then(() => {
      completed = true
    })
    await Promise.resolve()
    expect(completed).toBe(false)

    releaseContext.resolve()
    await inContext
    await completionObserved.promise
    expect(isInForwardDeliveryContext()).toBe(false)
  })

  test('signals completion when the forward-delivery call rejects', async () => {
    let completion: Promise<void> | undefined
    const inContext = runInForwardDeliveryContext(async () => {
      completion = getForwardDeliveryCompletion()
      throw new Error('forward delivery failed')
    })

    await expect(inContext).rejects.toThrow('forward delivery failed')
    await expect(completion).resolves.toBeUndefined()
  })
})
