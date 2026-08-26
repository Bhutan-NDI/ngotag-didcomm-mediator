import { AsyncLocalStorage } from 'node:async_hooks'

interface ForwardDeliveryContext {
  completion: Promise<void>
}

// This context is deliberately limited to coordinating the queue event emitted
// by processForwardMessage with Credo's subsequent local-delivery attempt. It
// does not carry request data or correlate messages across transports.
const forwardDeliveryContext = new AsyncLocalStorage<ForwardDeliveryContext>()

export function runInForwardDeliveryContext<T>(callback: () => Promise<T>): Promise<T> {
  let complete!: () => void
  const completion = new Promise<void>((resolve) => {
    complete = resolve
  })

  return forwardDeliveryContext.run({ completion }, async () => {
    try {
      return await callback()
    } finally {
      complete()
    }
  })
}

export function isInForwardDeliveryContext(): boolean {
  return forwardDeliveryContext.getStore() !== undefined
}

export function getForwardDeliveryCompletion(): Promise<void> | undefined {
  return forwardDeliveryContext.getStore()?.completion
}
