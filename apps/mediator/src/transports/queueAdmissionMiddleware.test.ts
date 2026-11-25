import type { Logger } from '@credo-ts/core'
import {
  DidCommForwardMessage,
  type DidCommInboundMessageContext,
  DidCommMessageForwardingStrategy,
  DidCommTrustPingMessage,
} from '@credo-ts/didcomm'
import { QueuedMessageTooLargeError } from '@credo-ts/didcomm-transport-queue-dynamodb'
import type { Request, Response } from 'express'
import { describe, expect, test, vi } from 'vitest'
import { HttpAdmissionSession } from './AdmissionHttpInboundTransport.js'
import { createQueueAdmissionMiddleware, MESSAGE_TOO_LARGE_REASON } from './queueAdmissionMiddleware.js'

const recipientVerkey = '8HH5gYEeNc3z7PYXmd54d4x6qAfCNrqQqEB3nS7Zfu7K'
const payload = { protected: 'p', iv: 'i', ciphertext: 'c', tag: 't' }
const forward = () => new DidCommForwardMessage({ to: recipientVerkey, message: payload })

const logger = () => ({ warn: vi.fn() }) as unknown as Logger & { warn: ReturnType<typeof vi.fn> }
const context = (message: unknown, encryptedMessage: object = {}) =>
  ({ message, encryptedMessage }) as unknown as DidCommInboundMessageContext
const tooLarge = () => {
  throw new QueuedMessageTooLargeError(500_000)
}

/** An Express response reduced to what the session uses. */
const response = () => {
  const res = {
    headersSent: false,
    statusCode: 0,
    body: undefined as unknown,
    status(code: number) {
      res.statusCode = code
      return res
    },
    type: () => res,
    contentType: () => res,
    send(body?: unknown) {
      res.body = body
      res.headersSent = true
      return res
    },
    json(body: unknown) {
      res.body = body
      res.headersSent = true
      return res
    },
    end() {
      res.headersSent = true
      return res
    },
  }
  return res
}
const session = (res = response()) => ({
  res,
  session: new HttpAdmissionSession('id', { headers: {} } as Request, res as unknown as Response),
})
const nextTurn = () => new Promise((resolve) => setImmediate(resolve))

describe('the queue admission middleware', () => {
  test('a forward the queue cannot store is not handled, and is logged with its sizes only', async () => {
    const log = logger()
    const next = vi.fn()
    const middleware = createQueueAdmissionMiddleware({
      queueTransportRepository: { assertMessageFits: tooLarge },
      messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
      logger: log,
    })

    await middleware(context(forward()), next)

    expect(next).not.toHaveBeenCalled()
    expect(log.warn).toHaveBeenCalledTimes(1)
    // Not over HTTP here (a WebSocket, say): there is no status to answer with, and the log says so.
    expect(log.warn.mock.calls[0][1]).toEqual({
      itemBytes: 500_000,
      maxItemBytes: 409_600,
      transport: 'other',
      senderInformed: false,
    })
  })

  test('the queue is asked about the recipient DID and the payload the forward handler will store', async () => {
    const assertMessageFits = vi.fn()
    const next = vi.fn()
    const middleware = createQueueAdmissionMiddleware({
      queueTransportRepository: { assertMessageFits },
      messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueAndLiveModeDelivery,
      logger: logger(),
    })

    await middleware(context(forward()), next)

    expect(assertMessageFits).toHaveBeenCalledWith({
      recipientDids: ['did:key:z6MkmjY8GnV5i9YTDtPETC2uUAW6ejw3nk5mXF5yci5ab7th'],
      payload,
    })
    expect(next).toHaveBeenCalledTimes(1)
  })

  test('with direct delivery a forward is not checked: it is only stored when its recipient has no session', async () => {
    const assertMessageFits = vi.fn(tooLarge)
    const next = vi.fn()
    const middleware = createQueueAdmissionMiddleware({
      queueTransportRepository: { assertMessageFits },
      messageForwardingStrategy: DidCommMessageForwardingStrategy.DirectDelivery,
      logger: logger(),
    })

    await middleware(context(forward()), next)

    expect(assertMessageFits).not.toHaveBeenCalled()
    expect(next).toHaveBeenCalledTimes(1)
  })

  test('a message that is not a forward, a backend with no size limit and a failing check all go on to the handler', async () => {
    const next = vi.fn()
    const limited = createQueueAdmissionMiddleware({
      queueTransportRepository: { assertMessageFits: vi.fn(tooLarge) },
      messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
      logger: logger(),
    })
    await limited(context(new DidCommTrustPingMessage({})), next)
    expect(next).toHaveBeenCalledTimes(1)

    const unlimited = createQueueAdmissionMiddleware({
      queueTransportRepository: {},
      messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
      logger: logger(),
    })
    await unlimited(context(forward()), next)
    expect(next).toHaveBeenCalledTimes(2)

    // An error that is not about the size is the forward handler's to report, as before.
    const failing = createQueueAdmissionMiddleware({
      queueTransportRepository: {
        assertMessageFits: () => {
          throw new Error('something else')
        },
      },
      messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
      logger: logger(),
    })
    await failing(context(forward()), next)
    expect(next).toHaveBeenCalledTimes(3)
  })
})

describe('the HTTP admission session', () => {
  test('close does not answer by itself; accept answers 200 once Credo has asked to close', async () => {
    const { res, session: s } = session()
    s.accept()
    expect(res.headersSent).toBe(false) // a request that waits for a reply on the connection is left open

    await s.close()
    expect(res.headersSent).toBe(false)
    s.accept()
    expect(res.statusCode).toBe(200)
    expect(res.headersSent).toBe(true)
  })

  test('refuse answers with the status and the reason, once', async () => {
    const { res, session: s } = session()
    await s.close()
    expect(s.refuse(413, MESSAGE_TOO_LARGE_REASON)).toBe(true)
    expect(res.statusCode).toBe(413)
    expect(res.body).toBe(MESSAGE_TOO_LARGE_REASON)
    expect(s.refuse(500, 'again')).toBe(false)
    expect(res.statusCode).toBe(413)

    // The fallback of close() does not answer a second time.
    await nextTurn()
    expect(res.statusCode).toBe(413)
  })

  test('with no decision by the next turn of the event loop, a closed session is acknowledged as Credo does', async () => {
    const { res, session: s } = session()
    await s.close()
    expect(res.headersSent).toBe(false)
    await nextTurn()
    expect(res.statusCode).toBe(200)
    expect(res.headersSent).toBe(true)
  })

  test('send refuses a session that was already answered', async () => {
    const { session: s } = session()
    await s.close()
    s.accept()
    await expect(s.send({} as never, payload)).rejects.toThrow('http transport session has been closed.')
  })
})
