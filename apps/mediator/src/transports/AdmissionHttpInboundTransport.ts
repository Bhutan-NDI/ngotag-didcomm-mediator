import { type AgentContext, CredoError, EventEmitter, utils } from '@credo-ts/core'
import {
  type DidCommEncryptedMessage,
  DidCommEventTypes,
  type DidCommMessage,
  type DidCommMessageProcessedEvent,
  type DidCommMessageReceivedEvent,
  DidCommMimeType,
  DidCommModuleConfig,
  DidCommTransportService,
  type DidCommTransportSession,
} from '@credo-ts/didcomm'
import { DidCommHttpInboundTransport } from '@credo-ts/node'
import type { Express, Request, Response } from 'express'

const supportedContentTypes: string[] = [DidCommMimeType.V0, DidCommMimeType.V1]

/**
 * The HTTP session of one inbound request.
 *
 * Credo's own session answers 200 as soon as a message is decrypted, before the message is handled
 * (`DidCommMessageReceiver` closes the session of every message that asks for no reply on the connection).
 * A message the mediator then cannot store was therefore already acknowledged, and the sender never learned
 * it was lost. This session lets the admission middleware decide first: `close()` only notes that Credo is
 * done with the session, and the answer is sent by `accept()` or `refuse()`.
 */
export class HttpAdmissionSession implements DidCommTransportSession {
  public readonly type = 'http'
  public keys?: DidCommTransportSession['keys']
  public inboundMessage?: DidCommMessage
  public connectionId?: string
  private closeRequested = false

  public constructor(
    public readonly id: string,
    private readonly req: Request,
    private readonly res: Response
  ) {}

  /**
   * Called by Credo right before it dispatches a message that asks for no reply on this connection. The
   * admission middleware is the first thing the dispatch runs and answers in the same turn of the event loop.
   * Should nothing answer by the next turn, the request is acknowledged as Credo itself would have done.
   */
  public async close(): Promise<void> {
    if (this.closeRequested) return
    this.closeRequested = true
    setImmediate(() => this.accept())
  }

  /** Acknowledges the request, once Credo has asked to close it. A request that waits for a reply is left open. */
  public accept(): void {
    if (!this.closeRequested || this.res.headersSent) return
    this.res.status(200).end()
  }

  /** Answers the request with an error status. Returns false when it was already answered. */
  public refuse(status: number, reason: string): boolean {
    if (this.res.headersSent) return false
    this.res.status(status).type('text/plain').send(reason)
    return true
  }

  public async send(agentContext: AgentContext, encryptedMessage: DidCommEncryptedMessage): Promise<void> {
    if (this.res.headersSent) throw new CredoError(`${this.type} transport session has been closed.`)

    // By default the mime type configured for the agent, unless the request came in with another supported one.
    let responseMimeType = agentContext.dependencyManager.resolve(DidCommModuleConfig).didCommMimeType as string
    const requestMimeType = this.req.headers['content-type']
    if (requestMimeType && supportedContentTypes.includes(requestMimeType)) responseMimeType = requestMimeType

    this.res.status(200).contentType(responseMimeType).json(encryptedMessage).end()
  }
}

const sessionsByMessage = new WeakMap<object, HttpAdmissionSession>()

/** The HTTP session an inbound message arrived on, if it arrived over this transport and is still open. */
export function getHttpAdmissionSession(encryptedMessage: unknown): HttpAdmissionSession | undefined {
  return typeof encryptedMessage === 'object' && encryptedMessage !== null
    ? sessionsByMessage.get(encryptedMessage)
    : undefined
}

type Waiter = { resolve: () => void; reject: (error: Error) => void; timer: NodeJS.Timeout }

/**
 * Credo's HTTP inbound transport, with the acknowledgement of a request left to the admission middleware
 * (see {@link HttpAdmissionSession}). Everything else is as in `DidCommHttpInboundTransport`: the same route,
 * content types, body limit, reply on the connection and time limit. One listener serves every request, where
 * Credo's subscribes one filter per request.
 *
 * ON EVERY CREDO UPGRADE, compare this file with Credo's `DidCommHttpInboundTransport` and `DidCommMessageReceiver`
 * again (written against 0.6.0). Refusing a message before it is acknowledged rests on three things Credo does
 * today: the receiver calls `session.close()` right before it dispatches the message; the encrypted message
 * keeps its object identity from the transport to the message context; and the first middleware runs before
 * the next turn of the event loop. If one of them changes, an over-size forward is answered 200 again and
 * nothing else breaks: `queueAdmission.e2e.test.ts` is the test that shows it.
 */
export class AdmissionHttpInboundTransport extends DidCommHttpInboundTransport {
  private readonly listenPort: number
  private readonly routePath: string
  private readonly processedTimeoutMs: number
  private readonly waiters = new WeakMap<object, Waiter>()
  private httpServer?: ReturnType<Express['listen']>
  private eventEmitter?: EventEmitter
  private readonly onProcessed = (event: DidCommMessageProcessedEvent) => {
    const key = event.payload.encryptedMessage
    if (typeof key !== 'object' || key === null) return
    const waiter = this.waiters.get(key)
    if (!waiter) return
    this.waiters.delete(key)
    clearTimeout(waiter.timer)
    waiter.resolve()
  }

  public constructor(options: {
    app?: Express
    path?: string
    port: number
    processedMessageListenerTimeoutMs?: number
  }) {
    // The parent registers the body parser on the app.
    super(options)
    this.listenPort = options.port
    this.routePath = options.path ?? '/'
    this.processedTimeoutMs = options.processedMessageListenerTimeoutMs ?? 10_000
  }

  public override get server() {
    return this.httpServer
  }

  public override async start(agentContext: AgentContext): Promise<void> {
    const transportService = agentContext.dependencyManager.resolve(DidCommTransportService)
    const eventEmitter = agentContext.dependencyManager.resolve(EventEmitter)
    this.eventEmitter = eventEmitter
    eventEmitter.on<DidCommMessageProcessedEvent>(DidCommEventTypes.DidCommMessageProcessed, this.onProcessed)

    agentContext.config.logger.debug('Starting HTTP inbound transport', { port: this.listenPort })

    this.app.post(this.routePath, async (req, res) => {
      const contentType = req.headers['content-type']
      if (!contentType || !supportedContentTypes.includes(contentType)) {
        return res
          .status(415)
          .send(`Unsupported content-type. Supported content-types are: ${supportedContentTypes.join(', ')}`)
      }

      const session = new HttpAdmissionSession(utils.uuid(), req, res)
      // The request can be closed before a reply is sent on it: the session must not outlive it.
      req.once('close', () => transportService.removeSession(session))

      let encryptedMessage: unknown
      try {
        encryptedMessage = JSON.parse(req.body)
        const processed = this.waitUntilProcessed(encryptedMessage)
        // Awaited below. Should the emit throw first, the time limit must not end as an unhandled rejection.
        processed.catch(() => {})
        if (typeof encryptedMessage === 'object' && encryptedMessage !== null) {
          sessionsByMessage.set(encryptedMessage, session)
        }

        eventEmitter.emit<DidCommMessageReceivedEvent>(agentContext, {
          type: DidCommEventTypes.DidCommMessageReceived,
          payload: { message: encryptedMessage as DidCommEncryptedMessage, session },
        })

        // Wait for the message to be handled: a reply may be sent on this connection until then.
        await processed

        if (!res.headersSent) res.status(200).end()
      } catch (error) {
        agentContext.config.logger.error(`Error processing inbound message: ${(error as Error).message}`, { error })
        if (!res.headersSent) res.status(500).send('Error processing message')
      } finally {
        if (typeof encryptedMessage === 'object' && encryptedMessage !== null) {
          sessionsByMessage.delete(encryptedMessage)
          const waiter = this.waiters.get(encryptedMessage)
          if (waiter) {
            this.waiters.delete(encryptedMessage)
            clearTimeout(waiter.timer)
          }
        }
        transportService.removeSession(session)
      }
    })

    this.httpServer = this.app.listen(this.listenPort)
  }

  public override async stop(): Promise<void> {
    this.eventEmitter?.off<DidCommMessageProcessedEvent>(DidCommEventTypes.DidCommMessageProcessed, this.onProcessed)
    return new Promise((resolve, reject) => {
      if (!this.httpServer) return resolve()
      this.httpServer.close((error) => (error ? reject(error) : resolve()))
    })
  }

  /** Resolves when Credo reports the message processed; rejects after the time limit, as Credo's transport does. */
  private waitUntilProcessed(encryptedMessage: unknown): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (typeof encryptedMessage === 'object' && encryptedMessage !== null) this.waiters.delete(encryptedMessage)
        reject(new Error(`The message was not processed within ${this.processedTimeoutMs} ms`))
      }, this.processedTimeoutMs)
      // Something that is not a JSON object is never reported processed: it only waits out the time limit.
      if (typeof encryptedMessage === 'object' && encryptedMessage !== null) {
        this.waiters.set(encryptedMessage, { resolve, reject, timer })
      }
    })
  }
}
