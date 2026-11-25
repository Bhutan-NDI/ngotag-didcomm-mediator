import { createServer } from 'node:net'
import { Agent, type Logger, LogLevel, TypedArrayEncoder, utils } from '@credo-ts/core'
import {
  type AddMessageOptions,
  DidCommBasicMessage,
  type DidCommEncryptedMessage,
  DidCommEnvelopeService,
  DidCommForwardMessage,
  DidCommHttpOutboundTransport,
  DidCommMessageForwardingStrategy,
  DidCommMessageHandlerRegistry,
  DidCommMimeType,
  DidCommModule,
  InMemoryQueueTransportRepository,
} from '@credo-ts/didcomm'
import {
  DYNAMODB_MAX_ITEM_BYTES,
  QueuedMessageTooLargeError,
  queuedMessageItemBytes,
} from '@credo-ts/didcomm-transport-queue-dynamodb'
import { agentDependencies } from '@credo-ts/node'
import { askar } from '@openwallet-foundation/askar-nodejs'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

// Two real agents, a mediator and a wallet it mediates for, over real HTTP. The queue keeps its messages in
// memory and answers the size question with the DynamoDB queue's own rule and limit, so the sizes are real ones.

const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const server = createServer()
    server.once('error', reject)
    server.listen(0, () => {
      const address = server.address()
      server.close(() =>
        address && typeof address === 'object' ? resolve(address.port) : reject(new Error('no port'))
      )
    })
  })

class RecordingLogger implements Logger {
  public logLevel = LogLevel.warn
  public warnings: Array<{ message: string; data?: Record<string, unknown> }> = []
  public test() {}
  public trace() {}
  public debug() {}
  public info() {}
  public warn(message: string, data?: Record<string, unknown>) {
    this.warnings.push({ message, data })
  }
  public error() {}
  public fatal() {}
}

describe('a forward too large for the queue, end to end', () => {
  const logger = new RecordingLogger()
  let mediator: Agent
  let wallet: Agent
  let endpoint: string
  let queue: InMemoryQueueTransportRepository
  let mediatedConnectionId: string
  let forwardFor: (contentBytes: number) => Promise<{ body: string; storedMessage: DidCommEncryptedMessage }>

  beforeAll(async () => {
    process.env = { ...process.env, LOG_LEVEL: 'off', ASKAR__STORE_ID: 'test', ASKAR__STORE_KEY: 'test' }
    // Loaded here, after the native binding above: Credo's Askar module reads the binding once, when it is first imported.
    const { AskarModule } = await import('@credo-ts/askar')
    const askarModule = (id: string) =>
      new AskarModule({
        askar,
        store: { id, key: 'e2e-test-key', database: { type: 'sqlite', config: { inMemory: true } } },
      })
    const { AdmissionHttpInboundTransport } = await import('./AdmissionHttpInboundTransport.js')
    const { createQueueAdmissionMiddleware } = await import('./queueAdmissionMiddleware.js')
    const { InstrumentedQueueTransportRepository } = await import(
      '../instrumentation/InstrumentedQueueTransportRepository.js'
    )

    const port = await freePort()
    endpoint = `http://localhost:${port}`

    queue = new InMemoryQueueTransportRepository()
    const sizedQueue = Object.assign(queue, {
      assertMessageFits(
        options: Pick<AddMessageOptions, 'recipientDids' | 'payload'> & { telemetry?: Record<string, string> }
      ) {
        const itemBytes = queuedMessageItemBytes({
          recipientDids: options.recipientDids,
          encryptedMessage: options.payload,
          telemetry: options.telemetry,
        })
        if (itemBytes > DYNAMODB_MAX_ITEM_BYTES) throw new QueuedMessageTooLargeError(itemBytes)
      },
    })
    // As the mediator wires it: the backend behind the instrumented wrapper.
    const queueTransportRepository = new InstrumentedQueueTransportRepository(sizedQueue, 'dynamodb')

    mediator = new Agent({
      config: { logger },
      dependencies: agentDependencies,
      modules: {
        askar: askarModule(`mediator-${utils.uuid()}`),
        didcomm: new DidCommModule({
          endpoints: [endpoint],
          didCommMimeType: DidCommMimeType.V0,
          queueTransportRepository,
          transports: {
            inbound: [new AdmissionHttpInboundTransport({ port })],
            outbound: [new DidCommHttpOutboundTransport()],
          },
          connections: { autoAcceptConnections: true },
          mediator: {
            autoAcceptMediationRequests: true,
            messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
          },
          basicMessages: false,
          credentials: false,
          proofs: false,
        }),
      },
    })
    mediator.dependencyManager.resolve(DidCommMessageHandlerRegistry).messageHandlerMiddlewares.unshift(
      createQueueAdmissionMiddleware({
        queueTransportRepository,
        messageForwardingStrategy: DidCommMessageForwardingStrategy.QueueOnly,
        logger,
      })
    )
    await mediator.initialize()

    wallet = new Agent({
      config: {},
      dependencies: agentDependencies,
      modules: {
        askar: askarModule(`wallet-${utils.uuid()}`),
        didcomm: new DidCommModule({
          transports: { outbound: [new DidCommHttpOutboundTransport()] },
          connections: { autoAcceptConnections: true },
          credentials: false,
          proofs: false,
        }),
      },
    })
    await wallet.initialize()

    // The wallet has no endpoint of its own, so every reply of the mediator comes back on the wallet's own
    // HTTP request: connecting and being granted mediation only work if the session still sends those replies.
    const invitation = await mediator.didcomm.oob.createInvitation({ multiUseInvitation: true })
    const { connectionRecord } = await wallet.didcomm.oob.receiveInvitation(invitation.outOfBandInvitation, {
      label: 'wallet',
    })
    if (!connectionRecord) throw new Error('no connection to the mediator')
    const connection = await wallet.didcomm.connections.returnWhenIsConnected(connectionRecord.id)
    const mediation = await wallet.didcomm.mediationRecipient.provision(connection)
    const routing = await wallet.didcomm.mediationRecipient.getRouting({ mediatorId: mediation.id })

    const [mediatedConnection] = await mediator.didcomm.connections.findAllByOutOfBandId(invitation.id)
    mediatedConnectionId = mediatedConnection.id

    const envelopeService = wallet.dependencyManager.resolve(DidCommEnvelopeService)
    forwardFor = async (contentBytes) => {
      // What an issuer sends: a message packed for the wallet, wrapped in a forward packed for the mediator.
      const storedMessage = await envelopeService.packMessage(
        wallet.context,
        new DidCommBasicMessage({ content: 'x'.repeat(contentBytes) }),
        { recipientKeys: [routing.recipientKey], routingKeys: [], senderKey: null }
      )
      const forward = new DidCommForwardMessage({
        to: TypedArrayEncoder.toBase58(routing.recipientKey.publicKey.publicKey),
        message: storedMessage,
      })
      const packed = await envelopeService.packMessage(wallet.context, forward, {
        recipientKeys: [routing.routingKeys[0]],
        routingKeys: [],
        senderKey: null,
      })
      return { body: JSON.stringify(packed), storedMessage }
    }
  }, 60_000)

  afterAll(async () => {
    await wallet?.shutdown()
    await mediator?.shutdown()
  })

  const post = (body: string, contentType: string = DidCommMimeType.V0) =>
    fetch(endpoint, { method: 'POST', headers: { 'content-type': contentType }, body })
  const queued = () => queue.getAvailableMessageCount(mediator.context, { connectionId: mediatedConnectionId })

  test('a forward that fits is acknowledged with 200 and stored', async () => {
    const before = await queued()
    const { body } = await forwardFor(250_000)
    const response = await post(body)
    expect(response.status).toBe(200)
    expect(await response.text()).toBe('')
    await expect.poll(queued).toBe(before + 1)
  })

  test('a forward whose message is too large is answered 413, and nothing is stored or handled', async () => {
    const before = await queued()
    logger.warnings.length = 0
    const { body, storedMessage } = await forwardFor(320_000)
    // The message itself is what makes the item too large, as the issue describes.
    expect(queuedMessageItemBytes({ recipientDids: [], encryptedMessage: storedMessage })).toBeGreaterThan(
      DYNAMODB_MAX_ITEM_BYTES
    )

    const response = await post(body)
    expect(response.status).toBe(413)
    expect(await response.text()).toBe('The message is too large for the mediator to store for its recipient')
    expect(await queued()).toBe(before)

    const refusals = logger.warnings.filter((w) => w.message.startsWith('Refused a forward'))
    expect(refusals).toHaveLength(1)
    expect(refusals[0].data).toMatchObject({
      maxItemBytes: DYNAMODB_MAX_ITEM_BYTES,
      transport: 'http',
      senderInformed: true,
    })
    expect(refusals[0].data?.itemBytes).toBeGreaterThan(DYNAMODB_MAX_ITEM_BYTES)
    // Sizes only, never the message.
    expect(JSON.stringify(refusals[0])).not.toContain(storedMessage.ciphertext.slice(0, 64))
  })

  test('the mediator keeps working after a refusal', async () => {
    const before = await queued()
    const response = await post((await forwardFor(1_000)).body)
    expect(response.status).toBe(200)
    await expect.poll(queued).toBe(before + 1)
  })

  test('a content type that is not DIDComm is still refused with 415', async () => {
    const response = await post('{}', 'application/json')
    expect(response.status).toBe(415)
  })
})
