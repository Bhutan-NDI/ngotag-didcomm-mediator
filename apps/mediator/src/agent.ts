import type { Socket } from 'node:net'
import { AskarModule, AskarStoreDuplicateError } from '@credo-ts/askar'
import { Agent } from '@credo-ts/core'
import {
  DidCommMediatorService,
  DidCommMessageReceiver,
  DidCommMimeType,
  DidCommModule,
  DidCommOutOfBandRole,
  DidCommOutOfBandState,
  DidCommTransportService,
} from '@credo-ts/didcomm'
import { DidCommPushNotificationsFcmModule } from '@credo-ts/didcomm-push-notifications'
import { agentDependencies, DidCommHttpInboundTransport, DidCommWsInboundTransport } from '@credo-ts/node'
import express, { type Express } from 'express'
import Redis from 'ioredis'
import type { WebSocket } from 'ws'
import { WebSocketServer } from 'ws'
import { loadAskar } from './config/askarLoader.js'
import { loadCacheStorage } from './config/cacheLoader.js'
import { ExtendedQueueTransportRepository, loadMessagePickupStorage } from './config/messagePickupLoader.js'
import { loadPushNotificationSender } from './config/pushNotificationLoader.js'
import { loadRedisMessageDelivery } from './config/redisMessageDeliveryLoader.js'
import { loadStorage } from './config/storageLoader.js'
import { config, logger } from './config.js'
import { wireEventInstrumentation } from './instrumentation/eventInstrumentation.js'
import { InstrumentedMediatorService } from './instrumentation/InstrumentedMediatorService.js'
import { InstrumentedMessageReceiver } from './instrumentation/InstrumentedMessageReceiver.js'
import { InstrumentedQueueTransportRepository } from './instrumentation/InstrumentedQueueTransportRepository.js'
import {
  registerDbPoolAccessor,
  registerQueueAccessor,
  wsSessionClosed,
  wsSessionOpened,
} from './instrumentation/metrics.js'
import { StorageServiceMessageQueue } from './storage/StorageMessageQueue.js'
import { InstrumentedHttpOutboundTransport } from './transports/InstrumentedHttpOutboundTransport.js'
import { InstrumentedTransportService } from './transports/InstrumentedTransportService.js'
import { InstrumentedWsOutboundTransport } from './transports/InstrumentedWsOutboundTransport.js'
import { startWsHeartbeat } from './transports/wsHeartbeat.js'

async function createModules({
  queueTransportRepository,
  app,
  socketServer,
}: {
  queueTransportRepository: ExtendedQueueTransportRepository
  app: Express
  socketServer: WebSocketServer
}) {
  const modules = {
    didcomm: new DidCommModule({
      endpoints: config.agentEndpoints,
      useDidSovPrefixWhereAllowed: true,
      didCommMimeType: DidCommMimeType.V0,
      queueTransportRepository,

      transports: {
        inbound: [
          new DidCommHttpInboundTransport({ app, port: config.agentPort }),
          new DidCommWsInboundTransport({ server: socketServer }),
        ],
        outbound: [new InstrumentedHttpOutboundTransport(), new InstrumentedWsOutboundTransport()],
      },

      connections: {
        autoAcceptConnections: true,
      },
      mediator: {
        autoAcceptMediationRequests: true,
        messageForwardingStrategy: config.messagePickup.forwardingStrategy,
      },

      // Protocols not needed for mediator
      basicMessages: false,
      credentials: false,
      proofs: false,
    }),
    pushNotificationsFcm: new DidCommPushNotificationsFcmModule(),
  } as const

  return modules
}

function instrumentSocketServer(socketServer: WebSocketServer): void {
  socketServer.on('connection', (socket: WebSocket) => {
    wsSessionOpened()
    socket.on('close', () => {
      wsSessionClosed()
    })
  })
}

interface RuntimeHandles {
  abortController: AbortController
  stopCache: () => Promise<void>
  stopHeartbeat: () => void
  stopQueue: () => Promise<void>
  stopRedisDelivery: () => Promise<void>
}

const runtimeHandles = new WeakMap<Agent, RuntimeHandles>()

export async function createAgent() {
  // We create our own instance of express here. This is not required
  // but allows use to use the same server (and port) for both WebSockets and HTTP
  const app = express()
  const socketServer = new WebSocketServer({ noServer: true })
  const redisClient = config.cache.type === 'redis' ? new Redis.default(config.cache.redisUrl) : undefined
  const abortController = new AbortController()

  instrumentSocketServer(socketServer)

  // Flow fix (independent of instrumentation): keep idle live-mode WebSocket
  // connections alive so an intermediary idle timeout doesn't silently drop them
  // and tear down the live pickup session. Set WS_HEARTBEAT_INTERVAL_SECONDS=0 to
  // disable for fully-stock WS behaviour.
  const stopHeartbeat = startWsHeartbeat(socketServer, config.wsHeartbeatIntervalSeconds * 1000)

  // Wrap whichever pickup-queue backend is configured (credo | postgres | dynamodb)
  // so queue operations have the same telemetry regardless of backend.
  const baseQueueTransportRepository = await loadMessagePickupStorage()
  const queueTransportRepository = new InstrumentedQueueTransportRepository(
    baseQueueTransportRepository,
    config.messagePickup.storage.type
  )
  const storageModules = loadStorage()
  const askarModules = await loadAskar()
  const cacheModules = loadCacheStorage({
    redisClient,
  })

  const modules = {
    ...storageModules,
    ...askarModules,
    ...cacheModules,
    ...(await createModules({
      queueTransportRepository,
      app,
      socketServer,
    })),
  } as const

  const agent = new Agent<typeof modules & { askar: AskarModule }>({
    config: {
      logger,
      autoUpdateStorageOnStartup: true,
    },
    dependencies: agentDependencies,
    modules: modules as typeof modules & { askar: AskarModule },
  })

  // Register before initialization so every message-processing path resolves the
  // traced singleton, including live delivery paths that bypass outbound transports.
  agent.dependencyManager.registerSingleton(DidCommMessageReceiver, InstrumentedMessageReceiver)
  agent.dependencyManager.registerSingleton(DidCommTransportService, InstrumentedTransportService)
  agent.dependencyManager.registerSingleton(DidCommMediatorService, InstrumentedMediatorService)

  // Added health check endpoint
  app.get('/health', async (_req, res) => {
    res.sendStatus(202)
  })

  app.get('/invite', async (req, res) => {
    if (!req.query._oobid || typeof req.query._oobid !== 'string') {
      return res.status(400).send('Missing or invalid _oobid')
    }

    const outOfBandRecord = await agent.didcomm.oob.findById(req.query._oobid)

    if (
      !outOfBandRecord ||
      outOfBandRecord.role !== DidCommOutOfBandRole.Sender ||
      outOfBandRecord.state !== DidCommOutOfBandState.AwaitResponse
    ) {
      return res.status(400).send(`No invitation found for _oobid ${req.query._oobid}`)
    }

    return res.send(outOfBandRecord.outOfBandInvitation.toJSON())
  })

  try {
    await agent.modules.askar.provisionStore()
    agent.config.logger.info('Provisioned store')
  } catch (error) {
    if (error instanceof AskarStoreDuplicateError) {
      agent.config.logger.info('Store already exists')
    } else {
      agent.config.logger.error('Error provisioning store', {
        error,
      })
    }
  }

  // Optionally initialize queue transport repository
  // TODO: We should refactor this so it's handled by the agent.initialize (using a module?)
  await queueTransportRepository.initialize?.(agent)

  await agent.initialize()

  const inboundTransport = agent.didcomm.config.inboundTransports.find(
    (transport) => transport instanceof DidCommHttpInboundTransport
  )

  inboundTransport?.server?.on('listening', () => {
    logger.info(`Agent listening on port ${config.agentPort}`)
  })

  inboundTransport?.server?.on('error', (err) => {
    logger.error(`Agent failed to start on port ${config.agentPort}`, err)
  })

  inboundTransport?.server?.on('close', () => {
    logger.info(`Agent stopped listening on port ${config.agentPort}`)
  })

  // When an 'upgrade' to WS is made on our http server, we forward the
  // request to the WS server
  inboundTransport?.server?.on('upgrade', (request, socket, head) => {
    socketServer.handleUpgrade(request, socket as Socket, head, (socket) => {
      socketServer.emit('connection', socket, request)
    })
  })

  if (baseQueueTransportRepository instanceof StorageServiceMessageQueue) {
    registerQueueAccessor(() => baseQueueTransportRepository.getQueueStats(agent.context))
  }
  if (
    'getPoolStats' in baseQueueTransportRepository &&
    typeof baseQueueTransportRepository.getPoolStats === 'function'
  ) {
    const getPoolStats = baseQueueTransportRepository.getPoolStats.bind(baseQueueTransportRepository)
    registerDbPoolAccessor('pickup', getPoolStats)
  }
  wireEventInstrumentation(agent)

  await loadPushNotificationSender(agent)
  const stopRedisDelivery = await loadRedisMessageDelivery({
    agent,
    abortSignal: abortController.signal,
    // FIXME: somehow reusing the same Redis client makes everything fail
    /* redisClient */
  })

  runtimeHandles.set(agent, {
    abortController,
    stopCache: async () => {
      if (redisClient && redisClient.status !== 'end') await redisClient.quit()
    },
    stopHeartbeat,
    stopQueue: () => queueTransportRepository.shutdown(agent.context),
    stopRedisDelivery,
  })

  return agent
}

export async function shutdownAgent(agent: Agent): Promise<void> {
  const handles = runtimeHandles.get(agent)
  if (handles) {
    handles.abortController.abort()
    handles.stopHeartbeat()
    await handles.stopRedisDelivery()
  }
  try {
    await agent.shutdown()
  } finally {
    try {
      await handles?.stopQueue()
    } finally {
      await handles?.stopCache()
      runtimeHandles.delete(agent)
    }
  }
}

export type MediatorAgent = Agent<Awaited<ReturnType<typeof createModules>>>
