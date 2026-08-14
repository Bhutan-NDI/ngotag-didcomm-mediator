import '@openwallet-foundation/askar-nodejs'
import {
  DidCommOutOfBandRecord,
  DidCommOutOfBandRepository,
  DidCommOutOfBandRole,
  DidCommOutOfBandState,
} from '@credo-ts/didcomm'
import { createAgent, MediatorAgent, shutdownAgent } from './agent.js'
import { config } from './config.js'
import { shutdownTelemetry } from './telemetry/sdk.js'

function logInvitationUrl(agent: MediatorAgent, outOfBandRecord: DidCommOutOfBandRecord) {
  const httpEndpoint = config.agentEndpoints.find((e) => e.startsWith('http'))
  if (!httpEndpoint) {
    throw new Error('No HTTP endpoint configured for invitation generation')
  }

  const mediatorInvitationUrlLong = outOfBandRecord.outOfBandInvitation.toUrl({
    domain: config.invitationUrl,
  })

  agent.config.logger.info(`Out of band invitation url:\n\n\t${mediatorInvitationUrlLong}`)
}

async function createMediatorInvitation(agent: MediatorAgent) {
  return agent.didcomm.oob.createInvitation({
    multiUseInvitation: true,
    goalCode: config.invitationGoalCode,
    goal: 'Mediator Invitation',
  })
}

let runningAgent: MediatorAgent | undefined
let shutdownPromise: Promise<void> | undefined

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  shutdownPromise ??= (async () => {
    runningAgent?.config.logger.info(`Received ${signal}; shutting down`)
    if (runningAgent) await shutdownAgent(runningAgent)
    await shutdownTelemetry()
  })()
  return shutdownPromise
}

process.once('SIGINT', () => {
  void shutdown('SIGINT').catch((error) => {
    console.error('Graceful shutdown failed', error)
    process.exitCode = 1
  })
})
process.once('SIGTERM', () => {
  void shutdown('SIGTERM').catch((error) => {
    console.error('Graceful shutdown failed', error)
    process.exitCode = 1
  })
})

async function main(): Promise<void> {
  const agent = await createAgent()
  runningAgent = agent
  agent.config.logger.info('Agent started')

  if (config.createNewInvitation) {
    agent.config.logger.info('Recreating out of band invitation')
    const outOfBandRecord = await createMediatorInvitation(agent)
    logInvitationUrl(agent, outOfBandRecord)
    return
  }

  const oobRepo = agent.dependencyManager.resolve(DidCommOutOfBandRepository)
  const outOfBandRecords = await oobRepo.findByQuery(agent.context, {
    state: DidCommOutOfBandState.AwaitResponse,
    role: DidCommOutOfBandRole.Sender,
  })

  let outOfBandRecord = outOfBandRecords
    .filter((oobRecord) => oobRecord.reusable)
    .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())[0]

  if (outOfBandRecord) {
    agent.config.logger.info('Reusing existing out of band invitation')
  } else {
    agent.config.logger.warn('No reusable out of band invitation found, creating a new one')
    outOfBandRecord = await createMediatorInvitation(agent)
  }

  logInvitationUrl(agent, outOfBandRecord)
}

void main().catch(async (error) => {
  console.error('Mediator failed to start', error)
  process.exitCode = 1
  try {
    if (runningAgent) await shutdownAgent(runningAgent)
    await shutdownTelemetry()
  } catch (shutdownError) {
    console.error('Shutdown after startup failure failed', shutdownError)
  }
})
