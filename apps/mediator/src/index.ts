import '@openwallet-foundation/askar-nodejs'
import {
  DidCommOutOfBandRecord,
  DidCommOutOfBandRepository,
  DidCommOutOfBandRole,
  DidCommOutOfBandState,
} from '@credo-ts/didcomm'
import { LogLevel } from '@credo-ts/core'
import { createAgent, MediatorAgent } from './agent.js'
import { config } from './config.js'
import { emitStructured } from './logger/StructuredLogger.js'

process.on('unhandledRejection', (reason) => {
  emitStructured(LogLevel.error, {
    hop: 'mediator.process.unhandled_rejection',
    notes: reason instanceof Error ? reason.stack ?? reason.message : String(reason),
  })
})

process.on('uncaughtException', (error) => {
  emitStructured(LogLevel.error, {
    hop: 'mediator.process.uncaught_exception',
    notes: error.stack ?? error.message,
  })
})

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

void createAgent().then(async (agent) => {
  agent.config.logger.info('Agent started')

  if (config.createNewInvitation) {
    agent.config.logger.info('Recreating out of band invitation')
    const outOfBandRecord = await createMediatorInvitation(agent)
    return logInvitationUrl(agent, outOfBandRecord)
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
}).catch((error) => {
  emitStructured(LogLevel.error, {
    hop: 'mediator.process.uncaught_exception',
    notes: error instanceof Error ? error.stack ?? error.message : String(error),
  })
})
