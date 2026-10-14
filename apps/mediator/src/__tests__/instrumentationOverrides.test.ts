import { Agent, DependencyManager, InjectionSymbols } from '@credo-ts/core'
import {
  DidCommMediatorService,
  DidCommMessageReceiver,
  DidCommMessageSender,
  DidCommModule,
  DidCommTransportService,
} from '@credo-ts/didcomm'
import { agentDependencies } from '@credo-ts/node'
import { describe, expect, it } from 'vitest'

describe('InstrumentationOverridesModule', () => {
  it('gives the message sender the transport service that stores inbound sessions', async () => {
    process.env = {
      LOG_LEVEL: 'off',
      ASKAR__STORE_ID: 'test',
      ASKAR__STORE_KEY: 'test',
    }
    const { InstrumentationOverridesModule } = await import('../instrumentation/InstrumentationOverridesModule.js')
    const { InstrumentedMediatorService } = await import('../instrumentation/InstrumentedMediatorService.js')
    const { InstrumentedMessageReceiver } = await import('../instrumentation/InstrumentedMessageReceiver.js')
    const { InstrumentedTransportService } = await import('../transports/InstrumentedTransportService.js')

    // Constructing the agent is all this test needs, so storage is never used.
    const dependencyManager = new DependencyManager()
    dependencyManager.registerInstance(InjectionSymbols.StorageService, {})
    new Agent(
      {
        config: {},
        dependencies: agentDependencies,
        modules: {
          didcomm: new DidCommModule({ mediator: { autoAcceptMediationRequests: true } }),
          instrumentationOverrides: new InstrumentationOverridesModule(),
        },
      },
      dependencyManager
    )

    const transportService = dependencyManager.resolve(DidCommTransportService)
    const messageSender = dependencyManager.resolve(DidCommMessageSender) as unknown as {
      transportService: DidCommTransportService
    }

    expect(transportService).toBeInstanceOf(InstrumentedTransportService)
    expect(messageSender.transportService).toBe(transportService)
    expect(dependencyManager.resolve(DidCommMessageReceiver)).toBeInstanceOf(InstrumentedMessageReceiver)
    expect(dependencyManager.resolve(DidCommMediatorService)).toBeInstanceOf(InstrumentedMediatorService)
  })
})
