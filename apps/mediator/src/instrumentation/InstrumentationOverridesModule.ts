import type { DependencyManager, Module } from '@credo-ts/core'
import { DidCommMediatorService, DidCommMessageReceiver, DidCommTransportService } from '@credo-ts/didcomm'

import { InstrumentedTransportService } from '../transports/InstrumentedTransportService.js'
import { InstrumentedMediatorService } from './InstrumentedMediatorService.js'
import { InstrumentedMessageReceiver } from './InstrumentedMessageReceiver.js'

// Must be the last module: the Agent constructor resolves every module API right after
// registration, so an override registered later leaves those APIs holding a second instance.
export class InstrumentationOverridesModule implements Module {
  public register(dependencyManager: DependencyManager): void {
    dependencyManager.registerSingleton(DidCommMessageReceiver, InstrumentedMessageReceiver)
    dependencyManager.registerSingleton(DidCommTransportService, InstrumentedTransportService)
    dependencyManager.registerSingleton(DidCommMediatorService, InstrumentedMediatorService)
  }
}
