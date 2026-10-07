import { EventEmitter, InjectionSymbols, inject, injectable, type Logger } from '@credo-ts/core'
import {
  DidCommConnectionService,
  type DidCommForwardMessage,
  type DidCommInboundMessageContext,
  DidCommMediationRepository,
  DidCommMediatorRoutingRepository,
  DidCommMediatorService,
} from '@credo-ts/didcomm'

import { config } from '../config.js'
import {
  forwardCounter,
  forwardDuration,
  getJweFingerprint,
  hashIdentifier,
  instrumentOperation,
  SpanKind,
} from '../telemetry/api.js'

@injectable()
export class InstrumentedMediatorService extends DidCommMediatorService {
  public constructor(
    mediationRepository: DidCommMediationRepository,
    mediatorRoutingRepository: DidCommMediatorRoutingRepository,
    eventEmitter: EventEmitter,
    @inject(InjectionSymbols.Logger) logger: Logger,
    connectionService: DidCommConnectionService
  ) {
    super(mediationRepository, mediatorRoutingRepository, eventEmitter, logger, connectionService)
  }

  public override async processForwardMessage(
    messageContext: DidCommInboundMessageContext<DidCommForwardMessage>
  ): Promise<void> {
    const strategy = config.messagePickup.forwardingStrategy
    await instrumentOperation('didcomm.forward', {
      span: {
        kind: SpanKind.INTERNAL,
        attributes: {
          'didcomm.forwarding.strategy': strategy,
          'didcomm.message.fingerprint': getJweFingerprint(messageContext.message.message),
          'didcomm.recipient_key.id_hash': hashIdentifier(messageContext.message.to),
        },
      },
      callback: async (span) => {
        span.addEvent('didcomm.forward.strategy.selected', { strategy })
        await super.processForwardMessage(messageContext)
      },
      errorOutcome: 'undeliverable',
      record: (outcome, elapsed) => {
        const attributes = { strategy, outcome }
        forwardCounter.add(1, attributes)
        forwardDuration.record(elapsed, attributes)
      },
    })
  }
}
