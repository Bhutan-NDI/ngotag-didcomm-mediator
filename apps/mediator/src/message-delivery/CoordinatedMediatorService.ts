import { EventEmitter, InjectionSymbols, inject, injectable, type Logger } from '@credo-ts/core'
import {
  DidCommConnectionService,
  type DidCommForwardMessage,
  type DidCommInboundMessageContext,
  DidCommMediationRepository,
  DidCommMediatorRoutingRepository,
  DidCommMediatorService,
} from '@credo-ts/didcomm'

import { runInForwardDeliveryContext } from './forwardDeliveryContext.js'

/**
 * Coordinates queue events emitted by Credo's forward-message path. In
 * QueueAndLiveModeDelivery, Credo performs local delivery after queueing, so
 * the Redis queue-event listener waits for that attempt before draining again.
 */
@injectable()
export class CoordinatedMediatorService extends DidCommMediatorService {
  public constructor(
    mediationRepository: DidCommMediationRepository,
    mediatorRoutingRepository: DidCommMediatorRoutingRepository,
    eventEmitter: EventEmitter,
    @inject(InjectionSymbols.Logger) logger: Logger,
    connectionService: DidCommConnectionService
  ) {
    super(mediationRepository, mediatorRoutingRepository, eventEmitter, logger, connectionService)
  }

  public override processForwardMessage(
    messageContext: DidCommInboundMessageContext<DidCommForwardMessage>
  ): Promise<void> {
    return runInForwardDeliveryContext(() => super.processForwardMessage(messageContext))
  }
}
