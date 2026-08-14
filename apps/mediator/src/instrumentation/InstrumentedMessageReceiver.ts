import { AgentContextProvider, InjectionSymbols, inject, injectable, type Logger } from '@credo-ts/core'
import {
  DidCommConnectionService,
  DidCommDispatcher,
  DidCommEnvelopeService,
  DidCommMessageHandlerRegistry,
  DidCommMessageReceiver,
  DidCommMessageSender,
  DidCommTransportService,
} from '@credo-ts/didcomm'

import {
  elapsedSeconds,
  getJweFingerprint,
  hashIdentifier,
  messageProcessDuration,
  SpanKind,
  withSpan,
} from '../telemetry/api.js'

@injectable()
export class InstrumentedMessageReceiver extends DidCommMessageReceiver {
  public constructor(
    envelopeService: DidCommEnvelopeService,
    transportService: DidCommTransportService,
    messageSender: DidCommMessageSender,
    connectionService: DidCommConnectionService,
    dispatcher: DidCommDispatcher,
    messageHandlerRegistry: DidCommMessageHandlerRegistry,
    @inject(InjectionSymbols.AgentContextProvider) agentContextProvider: AgentContextProvider,
    @inject(InjectionSymbols.Logger) logger: Logger
  ) {
    super(
      envelopeService,
      transportService,
      messageSender,
      connectionService,
      dispatcher,
      messageHandlerRegistry,
      agentContextProvider,
      logger
    )
  }

  public override async receiveMessage(
    inboundMessage: unknown,
    options?: Parameters<DidCommMessageReceiver['receiveMessage']>[1]
  ): Promise<void> {
    const transport = options?.session?.type?.toLowerCase() ?? 'internal'
    const startedAt = process.hrtime.bigint()
    const fingerprint = getJweFingerprint(inboundMessage)

    await withSpan(
      'didcomm.message.process',
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          'messaging.system': 'didcomm',
          'messaging.operation.name': 'process',
          'messaging.operation.type': 'process',
          'didcomm.transport': transport,
          'didcomm.encrypted': fingerprint !== undefined,
          'didcomm.message.fingerprint': fingerprint,
          'didcomm.connection.id_hash': hashIdentifier(options?.connection?.id),
        },
      },
      async () => {
        let outcome = 'ok'
        try {
          await super.receiveMessage(inboundMessage, options)
        } catch (error) {
          outcome = 'error'
          throw error
        } finally {
          messageProcessDuration.record(elapsedSeconds(startedAt), { transport, outcome })
        }
      }
    )
  }
}
