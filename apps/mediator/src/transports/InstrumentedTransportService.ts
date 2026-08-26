import { AgentContext, injectable } from '@credo-ts/core'
import { type DidCommEncryptedMessage, DidCommTransportService, type DidCommTransportSession } from '@credo-ts/didcomm'

import {
  deliveryCounter,
  deliveryDuration,
  getJweFingerprint,
  hashIdentifier,
  instrumentOperation,
  SpanKind,
  withQueuedTelemetryContext,
} from '../telemetry/api.js'

@injectable()
export class InstrumentedTransportService extends DidCommTransportService {
  private readonly instrumentedSessions = new WeakSet<DidCommTransportSession>()

  public override saveSession(session: DidCommTransportSession): void {
    this.instrumentSessionSend(session)
    super.saveSession(session)
  }

  private instrumentSessionSend(session: DidCommTransportSession): void {
    if (this.instrumentedSessions.has(session)) return
    this.instrumentedSessions.add(session)

    const originalSend = session.send.bind(session)
    session.send = async (agentContext: AgentContext, encryptedMessage: DidCommEncryptedMessage): Promise<void> => {
      const transport = session.type.toLowerCase()
      await withQueuedTelemetryContext(session.connectionId, (links) =>
        instrumentOperation('didcomm.delivery.live', {
          span: {
            kind: SpanKind.PRODUCER,
            links,
            attributes: {
              'didcomm.delivery.path': 'live_session',
              'didcomm.transport': transport,
              'didcomm.connection.id_hash': hashIdentifier(session.connectionId),
              'didcomm.transport_session.id_hash': hashIdentifier(session.id),
              'didcomm.message.fingerprint': getJweFingerprint(encryptedMessage),
            },
          },
          callback: async () => originalSend(agentContext, encryptedMessage),
          record: (outcome, elapsed) => {
            const attributes = { path: 'live_session', transport, outcome }
            deliveryCounter.add(1, attributes)
            deliveryDuration.record(elapsed, attributes)
          },
        })
      )
    }
  }
}
