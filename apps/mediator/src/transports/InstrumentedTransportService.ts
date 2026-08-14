import { AgentContext, injectable } from '@credo-ts/core'
import { type DidCommEncryptedMessage, DidCommTransportService, type DidCommTransportSession } from '@credo-ts/didcomm'

import {
  deliveryCounter,
  deliveryDuration,
  elapsedSeconds,
  getJweFingerprint,
  hashIdentifier,
  SpanKind,
  withSpan,
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
      const startedAt = process.hrtime.bigint()
      const transport = session.type.toLowerCase()
      await withSpan(
        'didcomm.delivery.live',
        {
          kind: SpanKind.PRODUCER,
          attributes: {
            'didcomm.delivery.path': 'live_session',
            'didcomm.transport': transport,
            'didcomm.connection.id_hash': hashIdentifier(session.connectionId),
            'didcomm.transport_session.id_hash': hashIdentifier(session.id),
            'didcomm.message.fingerprint': getJweFingerprint(encryptedMessage),
          },
        },
        async () => {
          let outcome = 'ok'
          try {
            await originalSend(agentContext, encryptedMessage)
          } catch (error) {
            outcome = 'error'
            throw error
          } finally {
            const attributes = { path: 'live_session', transport, outcome }
            deliveryCounter.add(1, attributes)
            deliveryDuration.record(elapsedSeconds(startedAt), attributes)
          }
        }
      )
    }
  }
}
