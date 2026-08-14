import { type DidCommOutboundPackage, DidCommWsOutboundTransport } from '@credo-ts/didcomm'

import {
  deliveryCounter,
  deliveryDuration,
  elapsedSeconds,
  getJweFingerprint,
  hashIdentifier,
  SpanKind,
  withSpan,
} from '../telemetry/api.js'

export class InstrumentedWsOutboundTransport extends DidCommWsOutboundTransport {
  public override async sendMessage(outboundPackage: DidCommOutboundPackage): Promise<void> {
    const startedAt = process.hrtime.bigint()
    await withSpan(
      'didcomm.delivery.transport',
      {
        kind: SpanKind.PRODUCER,
        attributes: {
          'didcomm.delivery.path': 'service_endpoint',
          'didcomm.transport': 'websocket',
          'server.address': getServerAddress(outboundPackage.endpoint),
          'didcomm.connection.id_hash': hashIdentifier(outboundPackage.connectionId),
          'didcomm.message.fingerprint': getJweFingerprint(outboundPackage.payload),
        },
      },
      async () => {
        let outcome = 'ok'
        try {
          await super.sendMessage(outboundPackage)
        } catch (error) {
          outcome = 'error'
          throw error
        } finally {
          const attributes = { path: 'service_endpoint', transport: 'websocket', outcome }
          deliveryCounter.add(1, attributes)
          deliveryDuration.record(elapsedSeconds(startedAt), attributes)
        }
      }
    )
  }
}

function getServerAddress(endpoint: string | undefined): string | undefined {
  try {
    return endpoint ? new URL(endpoint).hostname : undefined
  } catch {
    return undefined
  }
}
