import { DidCommHttpOutboundTransport, type DidCommOutboundPackage } from '@credo-ts/didcomm'

import {
  deliveryCounter,
  deliveryDuration,
  getJweFingerprint,
  getServerAddress,
  hashIdentifier,
  instrumentOperation,
  SpanKind,
  withQueuedTelemetryContext,
} from '../telemetry/api.js'

export class InstrumentedHttpOutboundTransport extends DidCommHttpOutboundTransport {
  public override async sendMessage(outboundPackage: DidCommOutboundPackage): Promise<void> {
    await withQueuedTelemetryContext(outboundPackage.connectionId, (links) =>
      instrumentOperation('didcomm.delivery.transport', {
        span: {
          kind: SpanKind.PRODUCER,
          links,
          attributes: {
            'didcomm.delivery.path': 'service_endpoint',
            'didcomm.transport': 'http',
            'server.address': getServerAddress(outboundPackage.endpoint),
            'didcomm.connection.id_hash': hashIdentifier(outboundPackage.connectionId),
            'didcomm.message.fingerprint': getJweFingerprint(outboundPackage.payload),
          },
        },
        callback: async () => super.sendMessage(outboundPackage),
        record: (outcome, elapsed) => {
          const attributes = { path: 'service_endpoint', transport: 'http', outcome }
          deliveryCounter.add(1, attributes)
          deliveryDuration.record(elapsed, attributes)
        },
      })
    )
  }
}
