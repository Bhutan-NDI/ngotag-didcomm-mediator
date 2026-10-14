import {
  DidCommEventTypes,
  DidCommForwardMessage,
  DidCommMessagePickupEventTypes,
  type DidCommMessagePickupLiveSessionSavedEvent,
  type DidCommMessageProcessedEvent,
  type DidCommMessageSentEvent,
  type MessagePickupCompletedEvent,
  type MessagePickupLiveSessionRemovedEvent,
} from '@credo-ts/didcomm'

import type { MediatorAgent } from '../agent.js'
import {
  activeSpan,
  deliveryCounter,
  getJweFingerprint,
  getProtocolAttributes,
  hashIdentifier,
  liveSessionEvents,
  messageProcessedCounter,
  pickupCompletedCounter,
} from '../telemetry/api.js'

const METRIC_PROTOCOLS = new Set([
  'connections',
  'coordinate-mediation',
  'messagepickup',
  'out-of-band',
  'routing',
  'trust-ping',
  'trust_ping',
])

export function wireEventInstrumentation(agent: MediatorAgent): void {
  agent.events.on<DidCommMessageProcessedEvent>(DidCommEventTypes.DidCommMessageProcessed, (event) => {
    try {
      const { message, encryptedMessage, connection } = event.payload
      const protocolAttributes = getProtocolAttributes(message.type)
      const span = activeSpan()

      span?.setAttributes({
        ...protocolAttributes,
        'didcomm.connection.id_hash': hashIdentifier(connection?.id),
      })

      if (message instanceof DidCommForwardMessage) {
        span?.addEvent('didcomm.forward.envelope.bridge', {
          'didcomm.message.outer_fingerprint': getJweFingerprint(encryptedMessage) ?? '',
          'didcomm.message.inner_fingerprint': getJweFingerprint(message.message) ?? '',
          'didcomm.recipient_key.id_hash': hashIdentifier(message.to) ?? '',
        })
      }

      const protocol = protocolAttributes['didcomm.protocol.name']
      messageProcessedCounter.add(1, {
        protocol: typeof protocol === 'string' && METRIC_PROTOCOLS.has(protocol) ? protocol : 'other',
      })
    } catch {
      // Telemetry must never affect message processing.
    }
  })

  agent.events.on<DidCommMessageSentEvent>(DidCommEventTypes.DidCommMessageSent, (event) => {
    try {
      const status = String(event.payload.status)
      activeSpan()?.addEvent('didcomm.message.sent', {
        'didcomm.delivery.status': status,
        'didcomm.message.type': event.payload.message.message?.type ?? '',
      })
      deliveryCounter.add(1, { path: 'coordination', status })
    } catch {
      // Telemetry must never affect message processing.
    }
  })

  agent.events.on<DidCommMessagePickupLiveSessionSavedEvent>(
    DidCommMessagePickupEventTypes.LiveSessionSaved,
    (event) => {
      liveSessionEvents.add(1, {
        event: 'saved',
        role: String(event.payload.session.role),
        protocol_version: String(event.payload.session.protocolVersion),
      })
    }
  )

  agent.events.on<MessagePickupLiveSessionRemovedEvent>(DidCommMessagePickupEventTypes.LiveSessionRemoved, (event) => {
    liveSessionEvents.add(1, {
      event: 'removed',
      role: String(event.payload.session.role),
      protocol_version: String(event.payload.session.protocolVersion),
    })
  })

  agent.events.on<MessagePickupCompletedEvent>(DidCommMessagePickupEventTypes.MessagePickupCompleted, () => {
    pickupCompletedCounter.add(1)
    activeSpan()?.addEvent('didcomm.pickup.completed')
  })
}
