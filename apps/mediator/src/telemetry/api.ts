import { createHash } from 'node:crypto'
import {
  type Attributes,
  context,
  metrics,
  propagation,
  type Span,
  SpanKind,
  type SpanOptions,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api'

const INSTRUMENTATION_NAME = 'didcomm-mediator'

export const tracer = trace.getTracer(INSTRUMENTATION_NAME)
export const meter = metrics.getMeter(INSTRUMENTATION_NAME)

export const messageProcessedCounter = meter.createCounter('didcomm.message.processed', {
  description: 'Number of DIDComm messages processed by the mediator',
})
export const messageProcessDuration = meter.createHistogram('didcomm.message.process.duration', {
  description: 'Time spent processing a DIDComm message',
  unit: 's',
})
export const forwardCounter = meter.createCounter('didcomm.forward.outcomes', {
  description: 'DIDComm forwarding outcomes',
})
export const forwardDuration = meter.createHistogram('didcomm.forward.duration', {
  description: 'Time spent forwarding a DIDComm message',
  unit: 's',
})
export const queueOperationCounter = meter.createCounter('didcomm.queue.operations', {
  description: 'Pickup queue operations',
})
export const queueOperationDuration = meter.createHistogram('didcomm.queue.operation.duration', {
  description: 'Pickup queue operation duration',
  unit: 's',
})
export const queueBatchSize = meter.createHistogram('didcomm.queue.batch.size', {
  description: 'Number of messages returned by a pickup queue operation',
  unit: '{message}',
})
export const queueMessageAge = meter.createHistogram('didcomm.queue.message.age', {
  description: 'Age of a message when it is taken from the pickup queue',
  unit: 's',
})
export const deliveryCounter = meter.createCounter('didcomm.delivery.outcomes', {
  description: 'DIDComm delivery outcomes',
})
export const deliveryDuration = meter.createHistogram('didcomm.delivery.duration', {
  description: 'DIDComm delivery operation duration',
  unit: 's',
})
export const websocketSessions = meter.createUpDownCounter('didcomm.websocket.sessions', {
  description: 'Number of active inbound WebSocket sessions',
  unit: '{session}',
})
export const websocketSessionEvents = meter.createCounter('didcomm.websocket.session.events', {
  description: 'WebSocket session lifecycle events',
})
export const liveSessionEvents = meter.createCounter('didcomm.pickup.live_session.events', {
  description: 'DIDComm pickup live-session lifecycle events',
})
export const pickupCompletedCounter = meter.createCounter('didcomm.pickup.completed', {
  description: 'Completed DIDComm pickup protocol exchanges',
})
export const pushNotificationCounter = meter.createCounter('didcomm.push.outcomes', {
  description: 'Push notification outcomes',
})
export const pushNotificationDuration = meter.createHistogram('didcomm.push.duration', {
  description: 'Push notification duration',
  unit: 's',
})

export type TelemetryCarrier = Record<string, string>

export function elapsedSeconds(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1e9
}

export function hashIdentifier(value: string | undefined | null): string | undefined {
  if (!value) return undefined
  return createHash('sha256').update(value).digest('hex').slice(0, 16)
}

export function getJweFingerprint(payload: unknown): string | undefined {
  try {
    const parsed = typeof payload === 'string' ? (JSON.parse(payload) as Record<string, unknown>) : payload
    if (!parsed || typeof parsed !== 'object') return undefined
    const iv = (parsed as Record<string, unknown>).iv
    return typeof iv === 'string' ? hashIdentifier(iv) : undefined
  } catch {
    return undefined
  }
}

export function getProtocolAttributes(messageType: string | undefined): Attributes {
  if (!messageType) return {}

  try {
    const url = new URL(messageType)
    const parts = url.pathname.split('/').filter(Boolean)
    const versionIndex = parts.findIndex((part) => /^\d+(?:\.\d+)*$/.test(part))
    return {
      'didcomm.message.type': messageType,
      ...(versionIndex > 0 ? { 'didcomm.protocol.name': parts[versionIndex - 1] } : {}),
      ...(versionIndex >= 0 ? { 'didcomm.protocol.version': parts[versionIndex] } : {}),
    }
  } catch {
    return { 'didcomm.message.type': messageType }
  }
}

export async function withSpan<T>(
  name: string,
  options: SpanOptions,
  callback: (span: Span) => Promise<T>
): Promise<T> {
  return tracer.startActiveSpan(name, options, async (span) => {
    try {
      return await callback(span)
    } catch (error) {
      if (error instanceof Error) span.recordException(error)
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message.slice(0, 256) : 'Unknown error',
      })
      throw error
    } finally {
      span.end()
    }
  })
}

export function activeSpan(): Span | undefined {
  return trace.getActiveSpan()
}

export function injectTelemetryContext(): TelemetryCarrier {
  const injected: TelemetryCarrier = {}
  propagation.inject(context.active(), injected)

  // Redis stream messages are persisted. Carry only W3C trace context and never
  // persist baggage, which may contain arbitrary application metadata.
  const carrier: TelemetryCarrier = {}
  for (const key of ['traceparent', 'tracestate']) {
    if (injected[key]) carrier[key] = injected[key]
  }
  return carrier
}

export function withExtractedTelemetryContext<T>(carrier: TelemetryCarrier | undefined, callback: () => T): T {
  if (!carrier) return callback()
  return context.with(propagation.extract(context.active(), carrier), callback)
}

export { SpanKind, SpanStatusCode }
