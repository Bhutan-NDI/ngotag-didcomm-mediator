import { createHash } from 'node:crypto'
import {
  type Attributes,
  context,
  type Link,
  metrics,
  propagation,
  type Span,
  type SpanContext,
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

type TelemetryHeaders = Record<string, string | string[] | undefined>

const queuedTelemetry = new Map<
  string,
  Array<{ messageCarriers: TelemetryCarrier[]; consumerCarrier: TelemetryCarrier }>
>()
const websocketTelemetry = new WeakMap<object, TelemetryCarrier>()

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

export function getServerAddress(endpoint: string | undefined): string | undefined {
  try {
    return endpoint ? new URL(endpoint).hostname : undefined
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

export async function instrumentOperation<T>(
  name: string,
  options: {
    span: SpanOptions
    callback: (span: Span) => Promise<T>
    record: (outcome: string, elapsed: number) => void
    successOutcome?: string
    errorOutcome?: string
    resultOutcome?: (result: T) => string
  }
): Promise<T> {
  const startedAt = process.hrtime.bigint()
  const successOutcome = options.successOutcome ?? 'ok'
  let outcome = options.errorOutcome ?? 'error'

  return withSpan(name, options.span, async (span) => {
    try {
      const result = await options.callback(span)
      outcome = options.resultOutcome?.(result) ?? successOutcome
      if (outcome !== successOutcome) span.setStatus({ code: SpanStatusCode.ERROR })
      return result
    } finally {
      try {
        options.record(outcome, elapsedSeconds(startedAt))
      } catch {
        // Telemetry must never change application behaviour.
      }
    }
  })
}

export function activeSpan(): Span | undefined {
  return trace.getActiveSpan()
}

export function injectTelemetryContext(): TelemetryCarrier {
  const injected: TelemetryCarrier = {}
  propagation.inject(context.active(), injected)

  // Redis and queue messages are persisted. Carry only W3C trace context and
  // never persist baggage, which may contain arbitrary application metadata.
  const carrier: TelemetryCarrier = {}
  for (const key of ['traceparent', 'tracestate']) {
    if (injected[key]) carrier[key] = injected[key]
  }
  return carrier
}

function telemetryCarrierFromHeaders(headers: TelemetryHeaders): TelemetryCarrier {
  const carrier: TelemetryCarrier = {}
  for (const key of ['traceparent', 'tracestate']) {
    const value = headers[key]
    if (typeof value === 'string') carrier[key] = value
  }
  return carrier
}

export function registerWebSocketTelemetryContext(socket: object, headers: TelemetryHeaders): void {
  const carrier = telemetryCarrierFromHeaders(headers)
  if (carrier.traceparent) websocketTelemetry.set(socket, carrier)
}

export function getWebSocketTelemetryContext(session: unknown): TelemetryCarrier | undefined {
  if (!session || typeof session !== 'object') return undefined
  const socket = (session as { socket?: unknown }).socket
  return socket && typeof socket === 'object' ? websocketTelemetry.get(socket) : undefined
}

export function rememberQueuedTelemetry(connectionId: string, carriers: Array<TelemetryCarrier | undefined>): void {
  const validCarriers = carriers.filter((carrier): carrier is TelemetryCarrier => carrier?.traceparent !== undefined)
  if (validCarriers.length === 0) return

  const pending = queuedTelemetry.get(connectionId) ?? []
  pending.push({ messageCarriers: validCarriers, consumerCarrier: injectTelemetryContext() })
  queuedTelemetry.set(connectionId, pending)
}

function getSpanContext(carrier: TelemetryCarrier): SpanContext | undefined {
  return trace.getSpanContext(propagation.extract(context.active(), carrier))
}

export function withQueuedTelemetryContext<T>(connectionId: string | undefined, callback: (links: Link[]) => T): T {
  if (!connectionId) return callback([])

  const pending = queuedTelemetry.get(connectionId)
  const telemetry = pending?.shift()
  if (!telemetry) return callback([])
  if (!pending || pending.length === 0) queuedTelemetry.delete(connectionId)

  const parentContext = propagation.extract(context.active(), telemetry.messageCarriers[0])
  const links = [...telemetry.messageCarriers.slice(1), telemetry.consumerCarrier]
    .map(getSpanContext)
    .filter((spanContext): spanContext is SpanContext => spanContext !== undefined)
    .map((spanContext) => ({ context: spanContext }))

  return context.with(parentContext, () => callback(links))
}

export function withExtractedTelemetryContext<T>(carrier: TelemetryCarrier | undefined, callback: () => T): T {
  if (!carrier) return callback()
  return context.with(propagation.extract(context.active(), carrier), callback)
}

export { SpanKind, SpanStatusCode }
