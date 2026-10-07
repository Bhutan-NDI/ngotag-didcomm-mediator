import { createHash } from 'node:crypto'
import {
  type Attributes,
  context,
  createContextKey,
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

interface QueuedTelemetry {
  messageCarrier: TelemetryCarrier
  consumerCarrier: TelemetryCarrier
  expiresAt: number
}

// Keyed by queued message id, so only the delivery that carries that message
// consumes its context. Bounded because a taken batch may never be delivered
// (e.g. the socket drops before the Delivery is sent).
export const QUEUED_TELEMETRY_TTL_MS = 5 * 60 * 1000
export const MAX_QUEUED_TELEMETRY_ENTRIES = 10_000
const queuedTelemetry = new Map<string, QueuedTelemetry>()
const queuedDeliveryLinksKey = createContextKey('didcomm.queued_delivery.links')
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

function pruneQueuedTelemetry(now: number): void {
  // Entries share one TTL and are re-inserted on refresh, so expiry follows insertion order.
  for (const [messageId, entry] of queuedTelemetry) {
    if (entry.expiresAt > now && queuedTelemetry.size <= MAX_QUEUED_TELEMETRY_ENTRIES) break
    queuedTelemetry.delete(messageId)
  }
}

export function rememberQueuedTelemetry(
  messages: ReadonlyArray<{ id: string; telemetry?: TelemetryCarrier }>,
  now = Date.now()
): void {
  let consumerCarrier: TelemetryCarrier | undefined
  for (const message of messages) {
    if (!message.telemetry?.traceparent) continue
    consumerCarrier ??= injectTelemetryContext()
    queuedTelemetry.delete(message.id)
    queuedTelemetry.set(message.id, {
      messageCarrier: message.telemetry,
      consumerCarrier,
      expiresAt: now + QUEUED_TELEMETRY_TTL_MS,
    })
  }
  pruneQueuedTelemetry(now)
}

export function queuedTelemetrySize(): number {
  return queuedTelemetry.size
}

function getSpanContext(carrier: TelemetryCarrier): SpanContext | undefined {
  return trace.getSpanContext(propagation.extract(context.active(), carrier))
}

/**
 * Runs `callback` with the stored context of the given queued messages: the first
 * message's enqueue context becomes the parent, and the other messages and the
 * taking operation are exposed as links through {@link getQueuedDeliveryLinks}.
 */
export function withQueuedTelemetryContext<T>(messageIds: readonly string[], callback: () => T, now = Date.now()): T {
  const entries: QueuedTelemetry[] = []
  for (const messageId of messageIds) {
    const entry = queuedTelemetry.get(messageId)
    if (!entry) continue
    queuedTelemetry.delete(messageId)
    if (entry.expiresAt > now) entries.push(entry)
  }
  if (entries.length === 0) return callback()

  const consumerCarriers = new Set(entries.map((entry) => entry.consumerCarrier))
  const links = [...entries.slice(1).map((entry) => entry.messageCarrier), ...consumerCarriers]
    .map(getSpanContext)
    .filter((spanContext): spanContext is SpanContext => spanContext !== undefined)
    .map((spanContext) => ({ context: spanContext }))

  const parentContext = propagation.extract(context.active(), entries[0].messageCarrier)
  return context.with(parentContext.setValue(queuedDeliveryLinksKey, links), callback)
}

export function getQueuedDeliveryLinks(): Link[] {
  return (context.active().getValue(queuedDeliveryLinksKey) as Link[] | undefined) ?? []
}

export function withExtractedTelemetryContext<T>(carrier: TelemetryCarrier | undefined, callback: () => T): T {
  if (!carrier) return callback()
  return context.with(propagation.extract(context.active(), carrier), callback)
}

export { SpanKind, SpanStatusCode }
