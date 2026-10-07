import { context, propagation, SpanStatusCode, trace } from '@opentelemetry/api'
import { node } from '@opentelemetry/sdk-node'
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import {
  getJweFingerprint,
  getProtocolAttributes,
  getQueuedDeliveryLinks,
  getServerAddress,
  getWebSocketTelemetryContext,
  hashIdentifier,
  instrumentOperation,
  MAX_QUEUED_TELEMETRY_ENTRIES,
  QUEUED_TELEMETRY_TTL_MS,
  queuedTelemetrySize,
  registerWebSocketTelemetryContext,
  rememberQueuedTelemetry,
  SpanKind,
  withQueuedTelemetryContext,
  withSpan,
} from './api.js'

const exporter = new InMemorySpanExporter()
const provider = new node.NodeTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
})

beforeAll(() => {
  // Registers the AsyncLocalStorage context manager and W3C propagator, as the SDK does.
  provider.register()
})

afterAll(async () => {
  await provider.shutdown()
  propagation.disable()
  context.disable()
  trace.disable()
})

describe('telemetry API', () => {
  test('hashes identifiers and JWE fingerprints without retaining source values', () => {
    const identifier = 'sensitive-connection-id'
    const fingerprint = getJweFingerprint({ iv: identifier, ciphertext: 'must-not-be-read' })

    expect(hashIdentifier(identifier)).toHaveLength(16)
    expect(hashIdentifier(identifier)).toBe(hashIdentifier(identifier))
    expect(fingerprint).toBe(hashIdentifier(identifier))
    expect(fingerprint).not.toContain(identifier)
    expect(getJweFingerprint('{invalid json')).toBeUndefined()
  })

  test('extracts bounded protocol dimensions from a DIDComm message type', () => {
    expect(getProtocolAttributes('https://didcomm.org/messagepickup/3.0/delivery')).toEqual({
      'didcomm.message.type': 'https://didcomm.org/messagepickup/3.0/delivery',
      'didcomm.protocol.name': 'messagepickup',
      'didcomm.protocol.version': '3.0',
    })
    expect(getServerAddress('wss://mediator.example/path')).toBe('mediator.example')
    expect(getServerAddress('not a URL')).toBeUndefined()
  })

  test('records operation outcomes consistently', async () => {
    const outcomes: string[] = []

    await instrumentOperation('successful-instrumented-operation', {
      span: { kind: SpanKind.INTERNAL },
      callback: async () => true,
      resultOutcome: (result) => (result ? 'ok' : 'error'),
      record: (outcome) => outcomes.push(outcome),
    })
    await expect(
      instrumentOperation('failed-instrumented-operation', {
        span: { kind: SpanKind.INTERNAL },
        callback: async () => {
          throw new Error('expected failure')
        },
        errorOutcome: 'undeliverable',
        record: (outcome) => outcomes.push(outcome),
      })
    ).rejects.toThrow('expected failure')

    expect(outcomes).toEqual(['ok', 'undeliverable'])
  })

  test('restores queued and websocket W3C metadata without modifying a DIDComm message', async () => {
    exporter.reset()
    const enqueueCarrier = {
      traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    }
    const secondCarrier = {
      traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
    }

    await withSpan('take-from-queue', { kind: SpanKind.CONSUMER }, async () => {
      rememberQueuedTelemetry([
        { id: 'queued-1', telemetry: enqueueCarrier },
        { id: 'queued-2', telemetry: secondCarrier },
        { id: 'untraced' },
      ])
    })
    await withQueuedTelemetryContext(['queued-1', 'queued-2'], () =>
      withSpan('queued-delivery', { kind: SpanKind.PRODUCER, links: getQueuedDeliveryLinks() }, async () => {})
    )

    await provider.forceFlush()
    const delivery = exporter.getFinishedSpans().find((span) => span.name === 'queued-delivery')
    const take = exporter.getFinishedSpans().find((span) => span.name === 'take-from-queue')
    expect(delivery?.spanContext().traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736')
    expect(delivery?.links.map((link) => link.context.spanId)).toEqual(['b7ad6b7169203331', take?.spanContext().spanId])
    expect(queuedTelemetrySize()).toBe(0)

    const socket = {}
    registerWebSocketTelemetryContext(socket, enqueueCarrier)
    expect(getWebSocketTelemetryContext({ socket })).toEqual(enqueueCarrier)
  })

  test('only the delivery carrying a queued message consumes its context', () => {
    const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
    rememberQueuedTelemetry([{ id: 'queued-for-pickup', telemetry: { traceparent } }])

    // An unrelated send on the same connection (e.g. a trust ping response) carries no queued ids.
    const unrelated = withQueuedTelemetryContext([], () => trace.getActiveSpan()?.spanContext().traceId)
    expect(unrelated).toBeUndefined()
    expect(queuedTelemetrySize()).toBe(1)

    const delivered = withQueuedTelemetryContext(
      ['queued-for-pickup'],
      () => trace.getActiveSpan()?.spanContext().traceId
    )
    expect(delivered).toBe('4bf92f3577b34da6a3ce929d0e0e4736')
    expect(withQueuedTelemetryContext(['queued-for-pickup'], () => getQueuedDeliveryLinks())).toEqual([])
    expect(queuedTelemetrySize()).toBe(0)
  })

  test('bounds queued telemetry by TTL and size', () => {
    const telemetry = { traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' }

    rememberQueuedTelemetry([{ id: 'never-delivered', telemetry }], 0)
    const expired = withQueuedTelemetryContext(
      ['never-delivered'],
      () => trace.getActiveSpan()?.spanContext().traceId,
      QUEUED_TELEMETRY_TTL_MS
    )
    expect(expired).toBeUndefined()
    expect(queuedTelemetrySize()).toBe(0)

    rememberQueuedTelemetry([{ id: 'stale', telemetry }], 0)
    rememberQueuedTelemetry([{ id: 'fresh', telemetry }], QUEUED_TELEMETRY_TTL_MS)
    expect(queuedTelemetrySize()).toBe(1)
    withQueuedTelemetryContext(['fresh'], () => undefined, QUEUED_TELEMETRY_TTL_MS)

    rememberQueuedTelemetry(
      Array.from({ length: MAX_QUEUED_TELEMETRY_ENTRIES + 5 }, (_, index) => ({ id: `queued-${index}`, telemetry }))
    )
    expect(queuedTelemetrySize()).toBe(MAX_QUEUED_TELEMETRY_ENTRIES)
    expect(withQueuedTelemetryContext(['queued-0'], () => trace.getActiveSpan())).toBeUndefined()
    withQueuedTelemetryContext(
      Array.from({ length: MAX_QUEUED_TELEMETRY_ENTRIES + 5 }, (_, index) => `queued-${index}`),
      () => undefined
    )
    expect(queuedTelemetrySize()).toBe(0)
  })

  test('ends successful spans and records failures', async () => {
    exporter.reset()

    await withSpan(
      'successful-operation',
      { kind: SpanKind.INTERNAL, attributes: { 'test.attribute': 'present' } },
      async () => 'ok'
    )
    await expect(
      withSpan('failed-operation', { kind: SpanKind.INTERNAL }, async () => {
        throw new Error('expected failure')
      })
    ).rejects.toThrow('expected failure')

    await provider.forceFlush()
    const spans = exporter.getFinishedSpans()
    const successful = spans.find((span) => span.name === 'successful-operation')
    const failed = spans.find((span) => span.name === 'failed-operation')

    expect(successful?.attributes['test.attribute']).toBe('present')
    expect(successful?.status.code).toBe(SpanStatusCode.UNSET)
    expect(failed?.status.code).toBe(SpanStatusCode.ERROR)
    expect(failed?.events.some((event) => event.name === 'exception')).toBe(true)
  })
})
