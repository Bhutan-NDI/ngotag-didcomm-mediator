import { propagation, SpanStatusCode, trace } from '@opentelemetry/api'
import { core } from '@opentelemetry/sdk-node'
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import {
  getJweFingerprint,
  getProtocolAttributes,
  getServerAddress,
  getWebSocketTelemetryContext,
  hashIdentifier,
  instrumentOperation,
  registerWebSocketTelemetryContext,
  rememberQueuedTelemetry,
  SpanKind,
  withQueuedTelemetryContext,
  withSpan,
} from './api.js'

const exporter = new InMemorySpanExporter()
const provider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
})

beforeAll(() => {
  trace.setGlobalTracerProvider(provider)
  propagation.setGlobalPropagator(new core.W3CTraceContextPropagator())
})

afterAll(async () => {
  await provider.shutdown()
  propagation.disable()
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
    const enqueueCarrier = {
      traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    }
    const secondCarrier = {
      traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
    }

    rememberQueuedTelemetry('connection-id', [enqueueCarrier, secondCarrier])
    let queuedLinks = 0
    await withQueuedTelemetryContext('connection-id', (links) =>
      withSpan('queued-delivery', { kind: SpanKind.PRODUCER, links }, async () => {
        queuedLinks = links.length
      })
    )

    const socket = {}
    registerWebSocketTelemetryContext(socket, enqueueCarrier)
    expect(getWebSocketTelemetryContext({ socket })).toEqual(enqueueCarrier)
    expect(queuedLinks).toBe(1)
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
