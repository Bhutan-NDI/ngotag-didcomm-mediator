import { SpanStatusCode, trace } from '@opentelemetry/api'
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { getJweFingerprint, getProtocolAttributes, hashIdentifier, SpanKind, withSpan } from './api.js'

const exporter = new InMemorySpanExporter()
const provider = new BasicTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
})

beforeAll(() => {
  trace.setGlobalTracerProvider(provider)
})

afterAll(async () => {
  await provider.shutdown()
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
