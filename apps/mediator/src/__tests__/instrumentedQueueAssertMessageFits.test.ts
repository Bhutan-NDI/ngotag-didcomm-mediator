import { describe, expect, test, vi } from 'vitest'

describe('InstrumentedQueueTransportRepository.assertMessageFits', () => {
  test('asks the backend about the message with a telemetry carrier no smaller than the one that is stored', async () => {
    process.env = { ...process.env, LOG_LEVEL: 'off', ASKAR__STORE_ID: 'test', ASKAR__STORE_KEY: 'test' }
    const { InstrumentedQueueTransportRepository } = await import(
      '../instrumentation/InstrumentedQueueTransportRepository.js'
    )
    const assertMessageFits = vi.fn()
    const repository = new InstrumentedQueueTransportRepository({ assertMessageFits } as never, 'dynamodb')
    const payload = { protected: 'p', iv: 'i', ciphertext: 'c', tag: 't' }

    repository.assertMessageFits({ recipientDids: ['did:key:abc'], payload })

    expect(assertMessageFits).toHaveBeenCalledTimes(1)
    const options = assertMessageFits.mock.calls[0][0]
    expect(options.recipientDids).toEqual(['did:key:abc'])
    expect(options.payload).toBe(payload)
    // A W3C traceparent is always 55 characters: the carrier is sized as if tracing were on.
    expect(options.telemetry.traceparent).toHaveLength(55)
  })

  test('a backend with no size limit is not asked anything, and its error is passed on', async () => {
    const { InstrumentedQueueTransportRepository } = await import(
      '../instrumentation/InstrumentedQueueTransportRepository.js'
    )
    const payload = { protected: 'p', iv: 'i', ciphertext: 'c', tag: 't' }
    expect(() =>
      new InstrumentedQueueTransportRepository({} as never, 'postgres').assertMessageFits({
        recipientDids: [],
        payload,
      })
    ).not.toThrow()

    const refusal = new Error('too large')
    const limited = new InstrumentedQueueTransportRepository(
      {
        assertMessageFits: () => {
          throw refusal
        },
      } as never,
      'dynamodb'
    )
    expect(() => limited.assertMessageFits({ recipientDids: [], payload })).toThrow(refusal)
  })
})
