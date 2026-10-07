import {
  DidCommAttachment,
  DidCommBatchMessage,
  DidCommBatchMessageMessage,
  DidCommMessageDeliveryV2Message,
  type DidCommMessageSender,
  DidCommTrustPingResponseMessage,
} from '@credo-ts/didcomm'
import { context, propagation, trace } from '@opentelemetry/api'
import { node } from '@opentelemetry/sdk-node'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { queuedTelemetrySize, rememberQueuedTelemetry } from '../telemetry/api.js'
import { instrumentQueuedDelivery } from './instrumentQueuedDelivery.js'

const provider = new node.NodeTracerProvider()
const traceparent = (traceId: string) => ({ traceparent: `00-${traceId}-00f067aa0ba902b7-01` })

beforeAll(() => {
  provider.register()
})

afterAll(async () => {
  await provider.shutdown()
  propagation.disable()
  context.disable()
  trace.disable()
})

function createSender() {
  const sentTraceIds: Array<string | undefined> = []
  const sender = {
    sendMessage: async () => {
      sentTraceIds.push(trace.getActiveSpan()?.spanContext().traceId)
    },
  } as unknown as DidCommMessageSender
  instrumentQueuedDelivery(sender)
  return { sender, sentTraceIds }
}

describe('instrumentQueuedDelivery', () => {
  test('parents pickup v2 Delivery and v1 Batch sends on the messages they carry', async () => {
    const { sender, sentTraceIds } = createSender()
    rememberQueuedTelemetry([
      { id: 'delivery-message', telemetry: traceparent('4bf92f3577b34da6a3ce929d0e0e4736') },
      { id: 'batch-message', telemetry: traceparent('0af7651916cd43dd8448eb211c80319c') },
    ])

    const delivery = new DidCommMessageDeliveryV2Message({
      attachments: [new DidCommAttachment({ id: 'delivery-message', data: { json: {} } })],
    })
    const batch = new DidCommBatchMessage({
      messages: [new DidCommBatchMessageMessage({ id: 'batch-message', message: {} as never })],
    })
    const unrelated = new DidCommTrustPingResponseMessage({ threadId: 'ping' })

    await sender.sendMessage({ message: unrelated } as never)
    await sender.sendMessage({ message: batch } as never)
    await sender.sendMessage({ message: delivery } as never)

    expect(sentTraceIds).toEqual([undefined, '0af7651916cd43dd8448eb211c80319c', '4bf92f3577b34da6a3ce929d0e0e4736'])
    expect(queuedTelemetrySize()).toBe(0)
  })
})
