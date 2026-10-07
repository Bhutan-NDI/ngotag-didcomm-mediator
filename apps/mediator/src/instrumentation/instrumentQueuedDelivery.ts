import {
  DidCommBatchMessage,
  type DidCommMessage,
  DidCommMessageDeliveryV2Message,
  type DidCommMessageSender,
} from '@credo-ts/didcomm'

import { withQueuedTelemetryContext } from '../telemetry/api.js'

function getQueuedMessageIds(message: DidCommMessage): string[] {
  if (message instanceof DidCommMessageDeliveryV2Message) {
    return message.appendedAttachments?.map((attachment) => attachment.id) ?? []
  }
  if (message instanceof DidCommBatchMessage) return message.messages.map((batchMessage) => batchMessage.id)
  return []
}

/**
 * Restores the enqueue trace context for pickup Delivery (v2) and Batch (v1) messages.
 * Their attachment/message ids are the queued message ids, so the stored context is
 * consumed only by the send that actually carries those messages.
 *
 * The sender is wrapped on its resolved singleton instance because the module APIs
 * that use it are resolved when the agent is constructed.
 */
export function instrumentQueuedDelivery(messageSender: DidCommMessageSender): void {
  const originalSendMessage = messageSender.sendMessage.bind(messageSender)
  messageSender.sendMessage = (outboundMessageContext, options) =>
    withQueuedTelemetryContext(getQueuedMessageIds(outboundMessageContext.message), () =>
      originalSendMessage(outboundMessageContext, options)
    )
}
