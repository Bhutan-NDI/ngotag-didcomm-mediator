import { type Logger, verkeyToDidKey } from '@credo-ts/core'
import {
  DidCommForwardMessage,
  DidCommMessageForwardingStrategy,
  type DidCommMessageHandlerMiddleware,
} from '@credo-ts/didcomm'
import { QueuedMessageTooLargeError } from '@credo-ts/didcomm-transport-queue-dynamodb'
import type { ExtendedQueueTransportRepository } from '../config/messagePickupLoader.js'
import { forwardRefusedCounter } from '../telemetry/api.js'
import { getHttpAdmissionSession } from './AdmissionHttpInboundTransport.js'

/** What the sender of a refused forward is told. It names no size: the limit depends on what is stored beside the message. */
export const MESSAGE_TOO_LARGE_REASON = 'The message is too large for the mediator to store for its recipient'

/**
 * Decides, before a message is handled, whether the mediator takes it.
 *
 * A forward that the queue cannot store for its size is refused: over HTTP the sender is answered 413 instead
 * of the 200 it used to get for a message that was then dropped, and nothing is written or handled. Every
 * other message is acknowledged at once, exactly when Credo's own transport would have done it, so accepting
 * a message costs no round trip more than before.
 *
 * The check applies when the forwarding strategy stores every forward (queue only, or queue and live
 * delivery). With direct delivery a forward is only stored when its recipient has no open session, which is
 * not known here: the queue's own check then still refuses the write, with the same error in the log.
 *
 * It has to be the first middleware: the HTTP session waits for its answer for one turn of the event loop.
 */
export function createQueueAdmissionMiddleware({
  queueTransportRepository,
  messageForwardingStrategy,
  logger,
}: {
  queueTransportRepository: Pick<ExtendedQueueTransportRepository, 'assertMessageFits'>
  messageForwardingStrategy: DidCommMessageForwardingStrategy
  logger: Logger
}): DidCommMessageHandlerMiddleware {
  const storesEveryForward = messageForwardingStrategy !== DidCommMessageForwardingStrategy.DirectDelivery

  return async (messageContext, next) => {
    const session = getHttpAdmissionSession(messageContext.encryptedMessage)
    const message = messageContext.message

    if (storesEveryForward && message instanceof DidCommForwardMessage && typeof message.to === 'string') {
      let tooLarge: QueuedMessageTooLargeError | undefined
      try {
        // The same recipient DID and payload the forward handler hands to the queue.
        queueTransportRepository.assertMessageFits?.({
          recipientDids: [verkeyToDidKey(message.to)],
          payload: message.message,
        })
      } catch (error) {
        // Anything else (a malformed recipient key, for one) is the forward handler's to report, as before.
        if (error instanceof QueuedMessageTooLargeError) tooLarge = error
      }

      if (tooLarge) {
        const senderInformed = session?.refuse(413, MESSAGE_TOO_LARGE_REASON) ?? false
        forwardRefusedCounter.add(1, { reason: 'too_large', transport: session ? 'http' : 'other' })
        // Sizes only: never the message, which is as large as it is refused for.
        logger.warn('Refused a forward: the message is too large to be queued for its recipient', {
          itemBytes: tooLarge.itemBytes,
          maxItemBytes: tooLarge.maxItemBytes,
          transport: session ? 'http' : 'other',
          senderInformed,
        })
        // Not handled: nothing is stored, and the dispatcher reports the message processed.
        return
      }
    }

    session?.accept()
    await next()
  }
}
