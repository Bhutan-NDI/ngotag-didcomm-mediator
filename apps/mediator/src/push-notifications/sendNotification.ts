import { AgentContext } from '@credo-ts/core'
import { DidCommPushNotificationsFcmRepository } from '@credo-ts/didcomm-push-notifications'
import { config } from '../config.js'
import {
  hashIdentifier,
  instrumentOperation,
  pushNotificationCounter,
  pushNotificationDuration,
  SpanKind,
} from '../telemetry/api.js'
import { sendFcmPushNotification } from './fcm/events/PushNotificationEvent.js'

async function instrumentPush<T>(
  channel: 'webhook' | 'fcm',
  connectionId: string,
  callback: () => Promise<T>,
  isSuccess: (result: T) => boolean
): Promise<T> {
  return instrumentOperation(`didcomm.push.${channel}`, {
    span: {
      kind: SpanKind.CLIENT,
      attributes: {
        'didcomm.push.channel': channel,
        'didcomm.connection.id_hash': hashIdentifier(connectionId),
      },
    },
    callback: async () => callback(),
    resultOutcome: (result) => (isSuccess(result) ? 'ok' : 'error'),
    record: (outcome, elapsed) => {
      const attributes = { channel, outcome }
      pushNotificationCounter.add(1, attributes)
      pushNotificationDuration.record(elapsed, attributes)
    },
  })
}

export async function sendNotification(agentContext: AgentContext, connectionId: string) {
  if (!config.pushNotifications) return

  // Get the device token for the connection
  const pushNotificationsFcmRepository = agentContext.resolve(DidCommPushNotificationsFcmRepository)
  const pushNotificationFcmRecord = await pushNotificationsFcmRepository.findSingleByQuery(agentContext, {
    connectionId,
  })

  // Check for webhook Url
  if (config.pushNotifications.webhookUrl) {
    // Emit a webhook notification, which can send a notification based on the
    // connectionId or optionally the device token.
    await instrumentPush(
      'webhook',
      connectionId,
      () =>
        sendWebhookNotification(
          agentContext,
          config.pushNotifications.webhookUrl as string,
          connectionId,
          pushNotificationFcmRecord?.deviceToken
        ),
      (sent) => sent
    )
  }

  if (config.pushNotifications.firebase) {
    if (!pushNotificationFcmRecord) {
      agentContext.config.logger.debug(
        `No device token found for connection ${connectionId} so skip sending pushing notification`
      )
      return
    }

    // Check for firebase configuration
    // Send a Firebase Cloud Message notification to the device found for a given connection
    await instrumentPush(
      'fcm',
      connectionId,
      () => sendFcmPushNotification(agentContext, pushNotificationFcmRecord),
      (response) => response !== undefined
    )
  }
}

async function sendWebhookNotification(
  agentContext: AgentContext,
  webhookUrl: string,
  connectionId: string,
  deviceToken?: string | null
) {
  try {
    // Prepare a message to be sent to the device
    agentContext.config.logger.info(`Sending notification to ${connectionId}`)
    const body = {
      connectionId,
      fcmToken: deviceToken,
    }
    const requestOptions = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    }

    const response = await fetch(webhookUrl, requestOptions)

    if (response.ok) {
      agentContext.config.logger.info('Notification sent successfully')
      return true
    } else {
      agentContext.config.logger.error('Error sending notification', {
        cause: response.statusText,
      })
      return false
    }
  } catch (error) {
    agentContext.config.logger.error('Error sending notification', {
      cause: error,
    })
    return false
  }
}
