import type { Agent, AgentContext } from '@credo-ts/core'
import type {
  AddMessageOptions,
  GetAvailableMessageCountOptions,
  QueuedDidCommMessage,
  RemoveMessagesOptions,
  TakeFromQueueOptions,
} from '@credo-ts/didcomm'

import type { ExtendedQueueTransportRepository } from '../config/messagePickupLoader.js'
import {
  getJweFingerprint,
  hashIdentifier,
  injectTelemetryContext,
  instrumentOperation,
  queueBatchSize,
  queueMessageAge,
  queueOperationCounter,
  queueOperationDuration,
  rememberQueuedTelemetry,
  SpanKind,
  type TelemetryCarrier,
} from '../telemetry/api.js'

type QueueOperation = 'enqueue' | 'receive' | 'settle' | 'count'

export class InstrumentedQueueTransportRepository implements ExtendedQueueTransportRepository {
  public constructor(
    private readonly inner: ExtendedQueueTransportRepository,
    private readonly backend: 'credo' | 'postgres' | 'dynamodb'
  ) {}

  public initialize(agent: Agent): Promise<void> {
    return this.inner.initialize?.(agent) ?? Promise.resolve()
  }

  public getPoolStats() {
    return this.inner.getPoolStats?.() ?? null
  }

  public shutdown(agentContext: AgentContext): Promise<void> {
    return this.inner.shutdown?.(agentContext) ?? Promise.resolve()
  }

  private async instrument<T>(
    operation: QueueOperation,
    connectionId: string,
    kind: SpanKind,
    callback: () => T | Promise<T>,
    attributes: Record<string, string | number | boolean | undefined> = {}
  ): Promise<T> {
    return instrumentOperation(`${operation} didcomm.pickup.queue`, {
      span: {
        kind,
        attributes: {
          'messaging.system': 'didcomm',
          'messaging.destination.name': 'pickup.queue',
          'messaging.operation.name': operation,
          'messaging.operation.type': operation === 'enqueue' ? 'send' : operation === 'receive' ? 'receive' : 'settle',
          'didcomm.queue.backend': this.backend,
          'didcomm.connection.id_hash': hashIdentifier(connectionId),
          ...attributes,
        },
      },
      callback: async () => callback(),
      record: (outcome, elapsed) => {
        const metricAttributes = { backend: this.backend, operation, outcome }
        queueOperationCounter.add(1, metricAttributes)
        queueOperationDuration.record(elapsed, metricAttributes)
      },
    })
  }

  public getAvailableMessageCount(
    agentContext: AgentContext,
    options: GetAvailableMessageCountOptions
  ): Promise<number> {
    return this.instrument('count', options.connectionId, SpanKind.INTERNAL, () =>
      this.inner.getAvailableMessageCount(agentContext, options)
    )
  }

  public removeMessages(agentContext: AgentContext, options: RemoveMessagesOptions): Promise<void> {
    return this.instrument('settle', options.connectionId, SpanKind.CLIENT, () =>
      this.inner.removeMessages(agentContext, options)
    )
  }

  public takeFromQueue(agentContext: AgentContext, options: TakeFromQueueOptions): Promise<QueuedDidCommMessage[]> {
    return this.instrument(
      'receive',
      options.connectionId,
      SpanKind.CONSUMER,
      async () => {
        const messages = await this.inner.takeFromQueue(agentContext, options)
        queueBatchSize.record(messages.length, { backend: this.backend })
        const now = Date.now()
        for (const message of messages) {
          if (message.receivedAt) {
            queueMessageAge.record(Math.max(0, now - message.receivedAt.getTime()) / 1000, { backend: this.backend })
          }
        }
        rememberQueuedTelemetry(
          options.connectionId,
          messages.map((message) => (message as QueuedDidCommMessage & { telemetry?: TelemetryCarrier }).telemetry)
        )
        return messages
      },
      { 'messaging.batch.message_count': options.limit }
    )
  }

  public addMessage(agentContext: AgentContext, options: AddMessageOptions): Promise<string> {
    return this.instrument(
      'enqueue',
      options.connectionId,
      SpanKind.PRODUCER,
      () =>
        this.inner.addMessage(agentContext, {
          ...options,
          telemetry: injectTelemetryContext(),
        } as AddMessageOptions),
      { 'didcomm.message.fingerprint': getJweFingerprint(options.payload) }
    )
  }
}
