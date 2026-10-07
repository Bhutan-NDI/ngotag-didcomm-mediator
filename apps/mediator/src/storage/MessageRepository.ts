import {
  type AgentContext,
  EventEmitter,
  InjectionSymbols,
  inject,
  injectable,
  Repository,
  type StorageService,
} from '@credo-ts/core'

import { MessageRecord } from './MessageRecord.js'

@injectable()
export class MessageRepository extends Repository<MessageRecord> {
  public constructor(
    @inject(InjectionSymbols.StorageService)
    storageService: StorageService<MessageRecord>,
    eventEmitter: EventEmitter
  ) {
    super(MessageRecord, storageService, eventEmitter)
  }

  public findByConnectionId(agentContext: AgentContext, connectionId: string) {
    return this.findByQuery(agentContext, { connectionId })
  }

  // Aggregate queue-depth stats for OpenTelemetry observable gauges. Only used
  // by the in-tree `credo` pickup backend (best-effort).
  //
  // Bounded to 500 records to avoid a full-table scan on a large backlog:
  // `getAll()` hydrates every row through the storage layer on the same path
  // whose latency the gauge is diagnosing. `total` reflects the sample size
  // (accurate for queues ≤ 500; capped at 500 beyond that).
  public async getQueueStats(agentContext: AgentContext): Promise<{
    total: number
    oldestAgeMs: number
  }> {
    const SAMPLE_LIMIT = 500
    const records = await this.findByQuery(agentContext, {}, { limit: SAMPLE_LIMIT })

    let oldest = Date.now()

    for (const record of records) {
      if (record.createdAt.getTime() < oldest) oldest = record.createdAt.getTime()
    }

    return {
      total: records.length,
      oldestAgeMs: records.length > 0 ? Date.now() - oldest : 0,
    }
  }
}
