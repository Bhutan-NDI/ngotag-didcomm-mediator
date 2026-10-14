import type Redis from 'ioredis'

/** Arbitrates message claims across mediator instances that share one storage. */
export interface MessageClaimLock {
  /** Atomically claims each id for ttlMs. Resolves, per id, whether this caller won it. */
  tryClaim(messageIds: string[], ttlMs: number): Promise<boolean[]>
}

export class RedisMessageClaimLock implements MessageClaimLock {
  public constructor(
    private readonly redis: Redis.default,
    private readonly keyPrefix = 'didcomm:pickup-claim:'
  ) {}

  public async tryClaim(messageIds: string[], ttlMs: number): Promise<boolean[]> {
    if (messageIds.length === 0) return []

    const pipeline = this.redis.pipeline()
    for (const messageId of messageIds) pipeline.set(`${this.keyPrefix}${messageId}`, '1', 'PX', ttlMs, 'NX')
    const replies = (await pipeline.exec()) ?? []

    return replies.map(([error, reply]) => {
      if (error) throw error
      return reply === 'OK'
    })
  }
}
