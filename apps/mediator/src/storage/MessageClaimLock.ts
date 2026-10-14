import { randomUUID } from 'node:crypto'
import type Redis from 'ioredis'

/** Arbitrates message claims across mediator instances that share one storage. */
export interface MessageClaimLock {
  /** Atomically claims each id until claimedUntil (epoch ms). Resolves, per id, whether this caller won it. */
  tryClaim(messageIds: string[], claimedUntil: number): Promise<boolean[]>
  /** Releases claims this caller holds, so a failed take does not hide them until they expire. */
  release(messageIds: string[]): Promise<void>
}

// Deletes the key only while it still holds this owner's token, so a release never drops another instance's claim.
const RELEASE_IF_OWNER = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0"

export class RedisMessageClaimLock implements MessageClaimLock {
  private readonly owner = randomUUID()

  public constructor(
    private readonly redis: Redis.default,
    private readonly keyPrefix = 'didcomm:pickup-claim:'
  ) {}

  public async tryClaim(messageIds: string[], claimedUntil: number): Promise<boolean[]> {
    if (messageIds.length === 0) return []

    const pipeline = this.redis.pipeline()
    // PXAT expires the key at the same instant as the record's claimedUntil.
    for (const messageId of messageIds) pipeline.set(this.key(messageId), this.owner, 'PXAT', claimedUntil, 'NX')
    const replies = (await pipeline.exec()) ?? []

    return replies.map(([error, reply]) => {
      if (error) throw error
      return reply === 'OK'
    })
  }

  public async release(messageIds: string[]): Promise<void> {
    if (messageIds.length === 0) return

    const pipeline = this.redis.pipeline()
    for (const messageId of messageIds) pipeline.eval(RELEASE_IF_OWNER, 1, this.key(messageId), this.owner)
    const replies = (await pipeline.exec()) ?? []

    for (const [error] of replies) if (error) throw error
  }

  private key(messageId: string) {
    return `${this.keyPrefix}${messageId}`
  }
}
