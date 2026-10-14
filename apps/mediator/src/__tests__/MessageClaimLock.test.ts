import type Redis from 'ioredis'
import { describe, expect, it } from 'vitest'
import { RedisMessageClaimLock } from '../storage/MessageClaimLock.js'

function createRedis(replies: Array<[Error | null, unknown]>) {
  const commands: unknown[][] = []
  const pipeline = {
    set: (...args: unknown[]) => {
      commands.push(args)
      return pipeline
    },
    exec: async () => replies,
  }
  return { redis: { pipeline: () => pipeline } as unknown as Redis.default, commands }
}

describe('RedisMessageClaimLock', () => {
  it('claims each id with SET NX and a millisecond expiry', async () => {
    const { redis, commands } = createRedis([
      [null, 'OK'],
      [null, null],
    ])

    const won = await new RedisMessageClaimLock(redis).tryClaim(['a', 'b'], 30_000)

    expect(won).toEqual([true, false])
    expect(commands).toEqual([
      ['didcomm:pickup-claim:a', '1', 'PX', 30_000, 'NX'],
      ['didcomm:pickup-claim:b', '1', 'PX', 30_000, 'NX'],
    ])
  })

  it('does not call redis for an empty batch', async () => {
    const { redis, commands } = createRedis([])

    expect(await new RedisMessageClaimLock(redis).tryClaim([], 30_000)).toEqual([])
    expect(commands).toEqual([])
  })

  it('surfaces a failed command instead of treating it as a lost claim', async () => {
    const { redis } = createRedis([[new Error('redis down'), null]])

    await expect(new RedisMessageClaimLock(redis).tryClaim(['a'], 30_000)).rejects.toThrow('redis down')
  })
})
