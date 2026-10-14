import type Redis from 'ioredis'
import { describe, expect, it } from 'vitest'
import { RedisMessageClaimLock } from '../storage/MessageClaimLock.js'

function createRedis(replies: Array<[Error | null, unknown]>) {
  const commands: unknown[][] = []
  const pipeline = {
    set: (...args: unknown[]) => {
      commands.push(['set', ...args])
      return pipeline
    },
    eval: (...args: unknown[]) => {
      commands.push(['eval', ...args])
      return pipeline
    },
    exec: async () => replies,
  }
  return { redis: { pipeline: () => pipeline } as unknown as Redis.default, commands }
}

describe('RedisMessageClaimLock', () => {
  it('claims each id with SET NX, expiring at the claim deadline', async () => {
    const { redis, commands } = createRedis([
      [null, 'OK'],
      [null, null],
    ])

    const won = await new RedisMessageClaimLock(redis).tryClaim(['a', 'b'], 1_800_000_000_000)

    expect(won).toEqual([true, false])
    expect(commands).toEqual([
      ['set', 'didcomm:pickup-claim:a', expect.any(String), 'PXAT', 1_800_000_000_000, 'NX'],
      ['set', 'didcomm:pickup-claim:b', expect.any(String), 'PXAT', 1_800_000_000_000, 'NX'],
    ])
  })

  it('releases only keys that still hold its own token', async () => {
    const { redis, commands } = createRedis([
      [null, 'OK'],
      [null, 1],
    ])
    const lock = new RedisMessageClaimLock(redis)

    await lock.tryClaim(['a'], 1_800_000_000_000)
    await lock.release(['a'])

    const token = commands[0][2]
    expect(commands[1]).toEqual([
      'eval',
      expect.stringContaining("redis.call('get', KEYS[1]) == ARGV[1]"),
      1,
      'didcomm:pickup-claim:a',
      token,
    ])
  })

  it('does not call redis for an empty batch', async () => {
    const { redis, commands } = createRedis([])
    const lock = new RedisMessageClaimLock(redis)

    expect(await lock.tryClaim([], 1_800_000_000_000)).toEqual([])
    await lock.release([])
    expect(commands).toEqual([])
  })

  it('surfaces a failed command instead of treating it as a lost claim', async () => {
    const { redis } = createRedis([[new Error('redis down'), null]])

    await expect(new RedisMessageClaimLock(redis).tryClaim(['a'], 1_800_000_000_000)).rejects.toThrow('redis down')
  })
})
