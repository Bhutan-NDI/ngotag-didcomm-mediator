import { DrizzleStorageModule } from '@credo-ts/drizzle-storage'
import { didcommBundle } from '@credo-ts/drizzle-storage/didcomm'
import { drizzle as drizzleSqlite } from 'drizzle-orm/libsql'
import { drizzle as drizzlePostgres } from 'drizzle-orm/node-postgres'
import { Pool } from 'pg'
import { config, logger } from '../config.js'
import { mediatorBundle } from '../drizzle/bundle.js'
import { registerDbPoolAccessor } from '../instrumentation/metrics.js'

export function loadStorage(): { drizzle?: DrizzleStorageModule } {
  const { storage } = config

  if (storage.type === 'drizzle') {
    logger.info('Using drizzle storage')

    let database: ReturnType<typeof drizzlePostgres> | ReturnType<typeof drizzleSqlite>
    if (storage.dialect === 'postgres') {
      const pool = new Pool({ connectionString: storage.databaseUrl })
      registerDbPoolAccessor('drizzle', () => ({
        total: pool.totalCount,
        idle: pool.idleCount,
        waiting: pool.waitingCount,
      }))
      database = drizzlePostgres(pool)
    } else {
      database = drizzleSqlite(storage.databaseUrl)
    }

    return {
      drizzle: new DrizzleStorageModule({
        database,
        bundles: [didcommBundle, mediatorBundle],
      }),
    }
  }

  return {}
}
