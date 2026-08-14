import { meter, websocketSessionEvents, websocketSessions } from '../telemetry/api.js'

type QueueStatsAccessor = () => Promise<{
  total: number
  oldestAgeMs: number
} | null>

type DbPoolStatsAccessor = () => { total: number; idle: number; waiting: number } | null

const queueDepth = meter.createObservableGauge('didcomm.queue.depth', {
  description: 'Current aggregate pickup queue depth where the backend supports an efficient observation',
  unit: '{message}',
})
const queueOldestAge = meter.createObservableGauge('didcomm.queue.oldest_message.age', {
  description: 'Age of the oldest queued message where the backend supports an efficient observation',
  unit: 's',
})
const dbPoolConnections = meter.createObservableGauge('db.client.connection.count', {
  description: 'Number of PostgreSQL client connections by state',
  unit: '{connection}',
})
const dbPoolPendingRequests = meter.createObservableGauge('db.client.connection.pending_requests', {
  description: 'Number of requests waiting for a PostgreSQL pool connection',
  unit: '{request}',
})

let queueAccessor: QueueStatsAccessor | undefined
const dbPoolAccessors = new Map<string, DbPoolStatsAccessor>()
let cachedQueueSnapshot: Awaited<ReturnType<QueueStatsAccessor>> | undefined
let queueSnapshotPromise: ReturnType<QueueStatsAccessor> | undefined
let queueSnapshotExpiresAt = 0

async function getQueueSnapshot() {
  const now = Date.now()
  if (cachedQueueSnapshot !== undefined && now < queueSnapshotExpiresAt) return cachedQueueSnapshot
  if (!queueAccessor) return null

  queueSnapshotPromise ??= queueAccessor().finally(() => {
    queueSnapshotPromise = undefined
  })
  cachedQueueSnapshot = await queueSnapshotPromise
  // Observable callbacks for different instruments run close together. A short
  // cache keeps them consistent and avoids querying the queue twice per export.
  queueSnapshotExpiresAt = now + 5_000
  return cachedQueueSnapshot
}

queueDepth.addCallback(async (result) => {
  const snapshot = await getQueueSnapshot()
  if (snapshot) result.observe(snapshot.total, { backend: 'credo' })
})

queueOldestAge.addCallback(async (result) => {
  const snapshot = await getQueueSnapshot()
  if (snapshot) result.observe(snapshot.oldestAgeMs / 1000, { backend: 'credo' })
})

dbPoolConnections.addCallback((result) => {
  for (const [pool, accessor] of dbPoolAccessors) {
    const snapshot = accessor()
    if (!snapshot) continue
    result.observe(snapshot.total - snapshot.idle, { pool, state: 'used' })
    result.observe(snapshot.idle, { pool, state: 'idle' })
  }
})

dbPoolPendingRequests.addCallback((result) => {
  for (const [pool, accessor] of dbPoolAccessors) {
    const snapshot = accessor()
    if (snapshot) result.observe(snapshot.waiting, { pool })
  }
})

export function registerQueueAccessor(accessor: QueueStatsAccessor): void {
  queueAccessor = accessor
}

export function registerDbPoolAccessor(pool: string, accessor: DbPoolStatsAccessor): void {
  dbPoolAccessors.set(pool, accessor)
}

export function wsSessionOpened(): void {
  websocketSessions.add(1)
  websocketSessionEvents.add(1, { event: 'opened' })
}

export function wsSessionClosed(): void {
  websocketSessions.add(-1)
  websocketSessionEvents.add(1, { event: 'closed' })
}
