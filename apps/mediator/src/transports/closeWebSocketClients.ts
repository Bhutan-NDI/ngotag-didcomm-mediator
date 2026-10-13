import type { WebSocketServer } from 'ws'

const WS_CLOSE_GRACE_MS = 2000

// With `noServer: true`, ws only completes `close()` once every client has gone, so
// Credo's inbound transport stop would wait on wallets holding live-mode sockets.
// Close them with 1001 (going away) and drop any that miss the grace period.
export async function closeWebSocketClients(socketServer: WebSocketServer, graceMs = WS_CLOSE_GRACE_MS): Promise<void> {
  const clients = [...socketServer.clients]
  if (clients.length === 0) return

  const closed = Promise.all(
    clients.map(
      (client) =>
        new Promise<void>((resolve) => {
          client.once('close', () => resolve())
          client.close(1001, 'Server shutting down')
        })
    )
  )
  let timer: NodeJS.Timeout | undefined
  const graceExpired = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, graceMs)
  })
  await Promise.race([closed, graceExpired])
  clearTimeout(timer)

  for (const client of socketServer.clients) client.terminate()
}
