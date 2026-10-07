import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, test } from 'vitest'
import { WebSocket, WebSocketServer } from 'ws'

import { closeWebSocketClients } from './closeWebSocketClients.js'

let server: Server | undefined

afterEach(async () => {
  await new Promise((resolve) => server?.close(resolve) ?? resolve(undefined))
  server = undefined
})

async function startServer() {
  const socketServer = new WebSocketServer({ noServer: true })
  server = createServer()
  server.on('upgrade', (request, socket, head) => {
    socketServer.handleUpgrade(request, socket, head, (ws) => socketServer.emit('connection', ws, request))
  })
  await new Promise<void>((resolve) => server?.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  return { socketServer, url: `ws://127.0.0.1:${port}` }
}

async function connect(url: string, options?: { ignoreClose?: boolean }) {
  const client = new WebSocket(url)
  await new Promise((resolve) => client.once('open', resolve))
  // Simulate an unresponsive client: never answer the close frame.
  if (options?.ignoreClose) client.pause()
  return client
}

function closeServer(socketServer: WebSocketServer) {
  return new Promise<void>((resolve, reject) => socketServer.close((error) => (error ? reject(error) : resolve())))
}

describe('closeWebSocketClients', () => {
  test('lets a noServer WebSocketServer close while clients are connected', async () => {
    const { socketServer, url } = await startServer()
    const client = await connect(url)
    const closeCode = new Promise((resolve) => client.once('close', resolve))

    await closeWebSocketClients(socketServer)
    await closeServer(socketServer)

    expect(await closeCode).toBe(1001)
    expect(socketServer.clients.size).toBe(0)
  })

  test('terminates clients that do not complete the close handshake', async () => {
    const { socketServer, url } = await startServer()
    const client = await connect(url, { ignoreClose: true })

    const startedAt = Date.now()
    await closeWebSocketClients(socketServer, 100)
    await closeServer(socketServer)

    expect(Date.now() - startedAt).toBeLessThan(2000)
    expect(socketServer.clients.size).toBe(0)
    client.terminate()
  })
})
