export type { DynamoDbClientRepositoryOptions as DynamodbClientRepositoryOptions } from './client.js'
export type { QueuedMessageItemParts } from './itemSize.js'
export {
  CONNECTION_ID_ALLOWANCE_BYTES,
  DYNAMODB_MAX_ITEM_BYTES,
  QueuedMessageTooLargeError,
  queuedMessageItemBytes,
} from './itemSize.js'
export type { DynamoDbTransportQueueOptions } from './TransportQueueDynamoDb.js'
export { DidCommTransportQueueDynamoDb } from './TransportQueueDynamoDb.js'
