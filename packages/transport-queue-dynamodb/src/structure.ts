import { AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType } from '@aws-sdk/client-dynamodb'
import { QueuedDidCommMessage as CredoQueuedMessage } from '@credo-ts/didcomm'

// CredoQueuedMessage made Required right now, due to credo having them as optional, but we need it for efficient sorting
export type QueuedMessage = Required<CredoQueuedMessage> & {
  connectionId: string
  recipientDids: Array<string>
}

export const attributeDefinitions: Array<AttributeDefinition> = [
  {
    AttributeName: 'connectionId',
    AttributeType: ScalarAttributeType.S,
  },
  {
    AttributeName: 'messageId',
    AttributeType: ScalarAttributeType.N,
  },
]

export const keySchema: Array<KeySchemaElement> = [
  {
    AttributeName: 'connectionId',
    KeyType: KeyType.HASH,
  },
  {
    AttributeName: 'messageId',
    KeyType: KeyType.RANGE,
  },
]

// A DynamoDB GSI cannot index every value in `recipientDids`, because it is a
// list. Keep the queue table unchanged and use a companion table instead. One
// small index item is written for every recipient and points back to the
// canonical queue item. This keeps the encrypted payload stored once.
export const recipientIndexAttributeDefinitions: Array<AttributeDefinition> = [
  {
    AttributeName: 'connectionId',
    AttributeType: ScalarAttributeType.S,
  },
  {
    AttributeName: 'recipientMessageId',
    AttributeType: ScalarAttributeType.S,
  },
]

export const recipientIndexKeySchema: Array<KeySchemaElement> = [
  {
    AttributeName: 'connectionId',
    KeyType: KeyType.HASH,
  },
  {
    AttributeName: 'recipientMessageId',
    KeyType: KeyType.RANGE,
  },
]
