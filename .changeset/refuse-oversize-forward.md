---
"@credo-ts/didcomm-transport-queue-dynamodb": patch
---

fix: refuse a queued message whose item would be above DynamoDB's 400 KB limit before anything is written, with a `QueuedMessageTooLargeError`, and expose the same check as `assertMessageFits` so a mediator can refuse an over-size forward before it acknowledges it
