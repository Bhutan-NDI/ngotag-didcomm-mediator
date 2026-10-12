---
"@credo-ts/didcomm-transport-queue-dynamodb": patch
---

fix: claim queued messages on take so overlapping pickup v2 delivery-requests do not return the same messages
