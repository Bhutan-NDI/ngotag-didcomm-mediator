// Lease rules for pickup v2 claims, shared by every queue backend so they stay consistent.

/** How long a taken but unacknowledged message stays hidden from other takes. */
export const DEFAULT_MESSAGE_CLAIM_TTL_MS = 30_000

export function assertValidClaimTtl(claimTtlMs: number): void {
  if (!Number.isInteger(claimTtlMs) || claimTtlMs < 1) {
    throw new Error('claimTtlMs must be a positive integer number of milliseconds')
  }
}

export function isClaimed(claimedUntil: number | undefined, now: number): boolean {
  return claimedUntil !== undefined && claimedUntil > now
}
