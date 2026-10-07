export type SettledWithin<Result> =
  | { status: 'completed'; value: Result }
  | { status: 'errored'; error: unknown }
  | { status: 'timed-out' }

/**
 * Observe an operation for a bounded period without cancelling it. This lets a
 * caller start a fallback while the original operation remains owned and
 * serialized elsewhere.
 */
export async function settleWithin<Result>(
  operation: Promise<Result>,
  timeoutMs: number
): Promise<SettledWithin<Result>> {
  let timeout: NodeJS.Timeout | undefined
  const timedOut = new Promise<SettledWithin<Result>>((resolve) => {
    timeout = setTimeout(() => resolve({ status: 'timed-out' }), timeoutMs)
  })

  try {
    return await Promise.race([
      operation.then(
        (value): SettledWithin<Result> => ({ status: 'completed', value }),
        (error): SettledWithin<Result> => ({ status: 'errored', error })
      ),
      timedOut,
    ])
  } finally {
    clearTimeout(timeout)
  }
}
