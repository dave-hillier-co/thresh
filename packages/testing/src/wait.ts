export interface WaitForOptions {
  timeoutMs?: number;
  intervalMs?: number;
}

/**
 * Poll `condition` until it is truthy or the timeout elapses. Translates the
 * Orleans test suite's `WaitForXxx`/liveness-stabilize idioms without literal
 * sleeps. A condition that throws counts as not-yet-satisfied so callers can
 * poll operations that fail until the cluster converges.
 */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: WaitForOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 5_000;
  const intervalMs = options.intervalMs ?? 10;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  for (;;) {
    try {
      if (await condition()) return;
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    if (Date.now() >= deadline) {
      const cause = lastError !== undefined ? ` (last error: ${String(lastError)})` : "";
      throw new Error(`waitFor: condition not met within ${timeoutMs}ms${cause}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
