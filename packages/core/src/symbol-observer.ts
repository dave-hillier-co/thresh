/**
 * Reads a symbol-keyed observer factory off a grain instance and, if present,
 * returns it bound to the instance; otherwise `undefined`. Shared by the
 * implicit stream and broadcast-channel observers, which differ only in the
 * symbol and handler type.
 */
export function createSymbolObserver<T>(
  instance: object,
  symbol: symbol,
): ((namespace: string, key: string) => T) | undefined {
  const fn = (instance as Record<symbol, unknown>)[symbol];
  return typeof fn === "function"
    ? (namespace, key) =>
        (fn as (namespace: string, key: string) => T).call(instance, namespace, key)
    : undefined;
}
