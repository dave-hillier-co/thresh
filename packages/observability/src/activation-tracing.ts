import { trace, type Attributes, type Span } from "@opentelemetry/api";

/**
 * Span operation names for the ACTIVATION-path taxonomy (Orleans'
 * `ActivityNames`/`ActivitySources.{Runtime,Lifecycle,Storage}`): placement,
 * activation, directory registration, and persistent-state reads. Distinct
 * from the grain-CALL spans in `tracing.ts` (`Microsoft.Orleans.Application`
 * analogue), which wrap method dispatch, not activation. Kept as plain
 * string literals — exact matches of upstream's `ActivityNames` constants —
 * so ported parity test ids can assert on them directly.
 */
export const ActivityNames = {
  PlaceGrain: "place grain",
  FilterPlacementCandidates: "filter placement candidates",
  ActivateGrain: "activate grain",
  OnActivate: "execute OnActivateAsync",
  RegisterDirectoryEntry: "register directory entry",
  StorageRead: "read storage",
} as const;

const runtimeTracer = trace.getTracer("Microsoft.Orleans.Runtime");
const lifecycleTracer = trace.getTracer("Microsoft.Orleans.Lifecycle");
const storageTracer = trace.getTracer("Microsoft.Orleans.Storage");

async function withSpan<T>(
  tracer: ReturnType<typeof trace.getTracer>,
  name: string,
  attributes: Attributes,
  fn: () => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(name, { attributes }, async (span: Span) => {
    try {
      return await fn();
    } catch (err) {
      span.recordException(err as Error);
      throw err;
    } finally {
      span.end();
    }
  });
}

/** Wraps the placement decision for a not-yet-located grain (Runtime source). */
export function withPlaceGrainSpan<T>(fn: () => Promise<T>): Promise<T> {
  return withSpan(runtimeTracer, ActivityNames.PlaceGrain, {}, fn);
}

/**
 * Wraps bringing up a fresh activation — from winning placement through
 * running its activation hooks (Lifecycle source). `grainType` is recorded
 * as `orleans.grain.type` (upstream: the grain's type tag on the
 * `ActivateGrain` activity).
 */
export function withActivateGrainSpan<T>(
  attrs: { grainType: string },
  fn: () => Promise<T>,
): Promise<T> {
  return withSpan(
    lifecycleTracer,
    ActivityNames.ActivateGrain,
    { "orleans.grain.type": attrs.grainType },
    fn,
  );
}

/** Wraps registering a fresh activation's address in the grain directory (Runtime source). */
export function withRegisterDirectoryEntrySpan<T>(
  attrs: { grainId: string },
  fn: () => Promise<T>,
): Promise<T> {
  return withSpan(
    runtimeTracer,
    ActivityNames.RegisterDirectoryEntry,
    { "orleans.grain.id": attrs.grainId },
    fn,
  );
}

/**
 * Wraps a persistent-state read from a grain storage provider during
 * activation (Storage source).
 */
export function withStorageReadSpan<T>(
  attrs: { provider: string; stateName: string; grainId: string },
  fn: () => Promise<T>,
): Promise<T> {
  return withSpan(
    storageTracer,
    ActivityNames.StorageRead,
    {
      "orleans.storage.provider": attrs.provider,
      "orleans.storage.state.name": attrs.stateName,
      "orleans.grain.id": attrs.grainId,
    },
    fn,
  );
}
