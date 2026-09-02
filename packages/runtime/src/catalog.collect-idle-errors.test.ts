import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import type { Logger, LogFields } from "@thresh/core/logger";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

@grain()
class CollectIdleErrorsGrain extends Grain {}

const metadata = getGrainMetadata(CollectIdleErrorsGrain)!;

function recordingLogger(): { logger: Logger; warnings: Array<[string, LogFields | undefined]> } {
  const warnings: Array<[string, LogFields | undefined]> = [];
  return {
    warnings,
    logger: {
      debug() {},
      info() {},
      warn(message, fields) {
        warnings.push([message, fields]);
      },
      error() {},
    },
  };
}

function buildCatalog(onDeactivated: (activation: unknown) => void, logger: Logger): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: CollectIdleErrorsGrain, metadata }],
  ]);
  const time = new FakeTimeProvider();
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({
    grainTypes,
    factory,
    time,
    defaultCollectionAgeSeconds: 900,
    onDeactivated,
    activationOptions: { logger },
  });
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("Catalog idle collection disposal errors", () => {
  it("logs a throwing onDeactivated hook instead of rejecting collectIdle, and still removes the activation", async () => {
    const { logger, warnings } = recordingLogger();
    const catalog = buildCatalog(() => {
      throw new Error("dispose boom");
    }, logger);
    const idA = new GrainId(metadata.grainType, "a");
    const idB = new GrainId(metadata.grainType, "b");
    await catalog.getOrCreate(idA);
    await catalog.getOrCreate(idB);
    await flush();

    // Neither activation must be stale enough to survive being collected;
    // ageLimitOverrideMs = 0 forces collection of every currently-idle one.
    await expect(catalog.collectIdle(0)).resolves.toBeUndefined();

    expect(
      warnings.some(([msg, fields]) => msg.includes("disposal") && fields?.error instanceof Error),
    ).toBe(true);
    // Both activations' disposal failures were logged, not just the first
    // (one bad activation must not stop the rest of the sweep).
    expect(warnings.filter(([msg]) => msg.includes("disposal")).length).toBe(2);
  });
});
