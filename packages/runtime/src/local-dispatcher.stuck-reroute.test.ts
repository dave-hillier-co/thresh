import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import type { InvocationRequest } from "@thresh/core/request";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { LocalDispatcher } from "@thresh/runtime/local-dispatcher";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

let wedge = deferred<string>();

@grain()
class RerouteGrain extends Grain {
  async block(): Promise<string> {
    return wedge.promise;
  }
  async ping(): Promise<string> {
    return "pong";
  }
}

const metadata = getGrainMetadata(RerouteGrain)!;

const req = (id: GrainId, method: string, reentrancyId: string): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method,
  args: [],
  options: {},
  reentrancyId,
});

describe("LocalDispatcher reroutes calls queued behind a stuck activation (Orleans RerouteAllQueuedMessages)", () => {
  it("delivers a call that was queued behind the wedged turn to a fresh activation instead of rejecting it", async () => {
    wedge = deferred<string>();
    const time = new FakeTimeProvider();
    const factory = new GrainFactory(() => metadata.grainType, time);
    const catalog = new Catalog({
      grainTypes: new Map<string, RegisteredGrain>([
        [metadata.grainType, { ctor: RerouteGrain, metadata }],
      ]),
      factory,
      time,
      defaultCollectionAgeSeconds: 900,
      activationOptions: { maxRequestProcessingTimeMs: 1000 },
    });
    const dispatcher = new LocalDispatcher(catalog);
    const id = new GrainId(metadata.grainType, "a");

    void dispatcher.invoke(req(id, "block", "r-1"));
    await flush();
    const first = catalog.get(id);
    const queued = dispatcher.invoke(req(id, "ping", "r-2"));
    await flush();

    time.advance(1000);

    await expect(queued).resolves.toBe("pong");
    expect(catalog.get(id)).not.toBe(first);
    wedge.resolve("done");
  });
});
