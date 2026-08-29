import { describe, expect, it, vi } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import type { Catalog } from "@thresh/runtime/catalog";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import type { LocationCache } from "@thresh/directory/location-cache";
import {
  DistributedDispatcher,
  type DistributedDispatcherDeps,
} from "@thresh/runtime/distributed-dispatcher";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import { RandomPlacement } from "@thresh/runtime/placement/random-placement";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const local = silo(0);
const candidates = [silo(0), silo(1), silo(2)];

const request = (): InvocationRequest => ({
  target: new GrainId("Counter", "k"),
  interfaceId: 1,
  method: "ping",
  args: [],
  options: {},
  reentrancyId: "r",
});

/** Deps wired so a placement always misses the cache/directory and forwards remotely. */
function deps(overrides: Partial<DistributedDispatcherDeps> = {}): {
  deps: DistributedDispatcherDeps;
  send: ReturnType<typeof vi.fn>;
} {
  const send = vi.fn().mockResolvedValue("ok");
  return {
    send,
    deps: {
      local,
      directory: { lookup: async () => undefined } as unknown as GrainDirectory,
      cache: {
        get: () => undefined,
        put: () => undefined,
        invalidate: () => undefined,
      } as unknown as LocationCache,
      catalog: { isStatelessWorkerType: () => false } as unknown as Catalog,
      remote: { send },
      activeSilos: () => candidates,
      placementFor: () => new RandomPlacement(),
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0.9 }),
      ...overrides,
    },
  };
}

/** A filter keeping only the named silo. */
const only = (ringKey: string): PlacementFilter => ({
  filter: (_t, cs) => cs.filter((s) => s.ringKey === ringKey),
});

describe("DistributedDispatcher placement filters", () => {
  it("applies filters to prune candidates before the strategy chooses", async () => {
    const { deps: d, send } = deps({ filtersFor: () => [only("silo-1")] });
    // Random would point past silo-1, but filtering leaves only silo-1.
    await new DistributedDispatcher(d).invoke(request());
    expect(send).toHaveBeenCalledTimes(1);
    expect((send.mock.calls[0]![0] as SiloAddress).ringKey).toBe("silo-1");
  });

  it("throws a noCandidates RejectionError when filtering prunes every silo", async () => {
    const { deps: d } = deps({ filtersFor: () => [only("silo-9")] });
    const err = await new DistributedDispatcher(d).invoke(request()).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RejectionError);
    expect((err as RejectionError).kind).toBe("noCandidates");
  });

  it("places via the strategy when no filters are declared", async () => {
    const { deps: d, send } = deps(); // default random 0.9 -> silo-2 (remote)
    await new DistributedDispatcher(d).invoke(request());
    expect(send).toHaveBeenCalledTimes(1);
    expect((send.mock.calls[0]![0] as SiloAddress).ringKey).toBe("silo-2");
  });
});

// GAP-CANCELLATION-DISPATCHER (issue #18): `opts.deadlineMs` must be resolved
// into a wire-safe `req.deadline` BEFORE a remote forward, since an
// `AbortSignal` (`opts.signal`) cannot cross the wire but a plain timestamp
// can — this is what lets a downstream silo derive its own local signal.
describe("DistributedDispatcher ambient deadline (issue #18)", () => {
  it("embeds opts.deadlineMs into req.deadline before forwarding remotely", async () => {
    const { deps: d, send } = deps(); // default random 0.9 -> silo-2 (remote)
    const before = Date.now();

    await new DistributedDispatcher(d).invoke(request(), { deadlineMs: 10_000 });

    expect(send).toHaveBeenCalledTimes(1);
    const forwarded = send.mock.calls[0]![1] as InvocationRequest;
    expect(forwarded.deadline).toBeGreaterThanOrEqual(before + 10_000);
  });

  it("does not override a req.deadline the caller already set", async () => {
    const { deps: d, send } = deps();
    const req = { ...request(), deadline: 123 };

    await new DistributedDispatcher(d).invoke(req, { deadlineMs: 10_000 });

    const forwarded = send.mock.calls[0]![1] as InvocationRequest;
    expect(forwarded.deadline).toBe(123);
  });
});

// A key-aware placement strategy (Orleans `IPlacementDirector` reads
// `target.GrainIdentity.Key`) cannot work from the grain TYPE alone: Spiceport's
// GraphLocalityPlacementDirector hashes the grain KEY to co-locate compute with its data
// shard. The dispatcher has the whole `GrainId` in hand as `req.target`, so it passes it
// through the placement context.
describe("DistributedDispatcher placement context carries the grain id", () => {
  it("gives the strategy the target GrainId, not only the grain type", async () => {
    let seen: GrainId | undefined;
    const { deps: d } = deps({
      placementFor: () => ({
        choose: (_type, cs, ctx) => {
          seen = ctx.grainId;
          return cs[cs.length - 1]!; // a remote silo, so the call forwards rather than activating locally
        },
      }),
    });

    await new DistributedDispatcher(d).invoke(request());

    expect(seen?.type).toBe("Counter");
    expect(seen?.key).toBe("k");
  });

  it("gives placement filters the same grain id", async () => {
    let seen: GrainId | undefined;
    const { deps: d } = deps({
      filtersFor: () => [
        {
          filter: (_t, cs, ctx) => {
            seen = ctx.grainId;
            return cs;
          },
        },
      ],
    });

    await new DistributedDispatcher(d).invoke(request());

    expect(seen?.key).toBe("k");
  });

  it("does not let a placementContext provider override the dispatcher's grain id", async () => {
    let seen: GrainId | undefined;
    const { deps: d } = deps({
      placementContext: () =>
        ({ random: () => 0.9, grainId: new GrainId("Other", "wrong") }) as never,
      placementFor: () => ({
        choose: (_type, cs, ctx) => {
          seen = ctx.grainId;
          return cs[cs.length - 1]!; // a remote silo, so the call forwards rather than activating locally
        },
      }),
    });

    await new DistributedDispatcher(d).invoke(request());

    expect(seen?.type).toBe("Counter");
  });
});
