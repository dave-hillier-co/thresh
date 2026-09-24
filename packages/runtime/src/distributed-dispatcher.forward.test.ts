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

const local = new SiloAddress("silo-0", "uid-0", "silo-0:1");
const winner = new SiloAddress("silo-1", "uid-1", "silo-1:1");
const target = new GrainId("Counter", "k");

const request = (overrides: Partial<InvocationRequest> = {}): InvocationRequest => ({
  target,
  interfaceId: 1,
  method: "ping",
  args: [],
  options: {},
  reentrancyId: "r",
  ...overrides,
});

/**
 * Deps for a grain with no live local activation whose directory CAS is
 * ALWAYS lost to `winner` -- the "silo receives a call for a grain it turns
 * out not to host" shape `claimLocalActivation`'s forward branch handles
 * (issue #110). `directory`/`remote` are the fakes here; everything else is
 * the real `DistributedDispatcher`.
 */
function forwardingDeps(): {
  deps: DistributedDispatcherDeps;
  send: ReturnType<typeof vi.fn>;
} {
  const send = vi.fn().mockResolvedValue("ok");
  return {
    send,
    deps: {
      local,
      directory: {
        lookup: async () => undefined,
        register: async () => ({ grainId: target, silo: winner, activationId: "act-winner" }),
      } as unknown as GrainDirectory,
      cache: {
        get: () => undefined,
        put: () => undefined,
        invalidate: () => undefined,
      } as unknown as LocationCache,
      catalog: {
        isStatelessWorkerType: () => false,
        resolveLive: async () => undefined,
      } as unknown as Catalog,
      remote: { send },
      activeSilos: () => [local, winner],
      placementFor: () => {
        throw new Error("placement must not run: deliverLocal never falls through to it");
      },
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0 }),
    },
  };
}

// Issue #110: a silo that received a call for a grain it doesn't (or no
// longer) hosts used to forward it on with no signal telling the ORIGINAL
// caller its cached/looked-up address is wrong, and with no bound on how many
// times a single call could be forwarded -- an inconsistent directory view
// loops only until the 30s call timeout. Orleans caps this at
// `MaxForwardCount` (2) and tells the caller to invalidate its cache
// (`MessageCenter.AddToCacheInvalidationHeader`).
describe("DistributedDispatcher forwarding (issue #110)", () => {
  it("signals opts.onForward with the CAS winner and stamps the forward on the wire request", async () => {
    const { deps, send } = forwardingDeps();
    let forwardedTo: SiloAddress | undefined;

    const result = await new DistributedDispatcher(deps).deliverLocal(request(), {
      onForward: (to) => {
        forwardedTo = to;
      },
    });

    expect(result).toBe("ok");
    expect(forwardedTo).toBe(winner);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0]![0]).toBe(winner);
    expect((send.mock.calls[0]![1] as InvocationRequest).forwardCount).toBe(1);
  });

  it("increments forwardCount on each further forward", async () => {
    const { deps, send } = forwardingDeps();

    await new DistributedDispatcher(deps).deliverLocal(request({ forwardCount: 1 }));

    expect((send.mock.calls[0]![1] as InvocationRequest).forwardCount).toBe(2);
  });

  it("rejects instead of forwarding once the cap is reached, without sending anything further", async () => {
    const { deps, send } = forwardingDeps();

    const err = await new DistributedDispatcher(deps)
      .deliverLocal(request({ forwardCount: 2 }))
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(RejectionError);
    expect((err as RejectionError).kind).toBe("noActivation");
    expect(send).not.toHaveBeenCalled();
  });
});
