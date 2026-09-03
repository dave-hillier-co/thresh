import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { LogFields, Logger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import { LocationCache } from "@thresh/directory/location-cache";
import type { Catalog } from "@thresh/runtime/catalog";
import {
  DistributedDispatcher,
  type DistributedDispatcherDeps,
} from "@thresh/runtime/distributed-dispatcher";
import { RandomPlacement } from "@thresh/runtime/placement/random-placement";
import { waitFor } from "@thresh/testing/wait";

// The multi-silo half of the location-transparency fix: a `oneWay` call whose
// callee happens to live on THIS silo must resolve as promptly as one that
// hits the transport (`ClusterNode.sendRemote` returns right after `send`),
// instead of awaiting the local activation's whole turn.

const local = new SiloAddress("silo-0", "uid-0", "silo-0:1");
const remote = new SiloAddress("silo-1", "uid-1", "silo-1:1");
const target = new GrainId("Counter", "k");
const activationId = "act-1";

const request = (options: { oneWay?: boolean } = {}): InvocationRequest => ({
  target,
  interfaceId: 1,
  method: "notify",
  args: [],
  options,
  reentrancyId: "r",
});

interface RecordedLog {
  message: string;
  fields?: LogFields;
}

/** A `Logger` that just records its warnings, so the detached-failure log is assertable. */
function recordingLogger(): { logger: Logger; warnings: RecordedLog[] } {
  const warnings: RecordedLog[] = [];
  return {
    warnings,
    logger: {
      debug: () => undefined,
      info: () => undefined,
      warn: (message, fields) => {
        warnings.push({ message, ...(fields !== undefined ? { fields } : {}) });
      },
      error: () => undefined,
    },
  };
}

/**
 * Deps whose cache already points at a live LOCAL activation, so `invoke`
 * takes the `routeTo` -> `activation.invoke` path — the one that used to make
 * a one-way caller wait for the callee's turn.
 */
function localDeps(invoke: ReturnType<typeof vi.fn>): {
  deps: DistributedDispatcherDeps;
  warnings: RecordedLog[];
  send: ReturnType<typeof vi.fn>;
} {
  const { logger, warnings } = recordingLogger();
  const send = vi.fn().mockResolvedValue(undefined);
  const activation = { activationId, invoke };
  return {
    warnings,
    send,
    deps: {
      local,
      directory: { lookup: async () => undefined } as unknown as GrainDirectory,
      cache: {
        get: () => ({ grainId: target, silo: local, activationId }),
        put: () => undefined,
        invalidate: () => undefined,
      } as unknown as LocationCache,
      catalog: {
        isStatelessWorkerType: () => false,
        resolveLive: async () => activation,
      } as unknown as Catalog,
      remote: { send },
      activeSilos: () => [local, remote],
      placementFor: () => new RandomPlacement(),
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0 }),
      logger,
    },
  };
}

describe("DistributedDispatcher one-way delivery to a local activation", () => {
  let unhandled: unknown[];
  const onUnhandledRejection = (reason: unknown): void => {
    unhandled.push(reason);
  };

  beforeEach(() => {
    unhandled = [];
    process.on("unhandledRejection", onUnhandledRejection);
  });

  afterEach(() => {
    process.off("unhandledRejection", onUnhandledRejection);
  });

  it("resolves the caller while the local callee's turn is still running", async () => {
    let settle!: () => void;
    let ran = false;
    const turn = new Promise<void>((resolve) => (settle = resolve)).then(() => {
      ran = true;
    });
    const invoke = vi.fn().mockReturnValue(turn);
    const { deps } = localDeps(invoke);

    await expect(new DistributedDispatcher(deps).invoke(request({ oneWay: true }))).resolves.toBe(
      undefined,
    );

    expect(invoke).toHaveBeenCalledTimes(1); // the turn WAS scheduled
    expect(ran).toBe(false); // but the caller did not wait for it
    settle();
    await turn;
    expect(ran).toBe(true);
  });

  it("still awaits a non-oneWay call to a local activation", async () => {
    let settled = false;
    const invoke = vi.fn().mockImplementation(async () => {
      await Promise.resolve();
      settled = true;
      return "ok";
    });
    const { deps } = localDeps(invoke);

    await expect(new DistributedDispatcher(deps).invoke(request())).resolves.toBe("ok");
    expect(settled).toBe(true);
  });

  it("logs a detached local callee's failure instead of rejecting the caller", async () => {
    const failure = new Error("callee exploded");
    const invoke = vi.fn().mockRejectedValue(failure);
    const { deps, warnings } = localDeps(invoke);

    await expect(
      new DistributedDispatcher(deps).invoke(request({ oneWay: true })),
    ).resolves.toBeUndefined();
    await new Promise((r) => setTimeout(r, 20));

    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.fields).toMatchObject({
      grainId: target.toString(),
      method: "notify",
      error: failure,
    });
    expect(unhandled).toEqual([]);
  });

  it("still forwards a one-way call whose callee lives on another silo", async () => {
    const invoke = vi.fn();
    const { deps, send } = localDeps(invoke);
    const remoteDeps: DistributedDispatcherDeps = {
      ...deps,
      cache: {
        get: () => ({ grainId: target, silo: remote, activationId }),
        put: () => undefined,
        invalidate: () => undefined,
      } as unknown as LocationCache,
    };

    await expect(
      new DistributedDispatcher(remoteDeps).invoke(request({ oneWay: true })),
    ).resolves.toBeUndefined();

    expect(invoke).not.toHaveBeenCalled();
    expect(send).toHaveBeenCalledTimes(1);
    expect((send.mock.calls[0]![0] as SiloAddress).ringKey).toBe("silo-1");
  });

  it("keeps successive one-way calls to one local activation in admission order", async () => {
    const admitted: string[] = [];
    // Mirrors `ActivationData.invoke`, which pushes its turn onto the
    // scheduler queue synchronously before its first await: admission order
    // is therefore call order, and must stay so when the promise is detached.
    const invoke = vi.fn().mockImplementation((req: InvocationRequest) => {
      admitted.push(String(req.args[0]));
      return new Promise<void>((resolve) => setTimeout(resolve, 5));
    });
    const { deps } = localDeps(invoke);
    const dispatcher = new DistributedDispatcher(deps);

    await Promise.all(
      ["1", "2", "3"].map((tag) =>
        dispatcher.invoke({ ...request({ oneWay: true }), args: [tag] }),
      ),
    );

    expect(admitted).toEqual(["1", "2", "3"]);
  });
});

// Detaching the one-way caller means successive one-way calls are now IN FLIGHT
// TOGETHER (before the change, the caller held the whole delivery, so call n+1
// did not start until call n's turn had finished). For a grain that is not yet
// activated, they therefore all race the cache/directory/placement funnel at
// once — so the funnel itself, not the awaiting caller, has to be what keeps
// them in order and what keeps them converging on ONE activation. Orleans gets
// this from the target activation's own message queue: messages for a grain
// that is still coming up wait on the activation in arrival order rather than
// each independently re-running placement.

interface FakeActivation {
  readonly activationId: string;
  readonly state: string;
  invoke(req: InvocationRequest): Promise<unknown>;
}

/**
 * A directory whose `register` CAS is decided in ARRIVAL order but whose
 * promises settle in an order the test picks — the shape a real out-of-process
 * directory has (Redis/Postgres round trips over multiplexed connections need
 * not complete in request order), and the shape an in-memory `Map` fake cannot
 * exhibit. `settle: "in-order"` resolves each register as it is decided;
 * `"reversed"` holds every response until the test drains them last-first.
 */
function racyDirectory(settle: "in-order" | "reversed"): {
  directory: GrainDirectory;
  unregister: ReturnType<typeof vi.fn>;
} {
  const entries = new Map<string, GrainAddress>();
  const pending: (() => void)[] = [];
  const flushReversed = (): void => {
    while (pending.length > 0) pending.pop()!();
  };
  const unregister = vi.fn(async (addr: GrainAddress) => {
    const held = entries.get(addr.grainId.toString());
    if (held?.activationId === addr.activationId) entries.delete(addr.grainId.toString());
  });
  return {
    unregister,
    directory: {
      lookup: async (grainId: GrainId) => entries.get(grainId.toString()),
      register: async (addr: GrainAddress) => {
        // Several microtask hops before the CAS, so every caller that started
        // "at about the same time" is genuinely in flight when it is decided.
        for (let i = 0; i < 8; i += 1) await Promise.resolve();
        const key = addr.grainId.toString();
        const winner = entries.get(key) ?? addr;
        entries.set(key, winner);
        if (settle === "in-order") return winner;
        // Hold the response one macrotask, then hand back every response that
        // accumulated in that window LAST-FIRST, so a CAS loser can resume
        // before the winner it lost to.
        await new Promise<void>((resolve) => {
          pending.push(resolve);
          setTimeout(flushReversed, 1);
        });
        return winner;
      },
      unregister,
      unregisterSilo: async () => undefined,
    } as unknown as GrainDirectory,
  };
}

/**
 * Deps for a grain with NO activation anywhere yet: the first call has to run
 * the whole funnel (cache miss -> directory miss -> placement -> CAS ->
 * `Catalog.activateLocal`). The catalog fake mirrors the real
 * `Catalog.getOrActivate` in the property that matters here — it is not
 * `async` and stores the new activation synchronously, so a second caller can
 * never create a second activation for the same id.
 */
function coldDeps(
  admitted: string[],
  settle: "in-order" | "reversed" = "in-order",
): {
  deps: DistributedDispatcherDeps;
  activations: Map<string, FakeActivation>;
  unregister: ReturnType<typeof vi.fn>;
  send: ReturnType<typeof vi.fn>;
} {
  const { logger } = recordingLogger();
  const { directory, unregister } = racyDirectory(settle);
  const activations = new Map<string, FakeActivation>();
  const send = vi.fn().mockResolvedValue(undefined);
  const catalog = {
    isStatelessWorkerType: () => false,
    resolveLive: (id: GrainId) => Promise.resolve(activations.get(id.toString())),
    get: (id: GrainId) => activations.get(id.toString()),
    activateLocal: (id: GrainId, actId: string) => {
      const key = id.toString();
      const existing = activations.get(key);
      if (existing !== undefined) return Promise.resolve(existing);
      const created: FakeActivation = {
        activationId: actId,
        state: "valid",
        invoke: (req) => {
          admitted.push(String(req.args[0]));
          return new Promise<void>((resolve) => setTimeout(resolve, 1));
        },
      };
      activations.set(key, created);
      return Promise.resolve(created);
    },
  } as unknown as Catalog;
  return {
    activations,
    unregister,
    send,
    deps: {
      local,
      directory,
      cache: new LocationCache(),
      catalog,
      remote: { send },
      activeSilos: () => [local],
      placementFor: () => new RandomPlacement(),
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0 }),
      logger,
    },
  };
}

describe("DistributedDispatcher one-way delivery to a not-yet-activated grain", () => {
  it("admits successive one-way calls in call order while the activation is coming up", async () => {
    const admitted: string[] = [];
    const { deps } = coldDeps(admitted);
    const dispatcher = new DistributedDispatcher(deps);

    // Sequential from the caller's point of view — exactly the code the fix is
    // for — but each returns before its delivery has resolved a placement, so
    // all three are inside the funnel together.
    await dispatcher.invoke({ ...request({ oneWay: true }), args: ["1"] });
    await dispatcher.invoke({ ...request({ oneWay: true }), args: ["2"] });
    await dispatcher.invoke({ ...request({ oneWay: true }), args: ["3"] });

    await waitFor(() => admitted.length === 3);
    expect(admitted).toEqual(["1", "2", "3"]);
  });

  it("admits same-tick one-way calls in call order while the activation is coming up", async () => {
    const admitted: string[] = [];
    const { deps } = coldDeps(admitted);
    const dispatcher = new DistributedDispatcher(deps);

    // Issued in one synchronous slice, so every call is at the same point in
    // the funnel: nothing but the funnel's own ordering can order them, and a
    // local (`LocalDispatcher`) callee orders them by call order, so a
    // distributed one must too.
    await Promise.all(
      ["1", "2", "3"].map((tag) =>
        dispatcher.invoke({ ...request({ oneWay: true }), args: [tag] }),
      ),
    );

    await waitFor(() => admitted.length === 3);
    expect(admitted).toEqual(["1", "2", "3"]);
  });

  it("activates the grain once and never unregisters the winner's fresh entry", async () => {
    const admitted: string[] = [];
    const { deps, activations, unregister } = coldDeps(admitted, "reversed");
    const dispatcher = new DistributedDispatcher(deps);

    await Promise.all(
      ["1", "2"].map((tag) => dispatcher.invoke({ ...request({ oneWay: true }), args: [tag] })),
    );

    await waitFor(() => admitted.length === 2);
    expect(admitted).toEqual(["1", "2"]);
    expect(activations.size).toBe(1);
    // The CAS loser must never tear down a registration whose winner is still
    // being brought up on this silo: that would leave the live activation with
    // no directory entry, and another silo would then place a SECOND one.
    expect(unregister).not.toHaveBeenCalled();
  });

  it("never unregisters a winner that is still coming up, whatever kind of call lost the CAS", async () => {
    const admitted: string[] = [];
    const { deps, activations, unregister } = coldDeps(admitted, "reversed");
    const dispatcher = new DistributedDispatcher(deps);

    // Ordinary (awaited) calls: they are NOT funnelled through the one-way
    // claim gate — an awaited caller's claim races as it always has — so this
    // is the case the loser branch itself has to get right.
    await Promise.all(["1", "2"].map((tag) => dispatcher.invoke({ ...request(), args: [tag] })));

    expect(admitted).toHaveLength(2);
    expect(activations.size).toBe(1);
    expect(unregister).not.toHaveBeenCalled();
  });
});
