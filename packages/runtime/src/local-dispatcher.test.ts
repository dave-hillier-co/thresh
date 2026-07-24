import { describe, expect, it, vi } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { InvocationRequest } from "@tsva/core/request";
import type { Catalog } from "@tsva/runtime/catalog";
import type { InvokeCallOptions } from "@tsva/runtime/dispatcher";
import { LocalDispatcher } from "@tsva/runtime/local-dispatcher";

// GAP-CANCELLATION-DISPATCHER: `LocalDispatcher` previously had no way to
// carry a caller's `InvokeCallOptions` (ambient signal / per-call deadline)
// through to the activation it resolves. A fake `Catalog` (just the methods
// `LocalDispatcher` actually calls) keeps this sociable without a full
// `Catalog`/scheduler stack.
function fakeCatalog(target: GrainId): { catalog: Catalog; invoke: ReturnType<typeof vi.fn> } {
  const invoke = vi.fn().mockResolvedValue("ok");
  const activation = { invoke };
  const catalog = {
    isStatelessWorkerType: () => false,
    getOrCreate: async (id: GrainId) => {
      expect(id.equals(target)).toBe(true);
      return activation;
    },
    pickOrScaleWorker: () => activation,
  } as unknown as Catalog;
  return { catalog, invoke };
}

const req = (target: GrainId): InvocationRequest => ({
  target,
  interfaceId: 0,
  method: "ping",
  args: [],
  options: {},
  reentrancyId: "r1",
});

describe("LocalDispatcher ambient cancellation/deadline", () => {
  it("passes InvokeCallOptions through to the resolved activation's invoke", async () => {
    const target = new GrainId("Thing", "a");
    const { catalog, invoke } = fakeCatalog(target);
    const dispatcher = new LocalDispatcher(catalog);
    const controller = new AbortController();
    const opts: InvokeCallOptions = { signal: controller.signal };

    await dispatcher.invoke(req(target), opts);

    expect(invoke).toHaveBeenCalledTimes(1);
    const [, passedOpts] = invoke.mock.calls[0]!;
    expect(passedOpts).toBe(opts);
  });

  it("resolves opts.deadlineMs into req.deadline before delivering, leaving req.deadline untouched otherwise", async () => {
    const target = new GrainId("Thing", "b");
    const { catalog, invoke } = fakeCatalog(target);
    const dispatcher = new LocalDispatcher(catalog);
    const before = Date.now();

    await dispatcher.invoke(req(target), { deadlineMs: 5_000 });

    const [passedReq] = invoke.mock.calls[0]!;
    expect(passedReq.deadline).toBeGreaterThanOrEqual(before + 5_000);
    expect(passedReq.deadline).toBeLessThan(before + 5_000 + 2_000); // generous slack, no fake clock here
  });

  it("does not override an already-set req.deadline with opts.deadlineMs", async () => {
    const target = new GrainId("Thing", "c");
    const { catalog, invoke } = fakeCatalog(target);
    const dispatcher = new LocalDispatcher(catalog);
    const original = req(target);
    original.deadline = 123;

    await dispatcher.invoke(original, { deadlineMs: 5_000 });

    const [passedReq] = invoke.mock.calls[0]!;
    expect(passedReq.deadline).toBe(123);
  });
});
