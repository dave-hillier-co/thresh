import { describe, expect, it } from "vitest";
import { TurnScheduler } from "@tsva/runtime/turn-scheduler";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Flush pending microtasks and the macrotask queue. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("TurnScheduler", () => {
  it("runs exclusive turns one at a time, in FIFO order", async () => {
    const sched = new TurnScheduler();
    const log: string[] = [];
    const a = deferred();
    const b = deferred();

    void sched.schedule({
      options: {},
      run: async () => {
        log.push("A:start");
        await a.promise;
        log.push("A:end");
      },
    });
    const pB = sched.schedule({
      options: {},
      run: async () => {
        log.push("B:start");
        await b.promise;
        log.push("B:end");
      },
    });

    await flush();
    expect(log).toEqual(["A:start"]); // B is queued behind the running exclusive turn

    a.resolve();
    await flush();
    expect(log).toEqual(["A:start", "A:end", "B:start"]);

    b.resolve();
    await pB;
    expect(log).toEqual(["A:start", "A:end", "B:start", "B:end"]);
  });

  it("propagates the turn's result and errors to the caller", async () => {
    const sched = new TurnScheduler();
    await expect(sched.schedule({ options: {}, run: async () => 42 })).resolves.toBe(42);
    await expect(
      sched.schedule({
        options: {},
        run: async () => {
          throw new Error("boom");
        },
      }),
    ).rejects.toThrow("boom");
  });

  it("interleaves read-only turns with each other", async () => {
    const sched = new TurnScheduler();
    const log: string[] = [];
    const r1 = deferred();
    const r2 = deferred();
    void sched.schedule({
      options: { readOnly: true },
      run: async () => {
        log.push("r1:start");
        await r1.promise;
      },
    });
    void sched.schedule({
      options: { readOnly: true },
      run: async () => {
        log.push("r2:start");
        await r2.promise;
      },
    });
    await flush();
    expect(log).toEqual(["r1:start", "r2:start"]);
    r1.resolve();
    r2.resolve();
  });

  it("does not admit a read-only turn while an exclusive turn runs", async () => {
    const sched = new TurnScheduler();
    const log: string[] = [];
    const w = deferred();
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("w:start");
        await w.promise;
        log.push("w:end");
      },
    });
    void sched.schedule({
      options: { readOnly: true },
      run: async () => {
        log.push("ro:start");
      },
    });
    await flush();
    expect(log).toEqual(["w:start"]);
    w.resolve();
    await flush();
    expect(log).toEqual(["w:start", "w:end", "ro:start"]);
  });

  it("admits an alwaysInterleave turn while an exclusive turn runs", async () => {
    const sched = new TurnScheduler();
    const log: string[] = [];
    const w = deferred();
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("w:start");
        await w.promise;
      },
    });
    void sched.schedule({
      options: { alwaysInterleave: true },
      run: async () => {
        log.push("ai:start");
      },
    });
    await flush();
    expect(log).toEqual(["w:start", "ai:start"]);
    w.resolve();
  });

  it("admits a turn whose call-chain reentrancy id is already active", async () => {
    const sched = new TurnScheduler();
    const log: string[] = [];
    const root = deferred();
    void sched.schedule({
      options: {},
      reentrancyId: "chain-X",
      run: async () => {
        log.push("x1:start");
        await root.promise;
      },
    });
    // Same chain id: admitted concurrently (this is the deadlock-avoidance case).
    void sched.schedule({
      options: {},
      reentrancyId: "chain-X",
      run: async () => {
        log.push("x2:start");
      },
    });
    // No / different chain id: queued behind the running exclusive turn.
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("y:start");
      },
    });
    await flush();
    expect(log).toEqual(["x1:start", "x2:start"]);
    root.resolve();
    await flush();
    expect(log).toEqual(["x1:start", "x2:start", "y:start"]);
  });

  it("interleaves everything for a fully reentrant scheduler", async () => {
    const sched = new TurnScheduler({ reentrant: true });
    const log: string[] = [];
    const a = deferred();
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("a:start");
        await a.promise;
      },
    });
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("b:start");
      },
    });
    await flush();
    expect(log).toEqual(["a:start", "b:start"]);
    a.resolve();
  });

  it("barrierFirstTurn: a reentrant scheduler still queues everything behind the first turn", async () => {
    const sched = new TurnScheduler({ reentrant: true, barrierFirstTurn: true });
    const log: string[] = [];
    const activation = deferred();
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("activate:start");
        await activation.promise;
        log.push("activate:end");
      },
    });
    void sched.schedule({
      options: {},
      run: async () => {
        log.push("call:start");
      },
    });
    await flush();
    // The second turn must not start until the first (activation) settles,
    // even though the scheduler is fully reentrant.
    expect(log).toEqual(["activate:start"]);
    activation.resolve();
    await flush();
    expect(log).toEqual(["activate:start", "activate:end", "call:start"]);
  });

  describe("mayInterleave predicate (Orleans' [MayInterleave])", () => {
    it("admits an incoming turn whose method matches the predicate while another turn runs", async () => {
      const sched = new TurnScheduler({ mayInterleave: (method) => method === "goFast" });
      const log: string[] = [];
      const w = deferred();
      void sched.schedule({
        options: {},
        method: "goSlow",
        run: async () => {
          log.push("slow:start");
          await w.promise;
        },
      });
      void sched.schedule({
        options: {},
        method: "goFast",
        run: async () => {
          log.push("fast:start");
        },
      });
      await flush();
      // The predicate matches the incoming "goFast" call, so it interleaves with
      // the running "goSlow" call rather than queueing behind it.
      expect(log).toEqual(["slow:start", "fast:start"]);
      w.resolve();
    });

    it("queues an incoming turn whose method does not match the predicate", async () => {
      const sched = new TurnScheduler({ mayInterleave: (method) => method === "goFast" });
      const log: string[] = [];
      const w = deferred();
      void sched.schedule({
        options: {},
        method: "goSlow",
        run: async () => {
          log.push("slow1:start");
          await w.promise;
          log.push("slow1:end");
        },
      });
      void sched.schedule({
        options: {},
        method: "goSlow",
        run: async () => {
          log.push("slow2:start");
        },
      });
      await flush();
      // Neither the incoming nor the running call matches the predicate, so the
      // second "goSlow" queues behind the first exactly like the no-predicate case.
      expect(log).toEqual(["slow1:start"]);
      w.resolve();
      await flush();
      expect(log).toEqual(["slow1:start", "slow1:end", "slow2:start"]);
    });

    it("leaves a scheduler with no configured predicate unaffected (existing behavior)", async () => {
      const sched = new TurnScheduler();
      const log: string[] = [];
      const w = deferred();
      void sched.schedule({
        options: {},
        method: "goSlow",
        run: async () => {
          log.push("w:start");
          await w.promise;
          log.push("w:end");
        },
      });
      void sched.schedule({
        options: {},
        method: "goFast",
        run: async () => {
          log.push("fast:start");
        },
      });
      await flush();
      // No predicate configured: the method name is irrelevant — same FIFO
      // exclusive-turn behavior as every other test in this file.
      expect(log).toEqual(["w:start"]);
      w.resolve();
      await flush();
      expect(log).toEqual(["w:start", "w:end", "fast:start"]);
    });
  });
});
