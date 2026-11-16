import { describe, expect, it, vi } from "vitest";
import { LifecycleSubject, type LifecycleObserver } from "@thresh/runtime/lifecycle-subject";

function recordingObserver(
  log: string[],
  name: string,
  opts?: { failOnStart?: boolean; failOnStop?: boolean },
): LifecycleObserver {
  return {
    onStart: async () => {
      log.push(`start:${name}`);
      if (opts?.failOnStart) throw new Error(`fail-start:${name}`);
    },
    onStop: async () => {
      log.push(`stop:${name}`);
      if (opts?.failOnStop) throw new Error(`fail-stop:${name}`);
    },
  };
}

describe("LifecycleSubject", () => {
  it("starts observers in ascending stage order and stops in reverse order", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("c", 2, recordingObserver(log, "c"));
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("b", 1, recordingObserver(log, "b"));

    await subject.onStart();
    expect(log).toEqual(["start:a", "start:b", "start:c"]);

    await subject.onStop();
    expect(log).toEqual(["start:a", "start:b", "start:c", "stop:c", "stop:b", "stop:a"]);
  });

  it("stops the observers within one stage in the reverse of their subscription order, even when the first is slower", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, {
      onStart: async () => void log.push("start:a"),
      onStop: async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        log.push("stop:a");
      },
    });
    subject.subscribe("b", 0, recordingObserver(log, "b"));
    subject.subscribe("c", 0, recordingObserver(log, "c"));
    await subject.onStart();
    await subject.onStop();
    expect(log.filter((l) => l.startsWith("stop:"))).toEqual(["stop:c", "stop:b", "stop:a"]);
  });

  it("runs all observers within a stage before advancing to the next stage", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("a1", 0, recordingObserver(log, "a1"));
    subject.subscribe("a2", 0, recordingObserver(log, "a2"));
    subject.subscribe("b1", 1, recordingObserver(log, "b1"));

    await subject.onStart();
    expect(log.slice(0, 2).sort()).toEqual(["start:a1", "start:a2"]);
    expect(log[2]).toBe("start:b1");
  });

  it("aborts remaining stages when onStart throws, leaving unwind to a subsequent onStop", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("b", 1, recordingObserver(log, "b", { failOnStart: true }));
    subject.subscribe("c", 2, recordingObserver(log, "c"));

    await expect(subject.onStart()).rejects.toThrow("fail-start:b");
    // stage 2 ("c") never started.
    expect(log).toEqual(["start:a", "start:b"]);

    // A subsequent onStop() unwinds everything started so far, including the
    // failed stage, in reverse order — stage 2 ("c") is never stopped either.
    await subject.onStop();
    expect(log).toEqual(["start:a", "start:b", "stop:b", "stop:a"]);
  });

  it("continues stopping remaining stages when an onStop observer throws", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("b", 1, recordingObserver(log, "b", { failOnStop: true }));
    subject.subscribe("c", 2, recordingObserver(log, "c"));

    await subject.onStart();
    await expect(subject.onStop()).resolves.toBeUndefined();

    expect(log).toEqual(["start:a", "start:b", "start:c", "stop:c", "stop:b", "stop:a"]);
  });

  it("onStop is a no-op if the lifecycle was never started", async () => {
    const subject = new LifecycleSubject();
    await expect(subject.onStop()).resolves.toBeUndefined();
  });

  it("throws when subscribing after the lifecycle has started", async () => {
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, recordingObserver([], "a"));
    await subject.onStart();
    expect(() => subject.subscribe("late", 0, recordingObserver([], "late"))).toThrow();
  });
});

describe("LifecycleSubject failure reporting", () => {
  it("reports a failed stop with the observer's name and stage, and keeps stopping", async () => {
    const log: string[] = [];
    const failures: Array<{ phase: string; observerName: string; stage: number }> = [];
    const subject = new LifecycleSubject({
      onError: (f) =>
        failures.push({ phase: f.phase, observerName: f.observerName, stage: f.stage }),
    });
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("b", 1, recordingObserver(log, "b", { failOnStop: true }));
    await subject.onStart();
    await subject.onStop();
    expect(log).toEqual(["start:a", "start:b", "stop:b", "stop:a"]);
    expect(failures).toEqual([{ phase: "stop", observerName: "b", stage: 1 }]);
  });

  it("treats an observer that throws synchronously like one that rejects", async () => {
    const log: string[] = [];
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("b", 1, {
      onStart: async () => undefined,
      onStop: () => {
        throw new Error("sync stop failure");
      },
    });
    await subject.onStart();
    await expect(subject.onStop()).resolves.toBeUndefined();
    expect(log).toEqual(["start:a", "stop:a"]);
  });

  it("ignores a reporter that throws", async () => {
    const subject = new LifecycleSubject({
      onError: () => {
        throw new Error("reporter down");
      },
    });
    subject.subscribe("a", 0, recordingObserver([], "a", { failOnStart: true }));
    await expect(subject.onStart()).rejects.toThrow("fail-start:a");
  });

  it("halts a start that a stop interrupts, stopping the stage in flight and never starting a later one", async () => {
    const log: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, recordingObserver(log, "a"));
    subject.subscribe("slow", 1, {
      onStart: async () => {
        log.push("start:slow");
        await gate;
      },
      onStop: async () => void log.push("stop:slow"),
    });
    subject.subscribe("after", 2, recordingObserver(log, "after"));

    const started = subject.onStart();
    const settled = started.then(
      () => "resolved",
      () => "rejected",
    );
    await vi.waitFor(() => expect(log).toContain("start:slow"));
    const stopped = subject.onStop();
    release();
    await stopped;
    expect(await settled).toBe("rejected");
    expect(log).toEqual(["start:a", "start:slow", "stop:slow", "stop:a"]);
  });

  it("stops a stage whose start finishes after a stop timed out waiting for it", async () => {
    const log: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const subject = new LifecycleSubject({
      stopTimeoutMs: 5,
      onError: ({ observerName }) => void log.push(`error:${observerName}`),
    });
    subject.subscribe("slow", 1, {
      onStart: async () => {
        log.push("start:slow");
        await gate;
      },
      onStop: async () => void log.push("stop:slow"),
    });
    subject.subscribe("after", 2, recordingObserver(log, "after"));
    const started = subject.onStart().catch(() => undefined);
    await vi.waitFor(() => expect(log).toContain("start:slow"));
    await subject.onStop();
    release();
    await started;
    expect(log.filter((l) => l.startsWith("start:") || l.startsWith("stop:"))).toEqual([
      "start:slow",
      "stop:slow",
    ]);
  });

  it("gives up on a stop that never settles, reports it, and runs the stages below", async () => {
    const log: string[] = [];
    const failures: string[] = [];
    const subject = new LifecycleSubject({
      stopTimeoutMs: 10,
      onError: ({ observerName, phase, error }) =>
        void failures.push(`${phase}:${observerName}:${(error as Error).message}`),
    });
    subject.subscribe("low", 0, recordingObserver(log, "low"));
    subject.subscribe("hung", 1, {
      onStart: async () => undefined,
      onStop: () => new Promise<void>(() => undefined),
    });
    await subject.onStart();
    await subject.onStop();
    expect(log).toEqual(["start:low", "stop:low"]);
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatch(/^stop:hung:.*timed out/);
  });

  it("returns the same pending stop to a second caller", async () => {
    const subject = new LifecycleSubject();
    subject.subscribe("a", 0, { onStart: async () => undefined, onStop: async () => undefined });
    await subject.onStart();
    expect(subject.onStop()).toBe(subject.onStop());
  });
});
