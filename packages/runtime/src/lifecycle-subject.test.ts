import { describe, expect, it } from "vitest";
import { LifecycleSubject, type LifecycleObserver } from "@tsva/runtime/lifecycle-subject";

function recordingObserver(log: string[], name: string, opts?: { failOnStart?: boolean; failOnStop?: boolean }): LifecycleObserver {
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
