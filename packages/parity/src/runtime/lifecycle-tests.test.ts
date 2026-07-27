// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Lifecycle/LifecycleTests.cs @ v10.1.0 (MIT).
// This exercises Orleans.Runtime's LifecycleSubject: a numbered-stage
// ILifecycleObservable/ILifecycleObserver protocol (Subscribe(stage, observer),
// OnStart()/OnStop() driving observers stage-by-stage, with cascading
// stage-abort semantics on a failed OnStart). Ported to @thresh/runtime's
// LifecycleSubject (packages/runtime/src/lifecycle-subject.ts), a standalone
// primitive driven directly here, exactly as upstream — it is not wired into
// grain/silo lifecycle, which stays a flat before/after hook pair (see
// GrainLifecycle in packages/core/src/define-grain.ts).
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { LifecycleSubject, type LifecycleObserver } from "@thresh/runtime/lifecycle-subject";

enum TestStages {
  Down = 0,
  Initialize = 1,
  Configure = 2,
  Run = 3,
}

const allTestStages = [
  TestStages.Down,
  TestStages.Initialize,
  TestStages.Configure,
  TestStages.Run,
];

class Observer implements LifecycleObserver {
  started = false;
  stopped = false;
  failedOnStart = false;
  failedOnStop = false;

  constructor(
    private readonly failOnStart: boolean,
    private readonly failOnStop: boolean,
  ) {}

  async onStart(): Promise<void> {
    this.started = true;
    this.failedOnStart = this.failOnStart;
    if (this.failOnStart) throw new Error("failOnStart");
  }

  async onStop(): Promise<void> {
    this.stopped = true;
    this.failedOnStop = this.failOnStop;
    if (this.failOnStop) throw new Error("failOnStop");
  }
}

async function runLifecycle(
  observerCountByStage: Map<TestStages, number>,
  failOnStart: TestStages | undefined,
  failOnStop: TestStages | undefined,
): Promise<Map<TestStages, Observer[]>> {
  const observersByStage = new Map<TestStages, Observer[]>();
  const lifecycle = new LifecycleSubject();
  for (const [stage, count] of observerCountByStage) {
    const observers = Array.from(
      { length: count },
      () => new Observer(failOnStart === stage, failOnStop === stage),
    );
    observersByStage.set(stage, observers);
    observers.forEach((o) => lifecycle.subscribe("observer", stage, o));
  }

  if (failOnStart !== undefined) {
    await expect(lifecycle.onStart()).rejects.toThrow();
  } else {
    await lifecycle.onStart();
  }
  await lifecycle.onStop();

  return observersByStage;
}

function observerCountsPerStage(observersPerStage: number): Map<TestStages, number> {
  const map = new Map<TestStages, number>();
  for (const stage of allTestStages) map.set(stage, observersPerStage);
  return map;
}

describe("Tester.LifecycleTests", () => {
  orleansTest("Tester.LifecycleTests.FullLifecycleTest", async () => {
    const observerCountByStage = observerCountsPerStage(2);
    const observersByStage = await runLifecycle(observerCountByStage, undefined, undefined);

    expect(observersByStage.size).toBe(observerCountByStage.size);
    for (const [stage, observers] of observersByStage) {
      expect(observers.length).toBe(observerCountByStage.get(stage));
      expect(observers.every((o) => o.started)).toBe(true);
      expect(observers.every((o) => o.stopped)).toBe(true);
      expect(observers.every((o) => !o.failedOnStart)).toBe(true);
      expect(observers.every((o) => !o.failedOnStop)).toBe(true);
    }
  });

  orleansTest("Tester.LifecycleTests.FailOnStartOnEachStageLifecycleTest", async () => {
    const observerCountByStage = observerCountsPerStage(2);

    for (const stage of allTestStages) {
      const observersByStage = await runLifecycle(observerCountByStage, stage, undefined);

      expect(observersByStage.size).toBe(observerCountByStage.size);
      for (const [key, observers] of observersByStage) {
        expect(observers.length).toBe(observerCountByStage.get(key));
        if (key < stage) {
          expect(observers.every((o) => o.started)).toBe(true);
          expect(observers.every((o) => o.stopped)).toBe(true);
          expect(observers.every((o) => !o.failedOnStart)).toBe(true);
          expect(observers.every((o) => !o.failedOnStop)).toBe(true);
        } else if (key === stage) {
          expect(observers.every((o) => o.started)).toBe(true);
          expect(observers.every((o) => o.stopped)).toBe(true);
          expect(observers.every((o) => o.failedOnStart)).toBe(true);
          expect(observers.every((o) => !o.failedOnStop)).toBe(true);
        } else {
          expect(observers.every((o) => !o.started)).toBe(true);
          expect(observers.every((o) => !o.stopped)).toBe(true);
          expect(observers.every((o) => !o.failedOnStart)).toBe(true);
          expect(observers.every((o) => !o.failedOnStop)).toBe(true);
        }
      }
    }
  });

  orleansTest("Tester.LifecycleTests.FailOnStopOnEachStageLifecycleTest", async () => {
    const observerCountByStage = observerCountsPerStage(2);

    for (const stage of allTestStages) {
      const observersByStage = await runLifecycle(observerCountByStage, undefined, stage);

      expect(observersByStage.size).toBe(observerCountByStage.size);
      for (const [key, observers] of observersByStage) {
        expect(observers.length).toBe(observerCountByStage.get(key));
        expect(observers.every((o) => o.started)).toBe(true);
        expect(observers.every((o) => o.stopped)).toBe(true);
        expect(observers.every((o) => !o.failedOnStart)).toBe(true);
        if (key === stage) {
          expect(observers.every((o) => o.failedOnStop)).toBe(true);
        } else {
          expect(observers.every((o) => !o.failedOnStop)).toBe(true);
        }
      }
    }
  });

  orleansTest("Tester.LifecycleTests.MultiStageObserverLifecycleTest", async () => {
    const started = new Map<TestStages, boolean>();
    const stopped = new Map<TestStages, boolean>();
    const lifecycle = new LifecycleSubject();

    // MultiStageObserver.Participate: subscribes itself at every stage.
    for (const stage of allTestStages) {
      lifecycle.subscribe("MultiStageObserver", stage, {
        onStart: async () => {
          started.set(stage, true);
        },
        onStop: async () => {
          stopped.set(stage, true);
        },
      });
    }

    await lifecycle.onStart();
    await lifecycle.onStop();

    expect(started.size).toBe(4);
    expect(stopped.size).toBe(4);
    expect([...started.values()].every((v) => v)).toBe(true);
    expect([...stopped.values()].every((v) => v)).toBe(true);
  });
});
