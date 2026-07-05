import { describe, expect, it } from "vitest";
import { AsyncSerialExecutor } from "./async-serial-executor";

describe("AsyncSerialExecutor", () => {
  it("never runs two submitted callbacks concurrently", async () => {
    const executor = new AsyncSerialExecutor<number>();
    let inProgress = 0;
    let maxObserved = 0;

    const operation = async (n: number): Promise<number> => {
      inProgress++;
      maxObserved = Math.max(maxObserved, inProgress);
      await Promise.resolve();
      await new Promise((resolve) => setTimeout(resolve, 0));
      inProgress--;
      return n;
    };

    const tasks = [1, 2, 3, 4, 5].map((n) => executor.addNext(() => operation(n)));
    const results = await Promise.all(tasks);

    expect(results).toEqual([1, 2, 3, 4, 5]);
    expect(maxObserved).toBe(1);
  });

  it("resolves each caller's promise with that callback's own result, in submission order", async () => {
    const executor = new AsyncSerialExecutor<string>();
    const order: string[] = [];

    const task = (label: string, delayMs: number) =>
      executor.addNext(async () => {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        order.push(label);
        return label;
      });

    const [a, b, c] = await Promise.all([task("a", 10), task("b", 0), task("c", 5)]);

    expect([a, b, c]).toEqual(["a", "b", "c"]);
    // Submission order is preserved even though "a" would finish last if run concurrently.
    expect(order).toEqual(["a", "b", "c"]);
  });

  it("surfaces a failing callback's rejection without stalling later callbacks", async () => {
    const executor = new AsyncSerialExecutor<number>();

    const failing = executor.addNext(async () => {
      throw new Error("boom");
    });
    const after = executor.addNext(async () => 42);

    await expect(failing).rejects.toThrow("boom");
    await expect(after).resolves.toBe(42);
  });
});
