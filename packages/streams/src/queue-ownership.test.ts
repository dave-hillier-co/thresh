import { describe, expect, it } from "vitest";
import { ownedQueueIndices, queueRingHash, type HashRange } from "@thresh/streams/queue-ownership";

const RING = 0x1_0000_0000;
const WHOLE: HashRange = [0, RING];

describe("queue ownership over the ring", () => {
  it("a silo owning the whole ring owns every queue", () => {
    expect(ownedQueueIndices("default", 8, [WHOLE])).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });

  it("partitions every queue to exactly one of two complementary owners", () => {
    const mid = RING / 2;
    const lower: HashRange = [0, mid];
    const upper: HashRange = [mid, RING];

    const a = ownedQueueIndices("default", 16, [lower]);
    const b = ownedQueueIndices("default", 16, [upper]);

    // Disjoint and complete: together they cover all 16 queues, with no overlap.
    expect([...a, ...b].sort((x, y) => x - y)).toEqual([...Array(16).keys()]);
    expect(a.some((i) => b.includes(i))).toBe(false);
  });

  it("assigns a queue to the range its ring point falls in", () => {
    const h = queueRingHash("default", 3);
    expect(ownedQueueIndices("default", 8, [[h, h + 1]])).toContain(3);
    expect(ownedQueueIndices("default", 8, [[h + 1, h + 2]])).not.toContain(3);
  });

  it("honours a range that wraps the ring", () => {
    const h = queueRingHash("default", 5);
    // Wrap (begin > end) that includes the queue's point: hash >= begin holds.
    expect(ownedQueueIndices("default", 8, [[h, h - 1]])).toContain(5);
    expect(ownedQueueIndices("default", 8, [[h + 1, h]])).not.toContain(5);
  });

  it("isolates ownership by provider name (queues hash distinctly per provider)", () => {
    const h = queueRingHash("default", 0);
    // The same index under another provider name lands at a different point, so a
    // tight range around "default"'s point need not own "other"'s queue 0.
    expect(queueRingHash("other", 0)).not.toBe(h);
  });
});
