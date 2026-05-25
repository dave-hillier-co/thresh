import { stableHash32 } from "@tsva/core/hash";

/** A half-open hash range `[begin, end)` a silo owns on the ring. */
export type HashRange = readonly [begin: number, end: number];

function inRanges(hash: number, ranges: readonly HashRange[]): boolean {
  return ranges.some(([begin, end]) =>
    begin <= end ? hash >= begin && hash < end : hash >= begin || hash < end,
  );
}

/**
 * The ring point of physical queue `index`. Each queue sits at a fixed point on
 * the same consistent-hash ring the directory and reminders use, so every silo
 * computing ownership from the same membership view agrees on a single owner per
 * queue — and a join/leave only moves the queues whose point changes hands.
 */
export function queueRingHash(providerName: string, index: number): number {
  return stableHash32(`${providerName}:queue:${index}`);
}

/** The queue indices whose ring point falls in one of the silo's owned ranges. */
export function ownedQueueIndices(
  providerName: string,
  queueCount: number,
  ranges: readonly HashRange[],
): number[] {
  const owned: number[] = [];
  for (let i = 0; i < queueCount; i++) {
    if (inRanges(queueRingHash(providerName, i), ranges)) owned.push(i);
  }
  return owned;
}
