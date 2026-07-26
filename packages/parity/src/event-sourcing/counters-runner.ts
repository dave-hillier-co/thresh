// Shared by CountersGrainTests and CountersGrainTests.Perf (both ported from
// dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/CountersGrainTests*.cs
// @ v10.1.0, which share this helper as `ConcurrentIncrementsRunner`).
import { expect } from "vitest";
import type { ICountersGrain } from "@thresh/parity/grains/impl/counters-grain";

const keys = ["a", "b", "c", "d", "e", "f", "g", "h"];

function randomKey(): string {
  return keys[Math.floor(Math.random() * keys.length)]!;
}

export async function concurrentIncrementsRunner(
  grain: ICountersGrain,
  count: number,
  waitForConfirmationOnEach: boolean,
): Promise<void> {
  // increment `count` times, on random keys, concurrently
  const tasks: Promise<void>[] = [];
  for (let i = 0; i < count; i++) tasks.push(grain.add(randomKey(), 1, waitForConfirmationOnEach));
  await Promise.all(tasks);

  // check that the tentative state shows all increments
  const tentative = await grain.getTentativeState();
  expect(Object.values(tentative).reduce((c, v) => c + v, 0)).toBe(count);

  // if we did not wait for confirmation on each event, wait now
  if (!waitForConfirmationOnEach) await grain.confirmAllPreviouslyRaisedEvents();

  // check that the confirmed state shows all the increments
  const confirmed = await grain.getConfirmedState();
  expect(Object.values(confirmed).reduce((c, v) => c + v, 0)).toBe(count);

  // reset all counters
  await grain.reset(true);
}
