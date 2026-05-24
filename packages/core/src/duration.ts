/** A span of time, expressed in whichever units read most clearly. */
export interface Duration {
  ms?: number;
  seconds?: number;
  minutes?: number;
  hours?: number;
  days?: number;
}

export function durationToMs(d: Duration): number {
  return (
    (d.ms ?? 0) +
    (d.seconds ?? 0) * 1_000 +
    (d.minutes ?? 0) * 60_000 +
    (d.hours ?? 0) * 3_600_000 +
    (d.days ?? 0) * 86_400_000
  );
}
