/**
 * Local-call semantics benchmark: what each `localCallSemantics` setting costs a same-silo call.
 *
 *   pnpm exec vite-node --config vitest.config.ts scripts/local-call-semantics-bench.ts [--quick]
 *
 * Two tables, one row per value, one column per setting:
 *
 * (a) the copy step alone — what the setting does to ONE value on its way across a call:
 *     "reference" hands it over as is, "copy" runs `copyValue`, "wire" round-trips the default
 *     MessagePack serializer (as `ClusterNode` does under that setting);
 * (b) end to end — sequential same-silo grain calls per second on a one-silo `TestCluster`, for a
 *     method that takes the value and returns it, so the value is carried both ways.
 *
 * Each figure is the median of several timed samples, after a warm-up; each sample's iteration
 * count is calibrated so the sample runs for about `SAMPLE_MS` and is printed beside the figure.
 * Timing uses `process.hrtime.bigint()`, a monotonic clock. Numbers are for comparing the
 * settings with each other on one machine, not absolute.
 */
import { defineGrain } from "../packages/core/src/define-grain";
import { immutable } from "../packages/core/src/immutable";
import { copyValue } from "../packages/core/src/value-copier";
import { MessagePackSerializer } from "../packages/messaging/src/msgpack-serializer";
import type { LocalCallSemantics } from "../packages/runtime/src/local-call-semantics-dispatcher";
import { TestCluster } from "../packages/testing/src/test-cluster";

const QUICK = process.argv.includes("--quick");
const SAMPLE_MS = QUICK ? 50 : 250;
const SAMPLES = QUICK ? 3 : 7;
const WARMUP_MS = QUICK ? 50 : 300;
const SETTINGS: readonly LocalCallSemantics[] = ["reference", "copy", "wire"];

// --- values ---------------------------------------------------------------------------------

function scalars(): unknown[] {
  return ["user-42", 7, true, null, 3.25];
}

/** About 1 KB as JSON: nested arrays, a Map, a Date and some strings. */
function oneKilobyte(): unknown {
  return {
    id: "order-000123",
    customer: { name: "Ada Lovelace", email: "ada@example.com", tier: "gold" },
    placedAt: new Date(1_700_000_000_000),
    lines: Array.from({ length: 8 }, (_, i) => ({
      sku: `SKU-${1000 + i}`,
      qty: i + 1,
      price: 9.99 + i,
      tags: ["fragile", "gift"].slice(0, (i % 2) + 1),
    })),
    matrix: [
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10, 11, 12],
    ],
    totals: new Map<string, number>([
      ["net", 120.5],
      ["tax", 24.1],
      ["gross", 144.6],
      ["discount", 0],
    ]),
    notes: "Leave at the door; ring twice.",
  };
}

function tuples(): unknown {
  return Array.from({ length: 10_000 }, (_, i) => [i, `k${i}`, i % 2 === 0]);
}

function bytes(): unknown {
  const out = new Uint8Array(100 * 1024);
  for (let i = 0; i < out.length; i++) out[i] = (i * 31) & 0xff;
  return out;
}

/** Each value, and the same value marked `immutable` (a fresh instance, so marks never leak). */
const VALUES: ReadonlyArray<readonly [string, () => unknown]> = [
  ["scalar argument list", scalars],
  ["~1 KB object", oneKilobyte],
  ["10,000 small tuples", tuples],
  ["100 KB Uint8Array", bytes],
  ["scalar argument list, immutable", () => immutable(scalars())],
  ["~1 KB object, immutable", () => immutable(oneKilobyte())],
  ["10,000 small tuples, immutable", () => immutable(tuples())],
  ["100 KB Uint8Array, immutable", () => immutable(bytes())],
];

// --- timing ---------------------------------------------------------------------------------

interface Measured {
  /** Operations per second, the median of the samples. */
  readonly perSecond: number;
  /** Iterations in each timed sample. */
  readonly iterations: number;
}

function nowNs(): bigint {
  return process.hrtime.bigint();
}

/** Keeps each result reachable, so the optimizer cannot drop the work that produced it. */
let sink: unknown;

function median(rates: number[]): number {
  rates.sort((a, b) => a - b);
  return rates[Math.floor(rates.length / 2)]!;
}

/** A synchronous step: timed in a plain loop, with no `await` per iteration to distort it. */
function measureSync(op: () => unknown): Measured {
  const deadline = nowNs() + BigInt(WARMUP_MS) * 1_000_000n;
  let warm = 0;
  while (nowNs() < deadline) {
    sink = op();
    warm++;
  }
  const iterations = Math.max(5, Math.round((warm / WARMUP_MS) * SAMPLE_MS));
  const rates: number[] = [];
  for (let s = 0; s < SAMPLES; s++) {
    const start = nowNs();
    for (let i = 0; i < iterations; i++) sink = op();
    rates.push(iterations / (Number(nowNs() - start) / 1e9));
  }
  return { perSecond: median(rates), iterations };
}

/** An asynchronous operation (a grain call), awaited one at a time. */
async function measureAsync(op: () => Promise<unknown>): Promise<Measured> {
  const deadline = nowNs() + BigInt(WARMUP_MS) * 1_000_000n;
  let warm = 0;
  while (nowNs() < deadline) {
    sink = await op();
    warm++;
  }
  const iterations = Math.max(5, Math.round((warm / WARMUP_MS) * SAMPLE_MS));
  const rates: number[] = [];
  for (let s = 0; s < SAMPLES; s++) {
    const start = nowNs();
    for (let i = 0; i < iterations; i++) sink = await op();
    rates.push(iterations / (Number(nowNs() - start) / 1e9));
  }
  return { perSecond: median(rates), iterations };
}

// --- (a) the copy step alone ----------------------------------------------------------------

const serializer = new MessagePackSerializer();
const copySteps: Record<LocalCallSemantics, (value: unknown) => unknown> = {
  reference: (value) => value,
  copy: (value) => copyValue(value),
  wire: (value) => serializer.deserialize(serializer.serialize(value)),
};

// --- (b) end to end -------------------------------------------------------------------------

const Echo = defineGrain("LocalCallSemanticsBenchEcho", () => ({
  echo: async (value: unknown): Promise<unknown> => value,
}));

// --- output ---------------------------------------------------------------------------------

function format(m: Measured): string {
  const rate =
    m.perSecond >= 1e6
      ? `${(m.perSecond / 1e6).toFixed(2)}M/s`
      : m.perSecond >= 1e3
        ? `${(m.perSecond / 1e3).toFixed(1)}k/s`
        : `${m.perSecond.toFixed(0)}/s`;
  const micros = 1e6 / m.perSecond;
  const latency = micros >= 1000 ? `${(micros / 1000).toFixed(2)} ms` : `${micros.toFixed(2)} µs`;
  return `${rate} (${latency}; n=${m.iterations})`;
}

function table(title: string, rows: ReadonlyArray<readonly [string, Measured[]]>): void {
  const header = ["value", ...SETTINGS];
  const cells = rows.map(([label, ms]) => [label, ...ms.map(format)]);
  const widths = header.map((h, i) => Math.max(h.length, ...cells.map((r) => r[i]!.length)));
  const line = (r: readonly string[]): string =>
    `| ${r.map((c, i) => c.padEnd(widths[i]!)).join(" | ")} |`;
  console.log(`\n${title}\n`);
  console.log(line(header));
  console.log(`|${widths.map((w) => "-".repeat(w + 2)).join("|")}|`);
  for (const r of cells) console.log(line(r));
}

async function main(): Promise<void> {
  console.log(
    `local-call semantics benchmark — node ${process.version}; ${SAMPLES} samples of ~${SAMPLE_MS} ms ` +
      `per figure after a ${WARMUP_MS} ms warm-up; median shown; n = iterations per sample.`,
  );

  const steps: Array<readonly [string, Measured[]]> = [];
  for (const [label, make] of VALUES) {
    const value = make();
    const row: Measured[] = [];
    for (const setting of SETTINGS) row.push(measureSync(() => copySteps[setting](value)));
    steps.push([label, row]);
  }
  table("(a) the copy step alone, one value (operations per second)", steps);

  const calls: Array<readonly [string, Measured[]]> = VALUES.map(([label]) => [label, []]);
  for (const setting of SETTINGS) {
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [Echo],
      reminders: false,
      localCallSemantics: setting,
    });
    try {
      const grain = cluster.primary.host.getGrain(Echo, "bench");
      await grain.echo(0);
      for (const [i, [, make]] of VALUES.entries()) {
        const value = make();
        calls[i]![1].push(await measureAsync(() => grain.echo(value)));
      }
    } finally {
      await cluster.dispose();
    }
  }
  table(
    "(b) end to end: sequential same-silo calls of echo(value) on a one-silo TestCluster (calls per second)",
    calls,
  );
}

await main();

void sink;
