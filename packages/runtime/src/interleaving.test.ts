import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Grain } from "@thresh/core/grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import { Silo } from "@thresh/runtime/silo";

/**
 * What an interleaved call may observe (issue #87). The contract, pinned here:
 *
 * - An interleaving call (`alwaysInterleave`, or `readOnly` alongside another
 *   `readOnly` turn) may run DURING another turn's `await` — not only between
 *   turns.
 * - It never runs concurrently with that turn's synchronous code: the code
 *   between two `await`s of a turn runs to completion before any other turn
 *   of the activation starts or resumes (one thread, one event loop), so every
 *   write the suspended turn made before its `await` happens-before the
 *   interleaved call's first read.
 * - It DOES observe whatever the suspended turn had written before its
 *   `await`. Not seeing a write's intermediate state is the grain's design,
 *   not the runtime's: keep the in-progress write out of the fields the
 *   interleaving method reads (a confirmed view, as `JournaledGrain.state` is),
 *   and change those fields only in one synchronous step.
 * - `readOnly` interleaves only when the running blocking turn is also
 *   `readOnly`; `alwaysInterleave` interleaves with anything.
 */
interface IWriter extends GrainKey<string> {
  /** Exclusive. Writes `a` and `b` in one synchronous step, stages, awaits the gate, confirms. */
  write(n: number): Promise<void>;
  /** Exclusive and read-only. */
  readOnlySnapshot(): Promise<Snapshot>;
  /** Read-only, held open on the gate. */
  readOnlyHeld(): Promise<Snapshot>;
  snapshot(): Promise<Snapshot>;
}

interface Snapshot {
  readonly a: number;
  readonly b: number;
  readonly staged: number;
  readonly confirmed: number;
}

const IWriter = defineGrainInterface<IWriter>("IWriter.interleaving", {
  options: {
    snapshot: { alwaysInterleave: true },
    readOnlySnapshot: { readOnly: true },
    readOnlyHeld: { readOnly: true },
  },
});

let events: string[] = [];
let gate: { promise: Promise<void>; open: () => void } = newGate();

function newGate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

@grain()
class WriterGrain extends Grain implements IWriter {
  private a = 0;
  private b = 0;
  private staged = 0;
  private confirmed = 0;

  async write(n: number): Promise<void> {
    events.push(`write(${String(n)}):start`);
    this.a = n;
    this.b = n;
    this.staged = n;
    await gate.promise;
    this.confirmed = this.staged;
    events.push(`write(${String(n)}):end`);
  }
  async readOnlySnapshot(): Promise<Snapshot> {
    events.push("readOnlySnapshot");
    return Promise.resolve(this.view());
  }
  async readOnlyHeld(): Promise<Snapshot> {
    events.push("readOnlyHeld:start");
    await gate.promise;
    events.push("readOnlyHeld:end");
    return this.view();
  }
  async snapshot(): Promise<Snapshot> {
    events.push("snapshot");
    return Promise.resolve(this.view());
  }
  private view(): Snapshot {
    return { a: this.a, b: this.b, staged: this.staged, confirmed: this.confirmed };
  }
}

const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

describe("what an interleaved call may observe (#87)", () => {
  let silo: Silo;

  beforeEach(() => {
    events = [];
    gate = newGate();
    silo = new Silo();
    silo.registerGrain(WriterGrain, { interfaces: [IWriter] });
    silo.start();
  });

  it("an alwaysInterleave call runs during a write's await, seeing every write made before it and none after", async () => {
    const g = silo.getGrain(IWriter, "during");
    const write = g.write(7);
    await tick();

    const seen = await g.snapshot();

    // It ran while the write was suspended, not after it.
    expect(events).toEqual(["write(7):start", "snapshot"]);
    // The write's synchronous step before its await is fully visible (a and b
    // together, never one without the other), and so is its staged value: the
    // runtime does not hide intermediate state. The confirmed field, written
    // only after the await, still holds the old value.
    expect(seen).toEqual({ a: 7, b: 7, staged: 7, confirmed: 0 });

    gate.open();
    await write;
    await expect(g.snapshot()).resolves.toEqual({ a: 7, b: 7, staged: 7, confirmed: 7 });
  });

  it("writes without an interleave option never interleave each other", async () => {
    const g = silo.getGrain(IWriter, "writes");
    const first = g.write(1);
    const second = g.write(2);
    await tick();
    expect(events).toEqual(["write(1):start"]);

    gate.open();
    await Promise.all([first, second]);
    expect(events).toEqual(["write(1):start", "write(1):end", "write(2):start", "write(2):end"]);
  });

  it("a readOnly call does not interleave with a non-readOnly write", async () => {
    const g = silo.getGrain(IWriter, "readonly-vs-write");
    const write = g.write(3);
    await tick();
    const read = g.readOnlySnapshot();
    await tick();
    expect(events).toEqual(["write(3):start"]);

    gate.open();
    await write;
    // Queued behind the write, so it sees the finished write.
    await expect(read).resolves.toEqual({ a: 3, b: 3, staged: 3, confirmed: 3 });
    expect(events).toEqual(["write(3):start", "write(3):end", "readOnlySnapshot"]);
  });

  it("a readOnly call interleaves with another readOnly turn", async () => {
    const g = silo.getGrain(IWriter, "readonly-vs-readonly");
    const held = g.readOnlyHeld();
    await tick();
    await g.readOnlySnapshot();
    expect(events).toEqual(["readOnlyHeld:start", "readOnlySnapshot"]);

    gate.open();
    await held;
  });

  it("an alwaysInterleave call interleaves with a readOnly turn too", async () => {
    const g = silo.getGrain(IWriter, "always-vs-readonly");
    const held = g.readOnlyHeld();
    await tick();
    await g.snapshot();
    expect(events).toEqual(["readOnlyHeld:start", "snapshot"]);

    gate.open();
    await held;
  });
});
