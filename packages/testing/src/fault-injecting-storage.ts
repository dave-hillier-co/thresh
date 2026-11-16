import type { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage, StateHolder } from "@thresh/core/grain-storage";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";

/** The failure the fault-injecting stores raise unless a fault supplies its own error. */
export class InjectedStorageFault extends Error {
  constructor(
    readonly operation: string,
    readonly target: StorageFaultTarget,
  ) {
    super(
      `injected ${operation} failure for ${target.name} of ${target.grainId.toString()} (fault injection)`,
    );
    this.name = "InjectedStorageFault";
  }
}

/** What an operation addresses: the grain, and its state name or log name. */
export interface StorageFaultTarget {
  readonly grainId: GrainId;
  readonly name: string;
}

/** Which operations a fault applies to, and how many times. */
export interface StorageFaultOptions {
  /** Only operations on a target this accepts; every target when left out. */
  readonly when?: (target: StorageFaultTarget) => boolean;
  /** The next `count` matching operations only; every one until healed when left out. */
  readonly count?: number;
}

/** How long a delayed operation waits before it runs: until a gate opens, or a duration. */
export type StorageDelay = { readonly until: Promise<unknown> } | { readonly ms: number };

/** A fault that is in force until healed. */
export interface StorageFaultHandle {
  heal(): void;
}

type Effect =
  | { readonly kind: "fail"; readonly error: ((target: StorageFaultTarget) => unknown) | undefined }
  | { readonly kind: "delay"; readonly delay: StorageDelay }
  | { readonly kind: "lose" };

interface Rule<Op extends string> {
  readonly operations: ReadonlySet<Op>;
  readonly when: ((target: StorageFaultTarget) => boolean) | undefined;
  remaining: number | undefined;
  readonly effect: Effect;
}

/**
 * The fault table both decorators share. A test arms faults by direct call; each fault applies to
 * the operations and targets it names, a set number of times or until healed. For one operation
 * the first matching fault decides, in the order they were armed. Nothing is random.
 */
abstract class StorageFaultInjector<Op extends string> {
  private readonly rules: Rule<Op>[] = [];
  private halted = false;
  private injectedCount = 0;

  protected constructor(private readonly time: TimeProvider) {}

  /** Operations a fault has fired on so far. */
  get injected(): number {
    return this.injectedCount;
  }

  /** Fail matching operations with `options.error()`, or an {@link InjectedStorageFault}. */
  fail(
    operation: Op | readonly Op[],
    options: StorageFaultOptions & {
      readonly error?: (target: StorageFaultTarget) => unknown;
    } = {},
  ): StorageFaultHandle {
    return this.arm(operation, options, { kind: "fail", error: options.error });
  }

  /** Hold matching operations until the gate opens or the duration passes, then run them. */
  delay(
    operation: Op | readonly Op[],
    options: StorageFaultOptions & StorageDelay,
  ): StorageFaultHandle {
    return this.arm(operation, options, { kind: "delay", delay: options });
  }

  /** Make every operation hang for good: what a store looks like to a process that has died. */
  halt(): void {
    this.halted = true;
  }

  /** Remove every fault (a halt stays). */
  healAll(): void {
    this.rules.length = 0;
  }

  /** Add a fault for `operation` with the given effect. */
  protected arm(
    operation: Op | readonly Op[],
    options: StorageFaultOptions,
    effect: Effect,
  ): StorageFaultHandle {
    const rule: Rule<Op> = {
      operations: new Set(typeof operation === "string" ? [operation] : operation),
      when: options.when,
      remaining: options.count,
      effect,
    };
    this.rules.push(rule);
    return { heal: () => this.remove(rule) };
  }

  /** Run `operation` against `target` through the fault table. `lost` is what a lost one returns. */
  protected async apply<T>(
    operation: Op,
    target: StorageFaultTarget,
    run: () => Promise<T>,
    lost?: () => T,
  ): Promise<T> {
    if (this.halted) return new Promise<T>(() => undefined);
    const rule = this.rules.find(
      (r) => r.operations.has(operation) && (r.when === undefined || r.when(target)),
    );
    if (rule === undefined) return run();
    if (rule.remaining !== undefined) {
      rule.remaining -= 1;
      if (rule.remaining <= 0) this.remove(rule);
    }
    this.injectedCount += 1;
    const effect = rule.effect;
    switch (effect.kind) {
      case "fail":
        throw effect.error !== undefined
          ? effect.error(target)
          : new InjectedStorageFault(operation, target);
      case "delay":
        if ("until" in effect.delay) await effect.delay.until;
        else await this.sleep(effect.delay.ms);
        if (this.halted) return new Promise<T>(() => undefined);
        return run();
      case "lose":
        if (lost === undefined) throw new Error(`a ${operation} cannot be lost`);
        return lost();
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => this.time.setTimer(resolve, ms));
  }

  private remove(rule: Rule<Op>): void {
    const i = this.rules.indexOf(rule);
    if (i >= 0) this.rules.splice(i, 1);
  }
}

export type GrainStorageOperation = "read" | "write" | "clear";

/**
 * A `GrainStorage` decorator that fails, delays or loses the operations a test tells it to and
 * passes everything else through to the store it wraps.
 */
export class FaultInjectingGrainStorage
  extends StorageFaultInjector<GrainStorageOperation>
  implements GrainStorage
{
  constructor(
    private readonly inner: GrainStorage,
    options: { readonly time?: TimeProvider } = {},
  ) {
    super(options.time ?? systemTimeProvider);
  }

  /** Report a write or clear as done without performing it: the store keeps what it had. */
  lose(
    operation: "write" | "clear" | ReadonlyArray<"write" | "clear">,
    options: StorageFaultOptions = {},
  ): StorageFaultHandle {
    return this.arm(operation, options, { kind: "lose" });
  }

  read<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.apply("read", { grainId, name: stateName }, () =>
      this.inner.read(stateName, grainId, state, signal),
    );
  }

  write<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.apply(
      "write",
      { grainId, name: stateName },
      () => this.inner.write(stateName, grainId, state, signal),
      () => undefined,
    );
  }

  clear<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.apply(
      "clear",
      { grainId, name: stateName },
      () => this.inner.clear(stateName, grainId, state, signal),
      () => undefined,
    );
  }
}

export type JournalStorageOperation = "read" | "append" | "replace" | "clear";

/**
 * A `JournalStorage` decorator that fails or delays the operations a test tells it to and passes
 * everything else through. It carries the wrapped store's `backingKey`, so the runtime still
 * recognises it as the same log store as the one it wraps (and as any other decorator over it).
 */
export class FaultInjectingJournalStorage
  extends StorageFaultInjector<JournalStorageOperation>
  implements JournalStorage
{
  declare readonly backingKey?: string;

  constructor(
    private readonly inner: JournalStorage,
    options: { readonly time?: TimeProvider } = {},
  ) {
    super(options.time ?? systemTimeProvider);
    if (inner.backingKey !== undefined) {
      Object.defineProperty(this, "backingKey", { value: inner.backingKey, enumerable: true });
    }
  }

  read(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<JournalSegment> {
    return this.apply("read", { grainId, name: logName }, () =>
      this.inner.read(logName, grainId, signal),
    );
  }

  append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    return this.apply("append", { grainId, name: logName }, () =>
      this.inner.append(logName, grainId, entries, expectedVersion, signal),
    );
  }

  replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    return this.apply("replace", { grainId, name: logName }, () =>
      this.inner.replace(logName, grainId, entries, expectedVersion, signal),
    );
  }

  clear(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<void> {
    return this.apply("clear", { grainId, name: logName }, () =>
      this.inner.clear(logName, grainId, signal),
    );
  }
}
