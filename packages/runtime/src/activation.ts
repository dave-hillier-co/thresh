import { newActivationId, type ActivationId } from "@tsva/core/activation-id";
import type { Duration } from "@tsva/core/duration";
import { GrainCallError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import type { GrainContext } from "@tsva/core/grain-context";
import type { GrainId } from "@tsva/core/grain-id";
import { getGrainInterface } from "@tsva/core/grain-interface";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { ActivationReason, DeactivationReason } from "@tsva/core/reasons";
import type { Remindable, TickStatus } from "@tsva/core/reminder";
import type { InvocationRequest } from "@tsva/core/request";
import { GrainTimerImpl } from "@tsva/runtime/grain-timer-impl";
import { invocationContext } from "@tsva/runtime/invocation-context";
import type { TimeProvider } from "@tsva/runtime/time-provider";
import { TurnScheduler } from "@tsva/runtime/turn-scheduler";

export type ActivationState = "creating" | "activating" | "valid" | "deactivating" | "invalid";

/**
 * The runtime's per-grain bookkeeping object and the grain's `GrainContext`.
 * It owns the turn scheduler and drives the activation lifecycle.
 */
export class ActivationData implements GrainContext {
  readonly id: GrainId;
  readonly activationId: ActivationId;
  readonly scheduler: TurnScheduler;

  instance!: Grain;
  runtime!: GrainRuntime;
  state: ActivationState = "creating";

  /** Runs once before `onActivate` (e.g. read persistent state); set by the catalog. */
  preActivate: (() => Promise<void>) | undefined;

  private lastActiveMs: number;
  private keepAliveUntilMs = 0;
  private deactivateRequested = false;
  private readonly timers = new Set<GrainTimerImpl>();

  constructor(
    id: GrainId,
    private readonly time: TimeProvider,
    private readonly collectionAgeMs: number,
    reentrant: boolean,
    activationId: ActivationId = newActivationId(),
  ) {
    this.id = id;
    this.activationId = activationId;
    this.scheduler = new TurnScheduler({ reentrant });
    this.lastActiveMs = time.now();
  }

  /** Schedule `onActivate` as the first turn, so it precedes any message. */
  beginActivate(reason: ActivationReason): void {
    this.state = "activating";
    void this.scheduler
      .schedule({
        options: {},
        reentrancyId: this.activationId,
        run: async () => {
          if (this.preActivate !== undefined) await this.preActivate();
          await this.instance.onActivate(reason);
          this.state = "valid";
        },
      })
      .catch(() => {
        this.state = "invalid";
      });
  }

  invoke(req: InvocationRequest): Promise<unknown> {
    this.touch();
    return this.scheduler
      .schedule({
        options: req.options,
        reentrancyId: req.reentrancyId,
        run: () => {
          if (this.state === "invalid" || this.state === "deactivating") {
            throw new GrainCallError(`activation unavailable: ${this.id.toString()}`);
          }
          return invocationContext.run(
            { senderId: req.sender, reentrancyId: req.reentrancyId },
            () => this.callMethod(req),
          );
        },
      })
      .finally(() => this.touch());
  }

  /** Deliver a reminder tick to the grain as a turn (if it implements Remindable). */
  deliverReminder(name: string, status: TickStatus): Promise<unknown> {
    this.touch();
    return this.scheduler.schedule({
      options: {},
      run: async () => {
        const remindable = this.instance as Partial<Remindable>;
        if (typeof remindable.receiveReminder === "function") {
          await remindable.receiveReminder(name, status);
        }
      },
    });
  }

  /** Register a non-durable timer that fires as a turn; cancelled on deactivation. */
  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer {
    const timer = new GrainTimerImpl(
      this.time,
      (cb) => this.scheduler.schedule({ options: {}, run: cb }),
      callback,
      due,
      period,
    );
    this.timers.add(timer);
    return timer;
  }

  async deactivate(reason: DeactivationReason): Promise<void> {
    if (this.state === "invalid" || this.state === "deactivating") return;
    this.state = "deactivating";
    for (const timer of this.timers) timer.dispose();
    this.timers.clear();
    await this.scheduler
      .schedule({ options: {}, run: () => this.instance.onDeactivate(reason) })
      .catch(() => undefined);
    this.state = "invalid";
  }

  requestDeactivation(): void {
    this.deactivateRequested = true;
  }

  delayDeactivation(byMs: number): void {
    this.keepAliveUntilMs = Math.max(this.keepAliveUntilMs, this.time.now() + byMs);
  }

  isStale(): boolean {
    if (this.state !== "valid") return false;
    if (this.scheduler.busy) return false;
    const now = this.time.now();
    if (now < this.keepAliveUntilMs) return false;
    if (this.deactivateRequested) return true;
    return now - this.lastActiveMs >= this.collectionAgeMs;
  }

  private touch(): void {
    this.lastActiveMs = this.time.now();
  }

  private async callMethod(req: InvocationRequest): Promise<unknown> {
    const iface = getGrainInterface(req.interfaceId);
    if (iface === undefined) throw new GrainCallError(`unknown interface ${req.interfaceId}`);
    const methodName = iface.methods[req.methodId];
    if (methodName === undefined) throw new GrainCallError(`unknown method ${req.methodId}`);
    const fn = (this.instance as unknown as Record<string, unknown>)[methodName];
    if (typeof fn !== "function") {
      throw new GrainCallError(`grain ${this.id.toString()} has no method ${methodName}`);
    }
    return await (fn as (...args: unknown[]) => unknown).apply(this.instance, req.args);
  }
}
