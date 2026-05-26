import { newActivationId, type ActivationId } from "@tsva/core/activation-id";
import type { Duration } from "@tsva/core/duration";
import { GrainCallError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import type { GrainContext } from "@tsva/core/grain-context";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { ActivationReason, DeactivationReason } from "@tsva/core/reasons";
import type { InvocationRequest } from "@tsva/core/request";
import { SequenceToken, StreamConsumerInterface, type StreamHandler } from "@tsva/core/stream";
import type { TransactionParticipant } from "@tsva/core/transaction-info";
import { TransactionResourceInterface } from "@tsva/core/transaction-resource";
import { getTransactionalFields } from "@tsva/core/transactional-state-metadata";
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
  /** Handlers for pulling-agent stream delivery, keyed by `namespace/key`. */
  private readonly streamHandlers = new Map<string, StreamHandler<unknown>>();

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
            {
              senderId: req.sender,
              reentrancyId: req.reentrancyId,
              transaction: req.transaction,
            },
            () => this.callMethod(req),
          );
        },
      })
      .finally(() => this.touch());
  }

  /** Bind a pulling-agent stream handler so a delivered `StreamConsumer` turn reaches it. */
  setStreamHandler(streamKey: string, handler: StreamHandler<unknown>): void {
    this.streamHandlers.set(streamKey, handler);
  }

  clearStreamHandler(streamKey: string): void {
    this.streamHandlers.delete(streamKey);
  }

  /** Run a stream delivery callback as a turn; rejects if the activation is gone. */
  runStreamTurn(callback: () => Promise<void>): Promise<unknown> {
    if (this.state === "invalid" || this.state === "deactivating") {
      return Promise.reject(new GrainCallError(`activation unavailable: ${this.id.toString()}`));
    }
    return this.scheduler.schedule({ options: {}, run: callback });
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
    // Stream delivery is a system extension, not a grain method: route it to the
    // handler the grain registered when it subscribed. Already on a turn here.
    if (req.interfaceId === StreamConsumerInterface.id) {
      const [streamKey, event, token] = req.args as [string, unknown, number];
      const handler = this.streamHandlers.get(streamKey);
      if (handler !== undefined) await handler.onNext(event, new SequenceToken(token));
      return undefined;
    }
    // Transaction-resource extension: drive a named transactional state's
    // prepare/commit/abort for the agent on another silo, found by state name.
    if (req.interfaceId === TransactionResourceInterface.id) {
      return await this.invokeTransactionResource(req);
    }
    const fn = (this.instance as unknown as Record<string, unknown>)[req.method];
    if (typeof fn !== "function") {
      throw new GrainCallError(`grain ${this.id.toString()} has no method ${req.method}`);
    }
    return await (fn as (...args: unknown[]) => unknown).apply(this.instance, req.args);
  }

  /** Route a `TransactionResource` system call to the named transactional state. */
  private async invokeTransactionResource(req: InvocationRequest): Promise<unknown> {
    const [stateName, ...rest] = req.args as [string, ...unknown[]];
    const field = getTransactionalFields(this.instance).find((f) => f.stateName === stateName);
    const resource =
      field === undefined
        ? undefined
        : ((this.instance as unknown as Record<string, unknown>)[field.fieldName] as
            | TransactionParticipant
            | undefined);
    if (resource === undefined) {
      throw new GrainCallError(
        `grain ${this.id.toString()} has no transactional state "${stateName}"`,
      );
    }
    const fn = (resource as unknown as Record<string, unknown>)[req.method] as
      | ((...a: unknown[]) => unknown)
      | undefined;
    if (typeof fn !== "function") {
      throw new GrainCallError(`transaction resource has no method ${req.method}`);
    }
    return await fn.apply(resource, rest);
  }
}
