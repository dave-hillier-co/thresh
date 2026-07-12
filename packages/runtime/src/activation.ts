import { newActivationId, type ActivationId } from "@tsva/core/activation-id";
import {
  broadcastChannelObserver,
  BroadcastConsumerInterface,
  type BroadcastChannelHandler,
} from "@tsva/core/broadcast-channel";
import type { Duration } from "@tsva/core/duration";
import {
  DurableJobConsumerInterface,
  durableJobHandler,
  type JobRunContext,
} from "@tsva/core/durable-job";
import {
  GrainCallError,
  GrainExtensionNotInstalledException,
  RejectionError,
} from "@tsva/core/errors";
import {
  CancellationTokenPlaceholder,
  GrainCancellationToken,
} from "@tsva/core/grain-cancellation-token";
import type { Grain } from "@tsva/core/grain";
import type { GrainContext } from "@tsva/core/grain-context";
import type { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata, type GrainConstructor } from "@tsva/core/grain-metadata";
import { MigrationBag } from "@tsva/core/grain-migration-participant";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer, TimerOptions } from "@tsva/core/grain-timer";
import type { InvokeMethodOptions } from "@tsva/core/invoke-options";
import {
  formatDeactivationReason,
  type ActivationReason,
  type DeactivationReason,
} from "@tsva/core/reasons";
import type { SiloAddress } from "@tsva/core/silo-address";
import { migrationParticipantsOf } from "@tsva/runtime/migration-participants";
import { getGrainInterface } from "@tsva/core/grain-interface";
import type { CancellationSourcesExtension } from "@tsva/runtime/cancellation-extension";
import {
  cancellationExtensionFactory,
  ICancellationSourcesExtension,
} from "@tsva/runtime/cancellation-extension";
import {
  grainIncomingFilter,
  runCallFilters,
  type IncomingGrainCallContext,
  type IncomingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import type { InvocationRequest } from "@tsva/core/request";
import {
  PLACEMENT_HINT_KEY,
  RequestContext,
  requestContextStore,
  runWithRequestContext,
} from "@tsva/core/request-context";
import {
  implicitStreamObserver,
  SequenceToken,
  StreamConsumerInterface,
  type StreamHandler,
} from "@tsva/core/stream";
import type { TransactionParticipant } from "@tsva/core/transaction-info";
import { TransactionResourceInterface } from "@tsva/core/transaction-resource";
import { getTransactionalFields } from "@tsva/core/transactional-state-metadata";
import { GrainTimerImpl } from "@tsva/runtime/grain-timer-impl";
import { invocationContext } from "@tsva/runtime/invocation-context";
import type { TimeProvider } from "@tsva/runtime/time-provider";
import { TurnScheduler } from "@tsva/runtime/turn-scheduler";
import { withOnDeactivateSpan } from "@tsva/observability/activation-tracing";

export type ActivationState = "creating" | "activating" | "valid" | "deactivating" | "invalid";

/** Split a `namespace/key` string; with no slash the whole string is the namespace and key is empty. */
function parseNamespaceKey(key: string): [namespace: string, key: string] {
  const slash = key.indexOf("/");
  return slash < 0 ? [key, ""] : [key.slice(0, slash), key.slice(slash + 1)];
}

/**
 * The runtime's per-grain bookkeeping object and the grain's `GrainContext`.
 * It owns the turn scheduler and drives the activation lifecycle.
 */
export class ActivationData implements GrainContext {
  readonly id: GrainId;
  readonly activationId: ActivationId;
  readonly scheduler: TurnScheduler;

  private _instance!: Grain;
  runtime!: GrainRuntime;
  state: ActivationState = "creating";

  get instance(): Grain {
    return this._instance;
  }

  /**
   * Binds the grain instance and, in the same step, resolves its
   * `mayInterleave` metadata (set by `@mayInterleave()`, absent by default)
   * onto the scheduler. Metadata is only known once the instance exists —
   * after the scheduler itself was constructed — so this late-binds it rather
   * than threading it through the constructor like `reentrant`.
   */
  set instance(value: Grain) {
    this._instance = value;
    this.scheduler.setMayInterleave(
      getGrainMetadata(value.constructor as GrainConstructor)?.mayInterleave,
    );
  }

  /**
   * The error thrown from pre-activation/`onActivate` when activation failed,
   * captured so the first call that cannot proceed surfaces the ORIGINAL
   * application error (Orleans parity) rather than a generic one. Undefined
   * unless `onActivate` (or `preActivate`) threw.
   */
  private activationFailure: unknown;
  private didFailActivation = false;

  /** True when this activation failed to activate (its `onActivate` threw). */
  get activationFailed(): boolean {
    return this.didFailActivation;
  }

  /** Runs once before `onActivate` (e.g. read persistent state); set by the catalog. */
  preActivate: (() => Promise<void>) | undefined;

  /** Incoming grain-call filters wrapping each grain-method dispatch; set by the catalog. */
  incomingCallFilters: readonly IncomingGrainCallFilter[] = [];

  /** When migrating: the directed target silo (undefined lets placement choose). */
  migrationTarget: SiloAddress | undefined;
  /**
   * When migrating with no explicit `migrationTarget`: the caller's ambient
   * `RequestContext` placement hint (Orleans `IPlacementDirector.PlacementHintKey`),
   * captured at `requestMigration` time since it must be read while the
   * triggering call's `RequestContext` is still in scope — by the time the
   * idle sweep actually migrates, that scope is long gone. Resolved against
   * live candidates at migration time (see `ClusterNode.migrateActivation`).
   */
  migrationHint: string | undefined;
  /** State captured on the source silo to restore on the target; set by the catalog. */
  rehydrationBag: Record<string, unknown> | undefined;

  private lastActiveMs: number;
  private keepAliveUntilMs = 0;
  private deactivateRequested = false;
  /**
   * True when `deactivateOnIdle()` was called while this activation was still
   * `"activating"` (from within `onActivate`). Orleans rejects the triggering
   * (and any in-flight) call rather than serving it from an activation that
   * asked to go away before it ever became valid, so this activation never
   * transitions to `"valid"` — see `beginActivate`.
   */
  private deactivateRequestedDuringActivation = false;
  private migrationRequested = false;
  private dehydrated = false;
  private readonly timers = new Set<GrainTimerImpl>();
  /** Handlers for pulling-agent stream delivery, keyed by `namespace/key`. */
  private readonly streamHandlers = new Map<string, StreamHandler<unknown>>();
  private readonly broadcastHandlers = new Map<string, BroadcastChannelHandler<unknown>>();
  /** Bound `GrainExtension` instances, keyed by interface id; see `getOrSetExtension`. */
  private readonly extensions = new Map<number, object>();
  /**
   * Auto-install factories for extension interfaces not yet bound, keyed by
   * interface id — set by the catalog (later task); unset here means an
   * un-bound extension call always throws
   * `GrainExtensionNotInstalledException` rather than auto-installing.
   */
  private extensionFactories?: Map<number, (activation: ActivationData) => object>;

  constructor(
    id: GrainId,
    private readonly time: TimeProvider,
    private readonly collectionAgeMs: number,
    reentrant: boolean,
    activationId: ActivationId = newActivationId(),
  ) {
    this.id = id;
    this.activationId = activationId;
    // Even a fully reentrant grain must finish activating (running state
    // binding, then `onActivate`) before any request is dispatched — Orleans
    // never interleaves a request with `OnActivateAsync`.
    this.scheduler = new TurnScheduler({ reentrant, barrierFirstTurn: true });
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
          try {
            if (this.preActivate !== undefined) await this.preActivate();
            await this.instance.onActivate(reason);
            if (this.deactivateRequestedDuringActivation) {
              // The grain asked to deactivate from onActivate: never go valid.
              // Run the deactivate hook inline (NOT via runDeactivateHook/the
              // scheduler — this callback is itself still running as the
              // activation turn, so scheduling another turn here would
              // deadlock) so the next queued turn (the call that triggered
              // this activation) observes "invalid" and is rejected rather
              // than served (Orleans parity: "Forwarding failed").
              this.state = "deactivating";
              for (const timer of this.timers) timer.dispose();
              this.timers.clear();
              await this.instance
                .onDeactivate({
                  code: "application-requested",
                  description: "deactivateOnIdle requested during activation",
                })
                .catch(() => undefined);
              this.state = "invalid";
              return;
            }
            this.state = "valid";
          } catch (err) {
            // Activation failed: mark invalid and capture the error (set
            // synchronously here, before this turn settles and the next queued
            // call runs) so the first failing call can surface the original
            // application error instead of a generic "activation unavailable".
            this.state = "invalid";
            this.activationFailure = err;
            this.didFailActivation = true;
            throw err;
          }
        },
      })
      .catch(() => undefined);
  }

  invoke(req: InvocationRequest): Promise<unknown> {
    this.touch();
    return this.scheduler
      .schedule({
        options: req.options,
        reentrancyId: req.reentrancyId,
        method: req.method,
        args: req.args,
        run: () => {
          if (this.state === "invalid" || this.state === "deactivating") {
            // A failed activation surfaces the original activation error to the
            // caller; the ordinary deactivating/idle-invalid cases have none, so
            // they fall back to the generic unavailable error.
            if (this.didFailActivation) throw this.activationFailure;
            throw new GrainCallError(`activation unavailable: ${this.id.toString()}`);
          }
          // Dehydrated for migration: the directory now points at the new host, so
          // reject as stale and let the caller re-resolve there.
          if (this.dehydrated) {
            throw new RejectionError(`activation migrated: ${this.id.toString()}`, "noActivation");
          }
          // A fresh, mutable copy of the incoming headers so `requestContext.set`
          // during the turn does not mutate the caller's bag but does flow to
          // downstream calls. Scoped via `runWithRequestContext` (the SAME
          // ambient store a non-grain/client caller uses), nested inside
          // `invocationContext.run` so both are in scope for the turn.
          return runWithRequestContext(req.headers !== undefined ? { ...req.headers } : {}, () =>
            invocationContext.run(
              {
                senderId: req.sender,
                ownerId: this.id,
                reentrancyId: req.reentrancyId,
                transaction: req.transaction,
              },
              () => this.callMethod(req),
            ),
          );
        },
      })
      .finally(() => this.touch());
  }

  /**
   * The handler for a delivered stream event, keyed by `namespace/key`. Falls
   * back to the grain's implicit-subscription observer (and caches its result)
   * when no handler was registered via `subscribe` — so an implicitly subscribed
   * grain (Orleans' `[ImplicitStreamSubscription]`) receives events without ever
   * calling `subscribe`. The fan-out only routes a stream to a grain that is an
   * explicit or declared-implicit subscriber, so an absent observer means drop.
   */
  private streamHandlerFor(streamKey: string): StreamHandler<unknown> | undefined {
    const existing = this.streamHandlers.get(streamKey);
    if (existing !== undefined) return existing;
    const observe = implicitStreamObserver(this.instance);
    if (observe === undefined) return undefined;
    const [namespace, key] = parseNamespaceKey(streamKey);
    const handler = observe(namespace, key);
    // A decline (`undefined`) is not cached: `probeStreamSubscription` needs
    // `observe` re-invoked so it can tell "declined" apart from "no observer
    // at all" (see its doc comment), and a declined admin subscription is
    // removed by its caller anyway, so there is nothing to cache toward.
    if (handler !== undefined) this.streamHandlers.set(streamKey, handler);
    return handler;
  }

  /**
   * Resolve (and cache) this activation's `STREAM_SUBSCRIPTION_OBSERVER` for
   * `streamKey`, without delivering anything — the confirmation step of an
   * administrative subscription (Orleans `IStreamSubscriptionObserver.OnSubscribed`).
   * `true` when the grain accepts (or never declared an observer at all, so
   * there is nothing to decline); `false` when it declared one and explicitly
   * declined (returned `undefined`) — the caller then drops the subscription.
   */
  private probeStreamSubscription(streamKey: string): boolean {
    if (implicitStreamObserver(this.instance) === undefined) return true;
    return this.streamHandlerFor(streamKey) !== undefined;
  }

  /**
   * The handler for a delivered broadcast-channel item, keyed by `namespace/key`,
   * resolved (and cached) from the grain's `BROADCAST_CHANNEL_OBSERVER` member.
   * An absent observer means the grain declared the implicit subscription but
   * exposes no handler, so the item is dropped — mirroring `streamHandlerFor`.
   */
  private broadcastHandlerFor(channelKey: string): BroadcastChannelHandler<unknown> | undefined {
    const existing = this.broadcastHandlers.get(channelKey);
    if (existing !== undefined) return existing;
    const observe = broadcastChannelObserver(this.instance);
    if (observe === undefined) return undefined;
    const [namespace, key] = parseNamespaceKey(channelKey);
    const handler = observe(namespace, key);
    this.broadcastHandlers.set(channelKey, handler);
    return handler;
  }

  /**
   * Get this activation's bound `GrainExtension` instance for `interfaceId`,
   * lazily creating it via `factory` on first use. Idempotent: a second call
   * for the same interface id returns the SAME instance and never re-runs
   * `factory` (see `GrainRuntime.getOrSetExtension`).
   */
  getOrSetExtension<T extends object>(interfaceId: number, factory: () => T): T {
    const existing = this.extensions.get(interfaceId);
    if (existing !== undefined) return existing as T;
    const created = factory();
    this.extensions.set(interfaceId, created);
    return created;
  }

  /**
   * Set the catalog's per-interface auto-install factories, used by
   * `callMethod` to bind an extension on first call rather than requiring the
   * grain to have called `getOrSetExtension` beforehand. Unset by default
   * (wired by a later silo-builder task); an un-bound extension call with no
   * factory configured throws `GrainExtensionNotInstalledException`.
   */
  setExtensionFactories(factories: Map<number, (activation: ActivationData) => object>): void {
    this.extensionFactories = factories;
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
  registerTimer(
    callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
    options?: TimerOptions,
  ): GrainTimer {
    // An interleaving timer's turns carry `alwaysInterleave` so the callback can
    // run while a non-reentrant call awaits it (Orleans' Interleave option);
    // otherwise the turn is exclusive like any grain call.
    const turnOptions: InvokeMethodOptions = options?.interleave ? { alwaysInterleave: true } : {};
    const timer = new GrainTimerImpl(
      this.time,
      (cb) => this.scheduler.schedule({ options: turnOptions, run: cb }),
      callback,
      due,
      period,
    );
    this.timers.add(timer);
    return timer;
  }

  async deactivate(reason: DeactivationReason): Promise<void> {
    await this.runDeactivateHook(reason);
    this.finalizeDeactivation();
  }

  /**
   * Run the `onDeactivate` hook only, leaving the activation in the
   * "deactivating" state so the caller can inspect `wantsMigration` (which
   * the hook may have just set, e.g. via `migrateOnIdle` called from
   * `onDeactivate`) before finalizing. No-op if already deactivating/invalid.
   */
  async runDeactivateHook(reason: DeactivationReason): Promise<void> {
    if (this.state === "invalid" || this.state === "deactivating") return;
    this.state = "deactivating";
    for (const timer of this.timers) timer.dispose();
    this.timers.clear();
    await this.scheduler
      .schedule({
        options: {},
        run: () =>
          withOnDeactivateSpan(
            {
              grainId: this.id.toString(),
              grainType: this.id.type,
              siloId: this.localSiloId(),
              activationId: this.activationId,
              reason: formatDeactivationReason(reason),
            },
            () => this.instance.onDeactivate(reason),
          ),
      })
      .catch(() => undefined);
  }

  /**
   * The local silo's address, for the `OnDeactivate` span's `orleans.silo.id`
   * tag — best-effort: some lightweight test harnesses build an
   * `ActivationData` with no local-silo address configured on its runtime
   * (`GrainRuntimeImpl.localSiloAddress` throws in that case), and the
   * deactivate hook must still run rather than being swallowed by
   * `runDeactivateHook`'s outer `.catch`.
   */
  private localSiloId(): string {
    try {
      return this.runtime.localSiloAddress().toString();
    } catch {
      return "unknown";
    }
  }

  /** Complete a deactivation whose hook already ran via `runDeactivateHook`. */
  finalizeDeactivation(): void {
    this.state = "invalid";
  }

  requestDeactivation(): void {
    this.deactivateRequested = true;
    // Called during onActivate (still "activating"): don't defer to the next
    // idle sweep — the activation must never become servable (see
    // `beginActivate`). Called during a regular turn ("valid"): unchanged,
    // deferred flag picked up by the next `collectIdle` sweep.
    if (this.state === "activating") this.deactivateRequestedDuringActivation = true;
  }

  delayDeactivation(byMs: number): void {
    this.keepAliveUntilMs = Math.max(this.keepAliveUntilMs, this.time.now() + byMs);
  }

  /** Mark this activation to migrate (rather than deactivate) when next idle. */
  requestMigration(target?: SiloAddress): void {
    this.migrationRequested = true;
    this.migrationTarget = target;
    // No explicit target: capture the caller's ambient placement hint now,
    // while it's still in scope — the idle sweep that actually migrates runs
    // well after this call's RequestContext scope has ended.
    this.migrationHint = target === undefined ? RequestContext.get(PLACEMENT_HINT_KEY) : undefined;
  }

  get wantsMigration(): boolean {
    return this.migrationRequested;
  }

  /**
   * Gather the activation's in-memory state into a transport-safe bag by running
   * each migration participant's `onDehydrate` on a turn. Marks the activation
   * dehydrated so no further calls run here — they re-resolve to the new host.
   */
  async dehydrate(): Promise<Record<string, unknown>> {
    const bag = new MigrationBag();
    await this.scheduler.schedule({
      options: {},
      reentrancyId: this.activationId,
      run: () => {
        for (const participant of migrationParticipantsOf(this.instance)) {
          participant.onDehydrate(bag);
        }
        this.dehydrated = true;
        return Promise.resolve();
      },
    });
    return bag.data;
  }

  /** Restore migrated state into the (freshly bound) participants on the target. */
  applyRehydration(): void {
    if (this.rehydrationBag === undefined) return;
    const ctx = new MigrationBag(this.rehydrationBag);
    for (const participant of migrationParticipantsOf(this.instance)) {
      participant.onRehydrate(ctx);
    }
  }

  /**
   * `ageLimitOverrideMs`, when given, replaces `collectionAgeMs` for this
   * check — the management grain's `ForceActivationCollection(ageLimit)`
   * (Orleans parity) sweeps with a caller-chosen age (e.g. zero, to collect
   * every currently-idle activation) rather than each activation's own
   * configured collection age.
   */
  isStale(ageLimitOverrideMs?: number): boolean {
    if (this.state !== "valid") return false;
    if (this.scheduler.busy) return false;
    const now = this.time.now();
    if (now < this.keepAliveUntilMs) return false;
    if (this.deactivateRequested) return true;
    return now - this.lastActiveMs >= (ageLimitOverrideMs ?? this.collectionAgeMs);
  }

  /**
   * True when a caller (e.g. `IGrainManagementExtension.deactivateOnIdle()`)
   * has flagged this activation to go away and it is idle right now (not
   * mid-turn). Unlike `isStale()`, this ignores `keepAliveUntilMs` and the
   * age-based idle threshold — it exists solely so `Catalog.getOrActivate`
   * can notice a PENDING explicit deactivation request lazily, on the very
   * next lookup for this grain id, and finalize the old activation before
   * handing out a fresh one (see `grain-management-extension.ts` for why the
   * finalization can't happen synchronously inside `deactivateOnIdle()`
   * itself). `delayDeactivation` deliberately does not suppress this: an
   * explicit deactivateOnIdle() request should not be overridden by an
   * unrelated keep-alive.
   */
  get deactivationRequestedAndIdle(): boolean {
    return this.state === "valid" && !this.scheduler.busy && this.deactivateRequested;
  }

  private touch(): void {
    this.lastActiveMs = this.time.now();
  }

  private async callMethod(req: InvocationRequest): Promise<unknown> {
    // Stream delivery is a system extension, not a grain method: route it to the
    // handler the grain registered when it subscribed. Already on a turn here.
    if (req.interfaceId === StreamConsumerInterface.id) {
      if (req.method === "confirmStreamSubscription") {
        const [streamKey] = req.args as [string];
        return this.probeStreamSubscription(streamKey);
      }
      const [streamKey, event, token] = req.args as [string, unknown, number];
      const handler = this.streamHandlerFor(streamKey);
      if (handler !== undefined) {
        await this.runSystemExtensionDelivery(req, () =>
          handler.onNext(event, new SequenceToken(token)),
        );
      }
      return undefined;
    }
    // Broadcast-channel delivery is likewise a system extension: route the
    // published item to the grain's broadcast observer.
    if (req.interfaceId === BroadcastConsumerInterface.id) {
      const [channelKey, item] = req.args as [string, unknown];
      const handler = this.broadcastHandlerFor(channelKey);
      if (handler !== undefined) {
        await this.runSystemExtensionDelivery(req, () =>
          Promise.resolve(handler.onPublished(item)),
        );
      }
      return undefined;
    }
    // Durable-job delivery is a system extension: run one attempt of
    // the job on the grain's `DURABLE_JOB_HANDLER`, as a turn here. A grain with
    // no handler throws, which the executor catches as `Failed` (the retry policy
    // then drops it) — rather than treating a missing handler as completion, and
    // without putting a non-serializable Error object in the cross-silo result.
    if (req.interfaceId === DurableJobConsumerInterface.id) {
      const [job] = req.args as [JobRunContext];
      const handler = durableJobHandler(this.instance);
      if (handler === undefined) {
        throw new GrainCallError(`grain ${this.id.toString()} has no durable job handler`);
      }
      return await handler(job);
    }
    // Transaction-resource extension: drive a named transactional state's
    // prepare/commit/abort for the agent on another silo, found by state name.
    if (req.interfaceId === TransactionResourceInterface.id) {
      return await this.invokeTransactionResource(req);
    }
    // GrainExtension dispatch: an interface marked `extension` in the registry
    // routes to a per-activation object bound via `getOrSetExtension`
    // (auto-installed from `extensionFactories` if not yet bound), NOT to
    // `this.instance` — an orthogonal method surface, dispatched by interface.
    // Runs through the SAME incoming call-filter pipeline as the grain-method
    // path below (Orleans parity: filters wrap extension calls too).
    const iface = getGrainInterface(req.interfaceId);
    if (iface?.extension === true) {
      return await this.invokeExtension(req, iface.name);
    }
    const fn = (this.instance as unknown as Record<string, unknown>)[req.method];
    if (typeof fn !== "function") {
      throw new GrainCallError(`grain ${this.id.toString()} has no method ${req.method}`);
    }
    const method = fn as (...args: unknown[]) => unknown;
    // Bind any wire-arrived cancellation-token placeholders to live tokens,
    // in place on `req.args`, BEFORE the method runs — once, here, so both the
    // no-filter and filtered dispatch paths below see the bound arguments
    // (`context.args` is a copy of `req.args` taken after this call).
    this.bindCancellationTokens(req.args);
    // The grain's own filter (if it declares one) runs innermost — after the
    // silo-wide filters, just before the method (Orleans parity).
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      return await method.apply(this.instance, req.args);
    }
    // Run the grain-method dispatch through the incoming call-filter pipeline;
    // filters see the context and proceed via `invoke()` (Orleans parity).
    // `headers` is the SAME object `invocationContext`'s AsyncLocalStorage
    // store holds for this turn (installed by `invoke()`) rather than a fresh
    // copy of `req.headers` — so a filter's write is visible to the grain
    // method body via `requestContext.get()` (Orleans parity: RequestContext
    // set in a filter flows into the handler it wraps).
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: req.sender,
      interfaceId: req.interfaceId,
      interfaceName: getGrainInterface(req.interfaceId)?.name ?? "",
      methodName: req.method,
      args: [...req.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    return await runCallFilters(filters, context, () =>
      Promise.resolve(method.apply(this.instance, context.args)),
    );
  }

  /**
   * Deliver a system-extension event (a stream `onNext`, a broadcast-channel
   * `onPublished`) through the SAME incoming call-filter pipeline as an
   * ordinary grain method (Orleans parity: an `IIncomingGrainCallFilter` wraps
   * stream and broadcast deliveries too, so a grain that filters its own calls
   * — e.g. `StreamInterceptionGrain` — can observe and react to a delivery it
   * just processed). A grain with no filters (the common case) delivers
   * directly, so this is behaviour-neutral for every non-filtering consumer.
   */
  private runSystemExtensionDelivery(
    req: InvocationRequest,
    deliver: () => Promise<void>,
  ): Promise<void> {
    return this.deliverThroughIncomingFilters(
      { interfaceId: req.interfaceId, methodName: req.method, source: req.sender, args: req.args },
      deliver,
    );
  }

  /**
   * Run a memory-stream `onNext` delivery (which reaches this activation as a
   * scheduler turn, not a `callMethod` dispatch — see
   * `ActivationStreamProvider`) through the incoming call-filter pipeline, so a
   * grain subscribed to a memory stream sees the same filter wrapping as one
   * fed by a pulling-agent provider (whose delivery flows through `callMethod`'s
   * `StreamConsumerInterface` branch). Behaviour-neutral without filters.
   */
  runStreamDelivery(deliver: () => Promise<void>): Promise<unknown> {
    return this.runStreamTurn(() =>
      this.deliverThroughIncomingFilters(
        {
          interfaceId: StreamConsumerInterface.id,
          methodName: "onNext",
          source: this.id,
          args: [],
        },
        deliver,
      ),
    );
  }

  private async deliverThroughIncomingFilters(
    seed: {
      interfaceId: number;
      methodName: string;
      source: GrainId | undefined;
      args: readonly unknown[];
    },
    deliver: () => Promise<void>,
  ): Promise<void> {
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      await deliver();
      return;
    }
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: seed.source,
      interfaceId: seed.interfaceId,
      interfaceName: getGrainInterface(seed.interfaceId)?.name ?? "",
      methodName: seed.methodName,
      args: [...seed.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    await runCallFilters(filters, context, () => Promise.resolve(deliver()));
  }

  /**
   * Replace each `CancellationTokenPlaceholder` in `args` (produced by
   * `decodeValue`, which has no activation context to bind a live signal) with
   * a real `GrainCancellationToken` whose `signal` is this activation's own
   * cancellation extension's controller for that `tokenId` — so a later
   * `cancelRemoteToken(tokenId)` call (routed to the SAME extension instance
   * via `getOrSetExtension`'s idempotent binding) fires it. A placeholder that
   * arrived already-cancelled (`cancelled: true`, set when `cancel()` ran
   * before the call was even sent) pre-aborts the controller here, so the
   * grain method sees an already-fired signal instead of racing one.
   */
  private bindCancellationTokens(args: unknown[]): void {
    for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (!(arg instanceof CancellationTokenPlaceholder)) continue;
      // Auto-install through the catalog's registered factory (which wires
      // this silo's real cascade canceller — see `Catalog`/`ClusterNode`)
      // rather than calling `cancellationExtensionFactory` bare; a standalone
      // activation with no catalog-set factories falls back to a no-op
      // canceller, matching the pre-cascade single-hop behaviour.
      const ext = this.getOrSetExtension(
        ICancellationSourcesExtension.id,
        () =>
          (this.extensionFactories?.get(ICancellationSourcesExtension.id)?.(this) ??
            cancellationExtensionFactory(async () => {})) as CancellationSourcesExtension,
      );
      const controller = ext.getOrCreateController(arg.tokenId);
      if (arg.cancelled) controller.abort();
      const tokenId = arg.tokenId;
      args[i] = new GrainCancellationToken({
        tokenId,
        signal: controller.signal,
        // If the grain forwards this token on to another grain call, the
        // grain-factory dispatch hook (`recordCancellationTarget`) calls this
        // so a later `cancelRemoteToken(tokenId)` reaching THIS activation
        // cascades on to that further target too.
        onDispatchToTarget: (target) => ext.recordForwardTarget(tokenId, target),
      });
    }
  }

  /**
   * Route a call targeting an extension interface to its bound instance,
   * auto-installing from `extensionFactories` if not yet bound (see
   * `setExtensionFactories`). Throws `GrainExtensionNotInstalledException`
   * when neither a bound instance nor a factory exists.
   *
   * Runs through the same incoming call-filter pipeline as an ordinary
   * grain-method call (Orleans parity: `GrainCallFilter_GrainExtension`) —
   * additive: with no silo-wide filters and no grain-level filter, this stays
   * a direct invoke of the extension method, same as before filters wrapped
   * extension calls at all.
   */
  private async invokeExtension(req: InvocationRequest, interfaceName: string): Promise<unknown> {
    let ext = this.extensions.get(req.interfaceId);
    if (ext === undefined) {
      const factory = this.extensionFactories?.get(req.interfaceId);
      if (factory !== undefined) {
        ext = factory(this);
        this.extensions.set(req.interfaceId, ext);
      }
    }
    if (ext === undefined) {
      throw new GrainExtensionNotInstalledException(
        `extension ${interfaceName} is not installed on ${this.id.toString()}`,
      );
    }
    const fn = (ext as Record<string, unknown>)[req.method];
    if (typeof fn !== "function") {
      throw new GrainCallError(`extension ${interfaceName} has no method ${req.method}`);
    }
    const method = fn as (...args: unknown[]) => unknown;
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      return await method.apply(ext, req.args);
    }
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: req.sender,
      interfaceId: req.interfaceId,
      interfaceName,
      methodName: req.method,
      args: [...req.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      // Orleans parity: `IGrainCallContext.Grain` is always the grain instance
      // (`GrainMethodInvoker.Grain => grainContext.GrainInstance`), never the
      // extension component itself, even when the call targets an extension
      // interface — so the grain's own `INCOMING_CALL_FILTER` (looked up via
      // `this.instance` above, not the extension) applies here too.
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    return await runCallFilters(filters, context, () =>
      Promise.resolve(method.apply(ext, context.args)),
    );
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
