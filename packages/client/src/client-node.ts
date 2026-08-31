import {
  BroadcastChannelPublisherInterface,
  broadcastPublisherGrainId,
  type BroadcastChannelProvider,
} from "@thresh/core/broadcast-channel";
import {
  GatewayTooBusyException,
  GrainCallError,
  GrainCallTimeoutError,
  RejectionError,
} from "@thresh/core/errors";
import { createClientId, createObserverId, isObserverGrainId } from "@thresh/core/client-grain-id";
import type { GrainClass } from "@thresh/core/grain-class";
import type { GrainRegistrationSpec } from "@thresh/core/grain-registration-spec";
import {
  mapCancellationValues,
  type CancellationValue,
} from "@thresh/core/cancellation-value-walk";
import {
  CancellationTokenPlaceholder,
  GrainCancellationToken,
  GrainCancellationTokenSource,
} from "@thresh/core/grain-cancellation-token";
import {
  grainIncomingFilter,
  runCallFilters,
  type IncomingGrainCallContext,
  type IncomingGrainCallFilter,
  type OutgoingGrainCallFilter,
} from "@thresh/core/grain-call-filter";
import type { GrainId } from "@thresh/core/grain-id";
import { IManagementGrain } from "@thresh/core/management-grain";
import type { GrainInterface } from "@thresh/core/grain-interface";
import { getGrainInterface } from "@thresh/core/grain-interface";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { grainReferenceIdentity, type GrainReferenceIdentity } from "@thresh/core/grain-reference";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKeyFor } from "@thresh/core/key-kinds";
import type { InvocationRequest } from "@thresh/core/request";
import { requestContextStore, runWithRequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";
import { ConnectionManager } from "@thresh/messaging/connection-manager";
import { CorrelationTable } from "@thresh/messaging/correlation-table";
import {
  nextCorrelationId,
  responseTo,
  type Message,
  type ResponseKind,
} from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Serializer } from "@thresh/messaging/serializer";
import type { Listener, Transport } from "@thresh/messaging/transport";
import { BroadcastChannelProviderImpl } from "@thresh/runtime/broadcast-channel-provider";
import { ICancellationSourcesExtension } from "@thresh/runtime/cancellation-extension";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { MANAGEMENT_GRAIN_TYPE } from "@thresh/runtime/management-grain";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { GatewayManager } from "@thresh/client/gateway-manager";
import { staticGatewayProvider, type GatewayListProvider } from "@thresh/client/gateway-provider";

export interface ClientConfig {
  clusterId: string;
  /** The client's own reachable address — replies flow back over a connection to it. */
  local: SiloAddress;
  transport: Transport;
  /**
   * A single fixed gateway silo to route every call through. Shorthand for a
   * one-entry `gateways` provider; supply one of `gateway` or `gateways`.
   */
  gateway?: SiloAddress;
  /**
   * Discovers the gateway silos to route through, with round-robin selection and
   * failover when one is unreachable (Orleans' gateway list provider + manager).
   * Use `membershipGatewayProvider`, `urlGatewayProvider`, or `staticGatewayProvider`.
   */
  gateways?: GatewayListProvider;
  serializer?: Serializer;
  callTimeoutMs?: number;
  /**
   * Injectable backoff used between failed gateway attempts in `invoke`.
   * Defaults to a real `setTimeout`-based sleep; tests substitute a fake to
   * keep the loop deterministic.
   */
  delay?: (ms: number) => Promise<void>;
  /**
   * Injectable wall clock `invoke` reads to enforce `callTimeoutMs` as a
   * cumulative budget across retries and backoff. Defaults to `Date.now`;
   * tests substitute a fake alongside `delay` to keep elapsed time
   * deterministic.
   */
  now?: () => number;
  /**
   * The client's own identity, used to mint observer references
   * (`createObjectReference`). Defaults to a fresh random client id; tests
   * override it for deterministic assertions.
   */
  clientId?: GrainId;
  /**
   * Incoming call filters wrapping a call to one of this client's hosted
   * objects (`createObjectReference`) — the client-side mirror of the
   * catalog's `incomingCallFilters` (Orleans' client-builder
   * `AddIncomingGrainCallFilter`), run through the SAME `runCallFilters`
   * pipeline the silo uses (see `ClientNode.dispatchToLocalObject`).
   */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
  /**
   * Outgoing call filters run on every call this client issues through its
   * internal `GrainFactory` (Orleans' client-builder `AddOutgoingGrainCallFilter`
   * — e.g. `tracingFilters().outgoing`, so a client-originated call injects W3C
   * trace context into `ctx.headers` the same way a grain-to-grain call does).
   * Applied via `GrainFactory.setOutgoingCallFilters`.
   */
  outgoingCallFilters?: readonly OutgoingGrainCallFilter[];
}

/** Orleans `ClientMessageCenter.MINIMUM_INTERCONNECT_DELAY`. */
const MINIMUM_INTERCONNECT_DELAY_MS = 100;
const MAX_INTERCONNECT_DELAY_MS = 2_000;

const defaultDelay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

interface RejectionPayload {
  message: string;
  kind: RejectionError["kind"];
}

interface GrainRegistration {
  readonly interfaces: readonly GrainInterface<unknown>[];
}

/** A client-hosted callback object registered under an observer `GrainId`. */
interface LocalObjectEntry {
  object: Record<string, (...args: unknown[]) => unknown>;
  interfaceId: number;
}

/**
 * An external client (docs/11). It is not a silo — it hosts no grains — but it
 * uses the same `getGrain` proxy mechanism, forwarding every call to a gateway
 * silo, which routes it to the grain's activation and replies. The client must
 * be reachable (it listens) so the gateway can return responses.
 */
export class ClientNode implements Dispatcher {
  private readonly interfaceToGrainType = new Map<number, GrainType>();
  private readonly connections: ConnectionManager;
  private readonly correlation = new CorrelationTable();
  private readonly serializer: Serializer;
  private readonly factory: GrainFactory;
  private readonly callTimeoutMs: number;
  private readonly gateways: GatewayManager;
  private readonly delay: (ms: number) => Promise<void>;
  private readonly now: () => number;
  private readonly clientId: GrainId;
  private readonly localObjects = new Map<string, LocalObjectEntry>();
  /**
   * Per-client store of `AbortController`s for cancellation tokens bound on
   * this client's hosted-object dispatch path, keyed by `tokenId` — the
   * client-side mirror of `ActivationData`'s per-activation extension store
   * (`CancellationSourcesExtension`), since a client hosts objects rather than
   * grain activations and so has no `getOrSetExtension` to lean on.
   */
  private readonly cancellationControllers = new Map<string, AbortController>();
  private readonly incomingCallFilters: readonly IncomingGrainCallFilter[];
  private listener: Listener | undefined;
  /**
   * The address this client tells a gateway to reach it on. Starts as the
   * configured `local` and is replaced by the listener's BOUND address in
   * `connect()`: a client asking for an ephemeral port (endpoint `host:0`,
   * which is how a silo provisions its embedded client leg) does not know its
   * own address until the listener binds, and every message it sends carries
   * that address as `sendingSilo` for the peer to reply to.
   */
  private localAddress: SiloAddress;

  constructor(private readonly config: ClientConfig) {
    this.localAddress = config.local;
    this.callTimeoutMs = config.callTimeoutMs ?? 30_000;
    this.clientId = config.clientId ?? createClientId();
    this.incomingCallFilters = config.incomingCallFilters ?? [];
    this.connections = new ConnectionManager(
      config.transport,
      config.local,
      config.clusterId,
      this.clientId,
    );
    this.serializer =
      config.serializer ??
      new MessagePackSerializer({ resolveGrainReference: (id) => this.rehydrate(id) });
    this.factory = new GrainFactory((interfaceId) => this.resolveGrainType(interfaceId));
    // The built-in system grains a silo hosts unconditionally: an Orleans
    // cluster client can call `IManagementGrain` without registering anything,
    // and so can this one - there is no application ctor to register it from.
    this.interfaceToGrainType.set(IManagementGrain.id, MANAGEMENT_GRAIN_TYPE);
    this.factory.setDispatcher(this);
    if (config.outgoingCallFilters !== undefined) {
      this.factory.setOutgoingCallFilters(config.outgoingCallFilters);
    }
    const provider =
      config.gateways ??
      (config.gateway !== undefined ? staticGatewayProvider([config.gateway]) : undefined);
    if (provider === undefined) {
      throw new Error("client requires a `gateway` or a `gateways` provider");
    }
    this.gateways = new GatewayManager(provider);
    this.delay = config.delay ?? defaultDelay;
    this.now = config.now ?? (() => Date.now());
  }

  /**
   * Map a grain's interfaces to its type (needed to address the same activation a
   * silo-side caller would, since TypeScript interfaces are erased). Only the
   * grain's metadata is read; no instance is created.
   */
  registerGrain(ctor: GrainClass, registration: GrainRegistration): this {
    const metadata = getGrainMetadata(ctor);
    if (metadata === undefined) throw new Error(`${ctor.name} is not decorated with @grain()`);
    for (const iface of registration.interfaces) {
      this.interfaceToGrainType.set(iface.id, metadata.grainType);
    }
    return this;
  }

  registerGrains(registrations: readonly GrainRegistrationSpec[]) {
    for (const r of registrations) this.registerGrain(r.ctor, r);
    return this;
  }

  /** Begin listening for replies and learn the initial gateway set; resolves once reachable. */
  async connect(): Promise<this> {
    this.listener = await this.config.transport.listen(this.config.local, (m) => this.onMessage(m));
    // Adopt the address the listener actually bound, before any gateway is
    // dialled: with an ephemeral port the configured `local` is a placeholder
    // and the peer would reply to a port nothing listens on.
    this.localAddress = this.listener.address;
    this.connections.setSelf(this.localAddress);
    await this.gateways.refresh();
    // Eagerly open a connection to a gateway so its `onAccept` fires and the
    // client-directory gossip runs immediately, rather than waiting for the
    // first real call. Best-effort: a temporarily unreachable gateway doesn't
    // fail connect() — the normal invoke() path retries and fails over.
    const gateway = this.gateways.next();
    if (gateway !== undefined) await this.connections.get(gateway).catch(() => undefined);
    return this;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  /**
   * Orleans `ILocalActivationStatusChecker.IsLocallyActivated` from the
   * client's perspective: a client never hosts grain activations (only
   * observer/local objects, a distinct id space — see `createObjectReference`),
   * so this always answers `false`, regardless of `id` or of what any silo in
   * the cluster is doing. Mirrors `SiloHost.isActive`'s silo-side check, which
   * this client is never the answer to.
   */
  isActive(_id: GrainId): boolean {
    return false;
  }

  /**
   * A `GrainCancellationTokenSource` whose `canceller` reaches any recorded
   * target grain's `ICancellationSourcesExtension`, wherever it lives
   * (`TestCluster.newCancellationTokenSource`'s client-side counterpart): the
   * client is itself a `Dispatcher`, so an extension reference built from its
   * own factory routes `cancelRemoteToken` the same way any other client call
   * does — via a gateway to the target's activation. Recording happens on
   * `GrainFactory`'s outgoing dispatch (`recordTarget`, run before this
   * client's own `invoke()` serializes the call), so passing `token` as an
   * argument to a grain method reaches the grain in time to be recorded, even
   * though the client's own call always crosses the wire.
   */
  newCancellationTokenSource(): GrainCancellationTokenSource {
    return new GrainCancellationTokenSource(async (target: GrainId, tokenId: string) => {
      const ext = this.factory.getReference(ICancellationSourcesExtension, target);
      await ext.cancelRemoteToken(tokenId);
    });
  }

  /**
   * A `BroadcastChannelProvider` whose writers publish from OUTSIDE any grain
   * (Orleans `IClusterClient.GetBroadcastChannelProvider`) — the client-side
   * counterpart of a grain's `this.runtime.getBroadcastChannelProvider()`.
   * Each `publish` reaches the gateway silo via THIS client's own
   * `GrainFactory` (the same proxy/dispatch path `newCancellationTokenSource`
   * uses), addressed to `broadcastPublisherGrainId()` — a fixed pseudo-target
   * `ClusterNode.receiveRequest` recognizes and handles directly (there's no
   * real grain activation to place), calling `publishToBroadcastChannel` on
   * whichever silo received the request, which then fans the item out
   * cluster-wide exactly like a grain-originated publish.
   */
  getBroadcastChannelProvider(name?: string): BroadcastChannelProvider {
    const providerName = name ?? "default";
    const publisher = this.factory.getReference(
      BroadcastChannelPublisherInterface,
      broadcastPublisherGrainId(),
    );
    return new BroadcastChannelProviderImpl(providerName, (provider, channel, item) =>
      publisher.publish(provider, channel, item),
    );
  }

  /**
   * Host `obj` as a callback object a grain can invoke (Orleans'
   * `CreateObjectReference`): mint a fresh observer identity scoped to this
   * client, register it locally, and return a grain-reference proxy carrying
   * that identity — pass it as a method argument like any other grain ref;
   * the serializer reduces it to `{grainId, interfaceId}` on the wire.
   */
  createObjectReference<T>(def: GrainInterface<T>, obj: object): T {
    if (grainReferenceIdentity(obj) !== undefined) {
      throw new TypeError("createObjectReference: obj is already a grain reference");
    }
    const observer = createObserverId(this.clientId);
    this.localObjects.set(observer.toString(), {
      object: obj as Record<string, (...args: unknown[]) => unknown>,
      interfaceId: def.id,
    });
    return this.factory.getReference(def, observer);
  }

  /** Stop hosting a reference previously returned by `createObjectReference`. */
  deleteObjectReference(ref: object): void {
    const identity = grainReferenceIdentity(ref);
    if (identity === undefined) return;
    this.localObjects.delete(identity.grainId.toString());
  }

  async close(): Promise<void> {
    await this.listener?.close();
    await this.connections.closeAll();
  }

  /**
   * Forward a grain call to a gateway and await its response, failing over to
   * another gateway when one is unreachable. A transport failure (a gateway we
   * cannot connect to or send through, or a reply that never arrives) drops that
   * gateway from rotation and retries the next; once all are exhausted we refresh
   * the gateway list once and retry. An error *carried in a response* (a grain
   * throw, or a rejection from the gateway's routing) is the call's real outcome
   * and propagates — failover would not change it.
   *
   * `callTimeoutMs` is a cumulative wall-clock budget for the whole call, not a
   * per-attempt allowance (Orleans deadline propagation): elapsed time is
   * tracked across every attempt and every backoff delay via the injectable
   * `now()`, and each attempt's correlation timeout is whatever of the budget
   * remains. Once the budget is exhausted — whether between attempts or while
   * one is outstanding — the call fails with a `GrainCallTimeoutError` naming
   * how many attempts were made, rather than retrying forever.
   */
  async invoke(req: InvocationRequest): Promise<unknown> {
    const deadline = this.now() + this.callTimeoutMs;
    let refreshed = false;
    let lastError: unknown;
    let backoffMs = MINIMUM_INTERCONNECT_DELAY_MS;
    let hadFailure = false;
    let attempts = 0;
    const timeoutError = (): GrainCallTimeoutError =>
      new GrainCallTimeoutError(
        `grain call timed out after ${this.callTimeoutMs}ms (${attempts} attempt${attempts === 1 ? "" : "s"} made)`,
      );
    for (;;) {
      // Wait between failed attempts so the loop does not busy-spin when every
      // gateway is unreachable (Orleans uses MINIMUM_INTERCONNECT_DELAY = 100ms
      // between connect attempts; we double up to a 2s cap). The wait itself
      // counts against the overall budget.
      if (hadFailure) {
        const remaining = deadline - this.now();
        if (remaining <= 0) throw timeoutError();
        await this.delay(Math.min(backoffMs, remaining));
        backoffMs = Math.min(backoffMs * 2, MAX_INTERCONNECT_DELAY_MS);
      }
      const remainingBudget = deadline - this.now();
      if (remainingBudget <= 0) throw timeoutError();
      let gateway = this.gateways.next();
      if (gateway === undefined && !refreshed) {
        await this.gateways.refresh();
        refreshed = true;
        gateway = this.gateways.next();
      }
      if (gateway === undefined) {
        throw lastError ?? new GrainCallError("no gateway available to route the call");
      }

      let response: Message;
      attempts++;
      try {
        const conn = await this.connections.get(gateway);
        const correlationId = nextCorrelationId();
        const message: Message = {
          correlationId,
          direction: req.options.oneWay ? "oneWay" : "request",
          targetGrain: req.target,
          sendingSilo: this.localAddress,
          sendingGrain: req.sender,
          interfaceId: req.interfaceId,
          method: req.method,
          requestContext: {
            reentrancyId: req.reentrancyId,
            ...(req.headers !== undefined ? { headers: req.headers } : {}),
          },
          body: this.serializer.serialize(req.args),
        };
        if (req.options.oneWay) {
          conn.send(message);
          return undefined;
        }
        const perAttemptTimeout = deadline - this.now();
        if (perAttemptTimeout <= 0) throw timeoutError();
        const pending = this.correlation.register(correlationId, perAttemptTimeout);
        conn.send(message);
        response = await pending;
      } catch (err) {
        if (this.now() >= deadline) throw timeoutError();
        // The gateway is unreachable (connect/send threw, or the reply timed out):
        // drop it and try another.
        this.gateways.markAsDead(gateway);
        await this.connections.drop(gateway);
        lastError = err;
        hadFailure = true;
        continue;
      }
      // We have a response: its kind decides success or an application error.
      return this.interpretResponse(response);
    }
  }

  private onMessage(message: Message): void {
    if (message.direction === "response") {
      this.correlation.complete(message);
      return;
    }
    // The only other traffic a client receives is a grain calling back into
    // one of its hosted observer objects (`createObjectReference`).
    if (isObserverGrainId(message.targetGrain)) {
      void this.dispatchToLocalObject(message);
    }
    // Any other inbound request/oneWay targets a real grain — a client hosts
    // none, so there is nothing to do with it.
  }

  /**
   * Invoke a hosted callback object for an inbound `request`/`oneWay`
   * targeting one of its observer references, and reply in kind (mirroring
   * the silo's own request/response construction convention). Async so it
   * runs fire-and-forget from the sync `onMessage`; every failure path here
   * is turned into an error response rather than an unhandled rejection.
   */
  private async dispatchToLocalObject(message: Message): Promise<void> {
    // A cancellation notification for a token this client bound while
    // dispatching a call to one of its hosted objects — not a hosted-object
    // call itself (the `ICancellationSourcesExtension` interface has no
    // entry in `localObjects`), so this must be checked before the
    // `localObjects` lookup below, which would otherwise reject it as
    // "no object hosted".
    if (message.interfaceId === ICancellationSourcesExtension.id) {
      const [tokenId] = this.serializer.deserialize<[string]>(message.body);
      this.getOrCreateCancellationController(tokenId).abort();
      if (message.direction === "oneWay") return;
      await this.replyToCaller(
        message,
        responseTo(message, "success", this.serializer.serialize(undefined), this.localAddress),
      );
      return;
    }
    const entry = this.localObjects.get(message.targetGrain.toString());
    if (entry === undefined) {
      if (message.direction === "oneWay") return; // nothing to reply to; drop silently
      await this.replyToCaller(
        message,
        responseTo(
          message,
          "rejection",
          this.serializer.serialize({
            message: `no object hosted for observer ${message.targetGrain.toString()}`,
            kind: "unknownTarget",
          }),
          this.localAddress,
        ),
      );
      return;
    }
    try {
      const args = this.serializer.deserialize<unknown[]>(message.body);
      // Bind any wire-arrived cancellation-token placeholders to live tokens
      // controlled by this client, BEFORE the hosted object's method runs
      // (mirrors `ActivationData.bindCancellationTokens` on the grain side).
      this.bindCancellationTokens(args);
      const method = entry.object[message.method];
      if (typeof method !== "function") {
        throw new GrainCallError(`hosted object has no method ${message.method}`);
      }
      // Run the dispatch inside an ambient invocation-context turn, exactly
      // like `ActivationData.invoke` does for a grain call: a filter's write
      // to `context.headers` is the SAME object the store holds, so it is
      // visible to the hosted object's method body via `requestContext.get`
      // (Orleans parity: `Observer_GrainCallFilter_Incoming_Order_Test`
      // builds up its RequestContext value through the filter chain the same
      // way the grain-side `GrainCallFilter_Incoming_Order_Test` does).
      const result = await runWithRequestContext(
        message.requestContext?.headers !== undefined ? { ...message.requestContext.headers } : {},
        () =>
          invocationContext.run(
            {
              senderId: message.sendingGrain,
              // A client-hosted observer isn't a grain activation, so it has
              // no id to stamp as `ownerId` for a further outgoing call.
              ownerId: undefined,
              reentrancyId: message.requestContext?.reentrancyId ?? "",
            },
            async () => {
              // The hosted object's own filter (if it declares one), like a
              // grain's, runs innermost — after this client's own incoming
              // filters, just before the method (Orleans parity).
              const own = grainIncomingFilter(entry.object);
              const filters =
                own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
              if (filters.length === 0) {
                return await method.apply(entry.object, args);
              }
              const context: IncomingGrainCallContext = {
                target: message.targetGrain,
                source: message.sendingGrain,
                interfaceId: message.interfaceId,
                interfaceName: getGrainInterface(message.interfaceId)?.name ?? "",
                methodName: message.method,
                args: [...args],
                result: undefined,
                headers: requestContextStore() ?? {},
                grain: entry.object,
                invoke: () => Promise.resolve(),
              };
              return await runCallFilters(filters, context, () =>
                Promise.resolve(method.apply(entry.object, context.args)),
              );
            },
          ),
      );
      if (message.direction === "oneWay") return;
      await this.replyToCaller(
        message,
        responseTo(message, "success", this.serializer.serialize(result), this.localAddress),
      );
    } catch (err) {
      if (message.direction === "oneWay") return;
      const { kind, body } = this.serializeError(err);
      await this.replyToCaller(message, responseTo(message, kind, body, this.localAddress));
    }
  }

  /** Get (or lazily create) this client's `AbortController` for `tokenId`. */
  private getOrCreateCancellationController(tokenId: string): AbortController {
    const existing = this.cancellationControllers.get(tokenId);
    if (existing !== undefined) return existing;
    const created = new AbortController();
    this.cancellationControllers.set(tokenId, created);
    return created;
  }

  /**
   * Replace each `CancellationTokenPlaceholder` in `args` (produced by
   * `decodeValue` for a wire-arrived `GrainCancellationToken` argument, which
   * has no client context to bind a live signal) with a real
   * `GrainCancellationToken` bound to this client's own controller for that
   * `tokenId` — so a later `cancelRemoteToken(tokenId)` notification (handled
   * above) fires it. A placeholder that arrived already-cancelled pre-aborts
   * the controller, mirroring `ActivationData.bindCancellationTokens`.
   */
  private bindCancellationTokens(args: unknown[]): void {
    // At any depth in a plain container, matching the caller-side conversion in
    // `GrainFactory` — a signal riding inside a request record is owed the same
    // unwrap as one in its own argument slot.
    for (let i = 0; i < args.length; i++) {
      args[i] = mapCancellationValues(args[i], (found) => this.bindCancellationValue(found));
    }
  }

  /** Bind one cancellation value found by `bindCancellationTokens`'s walk. */
  private bindCancellationValue(found: CancellationValue): unknown {
    // A same-silo-shaped token (an `asSignal` one never serialized) still owes
    // the hosted object an `AbortSignal`; anything else is already its shape.
    if (found instanceof GrainCancellationToken) {
      return found.asSignal ? found.signal : found;
    }
    if (!(found instanceof CancellationTokenPlaceholder)) return found;
    const controller = this.getOrCreateCancellationController(found.tokenId);
    if (found.cancelled) controller.abort();
    // The caller passed a plain `AbortSignal`; hand the hosted object one
    // back, exactly as `ActivationData.bindCancellationTokens` does.
    if (found.asSignal) return controller.signal;
    return new GrainCancellationToken({ tokenId: found.tokenId, signal: controller.signal });
  }

  /** Send a reply for a hosted-object dispatch back to the calling silo. */
  private async replyToCaller(request: Message, response: Message): Promise<void> {
    if (request.sendingSilo === undefined) return;
    try {
      const conn = await this.connections.get(request.sendingSilo);
      conn.send(response);
    } catch {
      // The caller has gone; dropping the reply is fine, matching the silo's
      // own best-effort reply behaviour.
    }
  }

  /**
   * Map a thrown error to the `(kind, body)` of an error/rejection response.
   *
   * Mirrors `ClusterNode.serializeError`: the error VALUE travels alongside its message, so a
   * domain error raised inside a client-hosted object (an observer callback) reaches the CALLING
   * GRAIN with its type — rebuilt by its surrogate, or as
   * `UnavailableExceptionFallbackException` carrying the name and carried state. Sending only the
   * message made this leg strictly worse than the silo's: registering a surrogate did not help,
   * because there was nothing on the wire for it to rebuild from.
   */
  private serializeError(err: unknown): { kind: ResponseKind; body: Uint8Array } {
    if (err instanceof RejectionError) {
      return {
        kind: "rejection",
        body: this.serializer.serialize({ message: err.message, kind: err.kind }),
      };
    }
    const message = err instanceof Error ? err.message : String(err);
    if (err instanceof Error) {
      try {
        return { kind: "error", body: this.serializer.serialize({ message, error: err }) };
      } catch {
        // An error carrying something the codec cannot encode must not turn the callee's failure
        // into a LOCAL serialization failure: the caller is owed the original fault, not this one.
      }
    }
    return { kind: "error", body: this.serializer.serialize({ message }) };
  }

  private interpretResponse(response: Message): unknown {
    if (response.responseKind === "success") return this.serializer.deserialize(response.body);
    if (response.responseKind === "rejection") {
      const payload = this.serializer.deserialize<RejectionPayload>(response.body);
      // A gateway's own overload rejection surfaces as `GatewayTooBusyException`
      // (Orleans parity), not the generic `RejectionError` every other
      // rejection kind uses — it's the one kind ORIGINATED specifically to
      // signal "retry me, possibly against a different gateway".
      if (payload.kind === "overloaded") throw new GatewayTooBusyException(payload.message);
      throw new RejectionError(payload.message, payload.kind);
    }
    const payload = this.serializer.deserialize<{
      message: string;
      aggregateMessages?: readonly string[];
      error?: unknown;
    }>(response.body);
    // A non-fire-and-forget broadcast publish's aggregated subscriber
    // failures (`ClusterNode.serializeError`) — reconstruct a real
    // `AggregateError` rather than collapsing to a generic `GrainCallError`,
    // so a caller can inspect `.errors` the way Orleans callers inspect
    // `AggregateException.InnerExceptions`.
    if (payload.aggregateMessages !== undefined) {
      throw new AggregateError(
        payload.aggregateMessages.map((m) => new Error(m)),
        payload.message,
      );
    }
    // A surrogate-registered error rebuilds as its own class (`ClusterNode.serializeError`), so a
    // client sees the same concrete type a silo caller would; a type with no surrogate arrives as
    // an `UnavailableExceptionFallbackException` carrying its name and carried state.
    // `GrainCallError` remains the floor for a peer that sent no error value at all.
    if (payload.error instanceof Error) throw payload.error;
    throw new GrainCallError(payload.message);
  }

  private resolveGrainType(interfaceId: number): GrainType {
    const grainType = this.interfaceToGrainType.get(interfaceId);
    if (grainType === undefined) {
      throw new Error(`client: no grain registered for interface ${interfaceId}`);
    }
    return grainType;
  }

  private rehydrate(id: GrainReferenceIdentity): unknown {
    const iface = getGrainInterface(id.interfaceId);
    if (iface === undefined) throw new GrainCallError(`unknown interface ${id.interfaceId}`);
    // Use the wire `grainId` as-is rather than re-resolving a type from the
    // interface: for a normal grain reference it is the same type `getGrain`
    // would produce, but for an observer reference it preserves the reserved
    // `$client` type + `+scope` key, which re-resolution would discard.
    return this.factory.getReference(iface, id.grainId);
  }
}

/** Build an external client; call `connect()` before issuing calls. */
export function createClient(config: ClientConfig): ClientNode {
  return new ClientNode(config);
}
