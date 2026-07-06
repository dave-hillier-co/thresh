import { GrainCallError, RejectionError } from "@tsva/core/errors";
import { createClientId, createObserverId, isObserverGrainId } from "@tsva/core/client-grain-id";
import type { Grain } from "@tsva/core/grain";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import { getGrainInterface } from "@tsva/core/grain-interface";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import {
  grainReferenceIdentity,
  type GrainReferenceIdentity,
} from "@tsva/core/grain-reference";
import type { GrainType } from "@tsva/core/grain-type";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { InvocationRequest } from "@tsva/core/request";
import type { SiloAddress } from "@tsva/core/silo-address";
import { ConnectionManager } from "@tsva/messaging/connection-manager";
import { CorrelationTable } from "@tsva/messaging/correlation-table";
import { nextCorrelationId, responseTo, type Message, type ResponseKind } from "@tsva/messaging/message";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import type { Serializer } from "@tsva/messaging/serializer";
import type { Listener, Transport } from "@tsva/messaging/transport";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import { GatewayManager } from "@tsva/client/gateway-manager";
import { staticGatewayProvider, type GatewayListProvider } from "@tsva/client/gateway-provider";

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
   * The client's own identity, used to mint observer references
   * (`createObjectReference`). Defaults to a fresh random client id; tests
   * override it for deterministic assertions.
   */
  clientId?: GrainId;
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
  interfaces: GrainInterface<unknown>[];
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
  private readonly clientId: GrainId;
  private readonly localObjects = new Map<string, LocalObjectEntry>();
  private listener: Listener | undefined;

  constructor(private readonly config: ClientConfig) {
    this.callTimeoutMs = config.callTimeoutMs ?? 30_000;
    this.connections = new ConnectionManager(config.transport, config.local, config.clusterId);
    this.serializer =
      config.serializer ??
      new MessagePackSerializer({ resolveGrainReference: (id) => this.rehydrate(id) });
    this.factory = new GrainFactory((interfaceId) => this.resolveGrainType(interfaceId));
    this.factory.setDispatcher(this);
    const provider =
      config.gateways ??
      (config.gateway !== undefined ? staticGatewayProvider([config.gateway]) : undefined);
    if (provider === undefined) {
      throw new Error("client requires a `gateway` or a `gateways` provider");
    }
    this.gateways = new GatewayManager(provider);
    this.delay = config.delay ?? defaultDelay;
    this.clientId = config.clientId ?? createClientId();
  }

  /**
   * Map a grain's interfaces to its type (needed to address the same activation a
   * silo-side caller would, since TypeScript interfaces are erased). Only the
   * grain's metadata is read; no instance is created.
   */
  registerGrain(ctor: new () => Grain, registration: GrainRegistration): this {
    const metadata = getGrainMetadata(ctor);
    if (metadata === undefined) throw new Error(`${ctor.name} is not decorated with @grain()`);
    for (const iface of registration.interfaces) {
      this.interfaceToGrainType.set(iface.id, metadata.grainType);
    }
    return this;
  }

  registerGrains(
    registrations: { ctor: new () => Grain; interfaces: GrainInterface<unknown>[] }[],
  ) {
    for (const r of registrations) this.registerGrain(r.ctor, r);
    return this;
  }

  /** Begin listening for replies and learn the initial gateway set; resolves once reachable. */
  async connect(): Promise<this> {
    this.listener = await this.config.transport.listen(this.config.local, (m) => this.onMessage(m));
    await this.gateways.refresh();
    return this;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  /**
   * Host `obj` as a callback object a grain can invoke (Orleans'
   * `CreateObjectReference`): mint a fresh observer identity scoped to this
   * client, register it locally, and return a grain-reference proxy carrying
   * that identity — pass it as a method argument like any other grain ref;
   * the serializer reduces it to `{grainId, interfaceId}` on the wire.
   */
  createObjectReference<T>(def: GrainInterface<T>, obj: object): T {
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
   */
  async invoke(req: InvocationRequest): Promise<unknown> {
    let refreshed = false;
    let lastError: unknown;
    let backoffMs = MINIMUM_INTERCONNECT_DELAY_MS;
    let hadFailure = false;
    for (;;) {
      // Wait between failed attempts so the loop does not busy-spin when every
      // gateway is unreachable (Orleans uses MINIMUM_INTERCONNECT_DELAY = 100ms
      // between connect attempts; we double up to a 2s cap).
      if (hadFailure) {
        await this.delay(backoffMs);
        backoffMs = Math.min(backoffMs * 2, MAX_INTERCONNECT_DELAY_MS);
      }
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
      try {
        const conn = await this.connections.get(gateway);
        const correlationId = nextCorrelationId();
        const message: Message = {
          correlationId,
          direction: req.options.oneWay ? "oneWay" : "request",
          targetGrain: req.target,
          sendingSilo: this.config.local,
          sendingGrain: req.sender,
          interfaceId: req.interfaceId,
          method: req.method,
          requestContext: { reentrancyId: req.reentrancyId },
          body: this.serializer.serialize(req.args),
        };
        if (req.options.oneWay) {
          conn.send(message);
          return undefined;
        }
        const pending = this.correlation.register(correlationId, this.callTimeoutMs);
        conn.send(message);
        response = await pending;
      } catch (err) {
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
          this.config.local,
        ),
      );
      return;
    }
    try {
      const args = this.serializer.deserialize<unknown[]>(message.body);
      const method = entry.object[message.method];
      if (typeof method !== "function") {
        throw new GrainCallError(`hosted object has no method ${message.method}`);
      }
      const result = await Promise.resolve(method.apply(entry.object, args));
      if (message.direction === "oneWay") return;
      await this.replyToCaller(
        message,
        responseTo(message, "success", this.serializer.serialize(result), this.config.local),
      );
    } catch (err) {
      if (message.direction === "oneWay") return;
      const { kind, body } = this.serializeError(err);
      await this.replyToCaller(message, responseTo(message, kind, body, this.config.local));
    }
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

  /** Map a thrown error to the `(kind, body)` of an error/rejection response. */
  private serializeError(err: unknown): { kind: ResponseKind; body: Uint8Array } {
    return err instanceof RejectionError
      ? { kind: "rejection", body: this.serializer.serialize({ message: err.message, kind: err.kind }) }
      : {
          kind: "error",
          body: this.serializer.serialize({
            message: err instanceof Error ? err.message : String(err),
          }),
        };
  }

  private interpretResponse(response: Message): unknown {
    if (response.responseKind === "success") return this.serializer.deserialize(response.body);
    if (response.responseKind === "rejection") {
      const payload = this.serializer.deserialize<RejectionPayload>(response.body);
      throw new RejectionError(payload.message, payload.kind);
    }
    const payload = this.serializer.deserialize<{ message: string }>(response.body);
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
