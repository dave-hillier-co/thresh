import type { GrainInterface } from "@tsva/core/grain-interface";

/**
 * Lifecycle phase of a {@link GrainService} instance (Orleans
 * `GrainService.GrainServiceStatus`, a protected/internal enum there).
 */
export enum GrainServiceStatus {
  Booting = "booting",
  Started = "started",
  Stopped = "stopped",
}

/**
 * Base class for a per-silo "grain service" (Orleans `GrainService` —
 * `Orleans.Runtime.Services.GrainService` / `Orleans.Runtime.GrainService`): a
 * single system service instantiated once per silo at startup, with its own
 * `init`/`start`/`stop` lifecycle, reachable from ordinary grains via
 * `GrainRuntime.getGrainService` (see `grain-runtime-impl.ts`). Distinct from
 * grain extensions (`IGrainExtension`), which bind per grain ACTIVATION — a
 * grain service can itself host extension instances via `addExtension`/
 * `asReference`, mirroring Orleans' `GrainService : SystemTarget` supporting
 * `ISiloBuilder.AddGrainExtension` on the service's own registration chain.
 *
 * Deviation from Orleans: `GrainService` there is a `SystemTarget` addressed
 * through the consistent-hash ring (`IRingRangeListener`), so a
 * `GrainServiceClient<T>` on any silo can reach the ONE instance that owns a
 * given key, wherever it lives, and range-changes redistribute the load
 * grain-service instances handle. This port has no cross-silo system-target
 * messaging substrate to route such a call over, so every silo instead runs
 * its own independent instance of each registered grain service, and a grain
 * only ever reaches the one on its OWN hosting silo (`GrainRuntime`
 * `getGrainService` resolves the local `GrainServiceRegistry`). Single-silo
 * clusters — including Orleans' own `GrainServiceTests` BVT suite, which pins
 * `InitialSilosCount = 1` — are unaffected by this; it only matters for a
 * grain service that would need to be reached identically from every silo in
 * a multi-silo cluster, which no ported test exercises.
 */
export abstract class GrainService {
  status: GrainServiceStatus = GrainServiceStatus.Booting;
  private readonly extensions = new Map<number, object>();

  /**
   * Invoked once at silo startup, before `start()`. Override to run setup;
   * the base implementation is a no-op (Orleans `GrainService.Init`).
   */
  init(): Promise<void> | void {}

  /**
   * Invoked once, right after `init()` resolves. Marks the service started
   * and fires the deferred `startInBackground()` hook without awaiting it —
   * matching Orleans `GrainService.Start()`, which returns immediately while
   * `StartInBackground()` runs to completion separately.
   */
  start(): void {
    this.status = GrainServiceStatus.Started;
    void this.startInBackground();
  }

  /**
   * Deferred part of startup (Orleans `GrainService.StartInBackground`),
   * invoked by `start()` without being awaited. Override for expensive
   * warm-up work that shouldn't hold up the rest of silo startup; the base
   * implementation is a no-op.
   */
  protected startInBackground(): Promise<void> | void {}

  /** Invoked once on silo shutdown (Orleans `GrainService.Stop`). */
  stop(): void {
    this.status = GrainServiceStatus.Stopped;
  }

  /**
   * Bind this instance's implementation of a `GrainExtension` interface
   * (Orleans `ISiloBuilder.AddGrainExtension<TExtensionInterface, TExtension>`
   * as chained off a service's registration). Call from the subclass'
   * constructor or `init()`.
   */
  protected addExtension<T extends object>(iface: GrainInterface<T>, instance: T): void {
    this.extensions.set(iface.id, instance);
  }

  /**
   * Get this instance's bound extension (Orleans `GrainService.AsReference<TExtensionInterface>()`,
   * used by a `GrainServiceClient` to reach the extension surface rather than
   * `ITestGrainService` itself). Throws if no extension was bound for `iface`.
   */
  asReference<T extends object>(iface: GrainInterface<T>): T {
    const extension = this.extensions.get(iface.id);
    if (extension === undefined) {
      throw new Error(`grain service has no extension bound for interface: ${iface.name}`);
    }
    return extension as T;
  }
}

/**
 * Per-silo registry of grain-service instances, keyed by the name each was
 * registered under (Orleans: one `GrainService` subclass per `GrainType`,
 * addressed via `SystemTargetGrainId`). The silo host builds one instance per
 * registered factory at startup, `init`s then `start`s every one (in
 * registration order), and `stop`s them all on shutdown.
 */
export class GrainServiceRegistry {
  private readonly services = new Map<string, GrainService>();

  register(name: string, service: GrainService): void {
    this.services.set(name, service);
  }

  /** Init then start every registered service, in registration order. */
  async start(): Promise<void> {
    for (const service of this.services.values()) {
      await service.init();
    }
    for (const service of this.services.values()) {
      service.start();
    }
  }

  /** Stop every registered service (silo shutdown). */
  stop(): void {
    for (const service of this.services.values()) service.stop();
  }

  /** Look up a registered service by name, cast to its concrete type. Throws if unregistered. */
  get<T extends GrainService>(name: string): T {
    const service = this.services.get(name);
    if (service === undefined) throw new Error(`no grain service registered: ${name}`);
    return service as T;
  }
}
