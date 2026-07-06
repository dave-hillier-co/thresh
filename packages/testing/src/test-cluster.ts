import type { Duration } from "@tsva/core/duration";
import type { Grain } from "@tsva/core/grain";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { MembershipService, MembershipSnapshot } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import type { TimeProvider } from "@tsva/core/time-provider";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { MemoryJobShardStore } from "@tsva/durable-jobs/memory-job-shard-store";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { MemoryTransactionalStorage } from "@tsva/transactions/memory-transactional-storage";
import { createSilo, type SiloBuilder } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";

export interface GrainRegistrationSpec {
  ctor: new () => Grain;
  interfaces: GrainInterface<unknown>[];
}

export interface TestClusterOptions {
  /** Number of silos to start with (Orleans TestClusterBuilder defaults to 2). */
  initialSilos?: number;
  clusterId?: string;
  /** Grains registered on every silo. */
  grains?: ReadonlyArray<GrainRegistrationSpec>;
  /** Clock injected into every silo; pass a `FakeTimeProvider` for determinism. */
  time?: TimeProvider;
  /**
   * Whether silos are built with transaction support. Defaults to `true`
   * (every silo gets a shared in-memory transactional-storage provider); pass
   * `false` to build a cluster with no transactional storage at all, so a
   * `[Transaction]`-style grain call throws `TransactionsDisabledError`
   * (Orleans: a cluster whose `SiloHostConfigurator` never calls
   * `.UseTransactions()`).
   */
  transactions?: boolean;
  /** Per-silo overrides, applied after the defaults. */
  configureSilo?: (builder: SiloBuilder, silo: { index: number; address: SiloAddress }) => void;
  /**
   * Static metadata a silo advertises via membership (e.g. `{ role: "worker" }`),
   * consulted by metadata-aware placement (Orleans role-based placement). Return
   * `undefined` for a silo that advertises no metadata.
   */
  siloMetadata?: (silo: {
    index: number;
    address: SiloAddress;
  }) => Readonly<Record<string, string>> | undefined;
  /**
   * Silo-wide default per-method response timeout, forwarded to every silo
   * (Orleans `[ResponseTimeout]` default). Off by default: pass a
   * `FakeTimeProvider` via `time` to drive it deterministically in tests.
   */
  defaultResponseTimeout?: Duration;
}

export interface TestSiloHandle {
  readonly index: number;
  readonly address: SiloAddress;
  readonly host: SiloHost;
}

/**
 * An in-process multi-silo cluster for tests — the analogue of
 * `Orleans.TestingHost.TestCluster`. Silos share one `InProcessNetwork`, a
 * mutable membership view, and one set of memory providers (so grain state,
 * reminders, journals and jobs survive a silo's death the way a durable
 * backend would); ported functional tests only supply grains.
 */
export class TestCluster {
  private live: InternalSilo[] = [];
  private nextIndex = 0;
  private disposed = false;
  // One membership authority for the whole cluster (silos see it through
  // per-silo `localSilo()` views) so every silo agrees on the view version.
  private shared: StaticMembershipService | undefined;

  // Cluster-wide "durable" backends, shared by every silo.
  readonly storage = new MemoryGrainStorage();
  readonly reminderTable = new MemoryReminderTable();
  readonly transactionalStorage = new MemoryTransactionalStorage();
  readonly journalStorage = new MemoryJournalStorage();
  readonly jobShardStore = new MemoryJobShardStore();

  private constructor(
    private readonly options: TestClusterOptions,
    readonly network: InProcessNetwork,
  ) {}

  static async start(options: TestClusterOptions = {}): Promise<TestCluster> {
    const cluster = new TestCluster(options, new InProcessNetwork());
    const count = options.initialSilos ?? 2;
    for (let i = 0; i < count; i += 1) cluster.buildSilo();
    await Promise.all(cluster.live.map((s) => s.host.start()));
    return cluster;
  }

  get silos(): ReadonlyArray<TestSiloHandle> {
    return this.live;
  }

  /** The first live silo; ported tests route client-style calls through it. */
  get primary(): TestSiloHandle {
    const first = this.live[0];
    if (first === undefined) throw new Error("TestCluster has no live silos");
    return first;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.primary.host.getGrain(def, key);
  }

  async startAdditionalSilo(): Promise<TestSiloHandle> {
    const silo = this.buildSilo();
    await silo.host.start();
    return silo;
  }

  /** Graceful shutdown: the silo drains and departs the membership view. */
  async stopSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    await silo.host.stop();
    this.shared?.removeSilo(silo.address);
  }

  /**
   * Abrupt failure: detach the silo from the network and declare it dead to the
   * survivors, without running any shutdown hooks — the in-process stand-in for
   * a killed pod.
   */
  async killSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    this.network.unregister(silo.address);
    this.shared?.removeSilo(silo.address);
  }

  /** Kill-then-replace: a fresh silo joins with a new uid, like a restarted pod. */
  async restartSilo(handle: TestSiloHandle): Promise<TestSiloHandle> {
    await this.killSilo(handle);
    return this.startAdditionalSilo();
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    const remaining = this.live;
    this.live = [];
    await Promise.all(remaining.map((s) => s.host.stop().catch(() => undefined)));
  }

  private take(handle: TestSiloHandle): InternalSilo {
    const silo = this.live.find((s) => s === handle);
    if (silo === undefined) throw new Error(`silo ${handle.address.podName} is not live`);
    this.live = this.live.filter((s) => s !== silo);
    return silo;
  }

  private buildSilo(): InternalSilo {
    const index = this.nextIndex;
    this.nextIndex += 1;
    const address = new SiloAddress(
      `test-silo-${index}`,
      `uid-${index}`,
      `test-silo-${index}:11111`,
    );
    const metadata = this.options.siloMetadata?.({ index, address });
    if (this.shared === undefined) {
      this.shared = new StaticMembershipService(
        address,
        [address],
        metadata !== undefined ? () => metadata : undefined,
      );
    } else {
      this.shared.addSilo(address, "active", metadata);
    }
    const membership = new MembershipView(this.shared, address);
    const builder = createSilo({
      clusterId: this.options.clusterId ?? "test-cluster",
      local: address,
      ...(this.options.time !== undefined ? { time: this.options.time } : {}),
      ...(metadata !== undefined ? { metadata } : {}),
      ...(this.options.defaultResponseTimeout !== undefined
        ? { defaultResponseTimeout: this.options.defaultResponseTimeout }
        : {}),
    })
      .useMembership(membership)
      .useInProcessTransport(this.network)
      .useMemoryStorage(this.storage)
      .useReminders(this.reminderTable)
      .useMemoryStreams()
      .useBroadcastChannels()
      .useMemoryJournaling(this.journalStorage)
      .useMemoryDurableJobs(this.jobShardStore);
    if (this.options.transactions ?? true) {
      builder.useMemoryTransactionalStorage(this.transactionalStorage);
    } else {
      builder.disableTransactions();
    }
    for (const spec of this.options.grains ?? []) {
      builder.registerGrain(spec.ctor, { interfaces: spec.interfaces });
    }
    this.options.configureSilo?.(builder, { index, address });
    const silo: InternalSilo = { index, address, membership, host: builder.build() };
    this.live.push(silo);
    return silo;
  }
}

interface InternalSilo extends TestSiloHandle {
  readonly membership: MembershipService;
}

/** Reports each silo's own `localSilo()` while sharing one membership view. */
class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}

  current(): MembershipSnapshot {
    return this.shared.current();
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    return this.shared.updates();
  }

  localSilo(): SiloAddress {
    return this.local;
  }
}
