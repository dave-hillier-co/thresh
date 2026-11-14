import { afterAll, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { completed, type DurableJob, type JobRunContext } from "@thresh/core/durable-job";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger, LogFields } from "@thresh/core/logger";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import {
  LocalDurableJobManager,
  resolveOptions,
  type ShardOwnershipContext,
} from "@thresh/durable-jobs/local-durable-job-manager";

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 });
const meterProvider = new MeterProvider({ readers: [reader] });
metrics.setGlobalMeterProvider(meterProvider);

interface LogRecord {
  level: string;
  message: string;
  fields: LogFields | undefined;
}

function recordingLogger(): { logger: Logger; records: LogRecord[] } {
  const records: LogRecord[] = [];
  const at =
    (level: string) =>
    (message: string, fields?: LogFields): void => {
      records.push({ level, message, fields });
    };
  return {
    records,
    logger: { debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") },
  };
}

const flush = async () => {
  for (let i = 0; i < 50; i++) await Promise.resolve();
};

function ctx(local: string, ...active: string[]): ShardOwnershipContext {
  return { localRingKey: local, activeRingKeys: active };
}

function manager(
  store: MemoryJobShardStore,
  time: FakeTimeProvider,
  ownership: ShardOwnershipContext,
  extra: {
    run?: (job: JobRunContext) => void;
    logger?: Logger;
    check?: number;
    maxAdopted?: number;
    forward?: (owner: string, job: DurableJob) => Promise<boolean>;
  } = {},
): LocalDurableJobManager {
  const options = {
    ...resolveOptions({
      periodicShardCheckInterval: { ms: extra.check ?? 0 },
      maxAdoptedCount: extra.maxAdopted ?? 3,
    }),
    ...(extra.logger !== undefined ? { logger: extra.logger } : {}),
  };
  return new LocalDurableJobManager(
    store,
    time,
    async (job) => {
      extra.run?.(job);
      return completed;
    },
    options,
    ownership,
    undefined,
    extra.forward,
  );
}

const request = (name: string, dueMs: number) => ({
  name,
  dueTime: new Date(dueMs),
  target: new GrainId("test", name),
});

describe("a job is not stranded by a lost forward", () => {
  it("runs a job written to the store for a shard another silo is running, with no membership change", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const ran: string[] = [];
    const { logger, records } = recordingLogger();
    const view = ctx("a", "a", "b");
    const owner = manager(store, time, view, {
      run: (j) => ran.push(j.name),
      check: 1_000,
    });
    // The non-owner's forward is lost.
    const nonOwner = manager(store, time, ctx("b", "a", "b"), {
      logger,
      forward: async () => {
        throw new Error("forward lost");
      },
    });
    await owner.scheduleJob(request("first", 600_000));
    await owner.refreshOwnership(view); // arms the periodic check
    await nonOwner.scheduleJob(request("stranded", 5_000));
    await flush();

    time.advance(1_000);
    await flush();
    time.advance(5_000);
    await flush();

    expect(ran).toEqual(["stranded"]);
    expect(
      records.some((r) => r.level === "error" && r.message.includes("forward")),
    ).toBe(true);
    await owner.stop();
  });

  it("does not run a job twice when the store re-read finds one it already loaded", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const ran: string[] = [];
    const view = ctx("a", "a");
    const owner = manager(store, time, view, { run: (j) => ran.push(j.name), check: 1_000 });
    await owner.scheduleJob(request("once", 5_000));
    await owner.refreshOwnership(view);
    time.advance(1_000);
    await flush();
    time.advance(10_000);
    await flush();
    time.advance(5_000);
    await flush();
    expect(ran).toEqual(["once"]);
    await owner.stop();
  });

  it("logs a forward that the target declines", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const { logger, records } = recordingLogger();
    const owner = manager(store, time, ctx("a", "a", "b"));
    await owner.scheduleJob(request("first", 600_000));
    const nonOwner = manager(store, time, ctx("b", "a", "b"), {
      logger,
      forward: async () => false,
    });
    await nonOwner.scheduleJob(request("declined", 700_000));
    expect(records.some((r) => r.message.includes("forward"))).toBe(true);
    await owner.stop();
  });
});

describe("poisoned shards are visible", () => {
  it("logs and counts a shard poisoned by a dead-owner adoption past the maximum", async () => {
    afterAll(async () => meterProvider.shutdown());
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    await store.persistAdd({
      id: "j",
      name: "job",
      dueTime: new Date(600_000),
      target: new GrainId("test", "g"),
      shardKey: 0,
      metadata: {},
    });
    await store.claimShard(0, "dead", { maxAdoptedCount: 1 });
    const { logger, records } = recordingLogger();
    const m = manager(store, time, ctx("a", "a"), { logger, maxAdopted: 0 });
    await m.refreshOwnership(ctx("a", "a"));

    expect(m.ownedShards()).toEqual([]);
    expect((await store.listShards())[0]?.poisoned).toBe(true);
    expect(records.filter((r) => r.message.includes("poison"))).toHaveLength(1);
    await meterProvider.forceFlush();
    const counter = exporter
      .getMetrics()
      .flatMap((rm) => rm.scopeMetrics)
      .flatMap((sm) => sm.metrics)
      .find((x) => x.descriptor.name === "thresh.durablejobs.shard.poisoned");
    expect(counter?.dataPoints[0]?.value).toBe(1);

    // A later reconcile does not re-report an already poisoned shard.
    await m.refreshOwnership(ctx("a", "a"));
    expect(records.filter((r) => r.message.includes("poison"))).toHaveLength(1);
  });
});

/** A store whose every persist and release fails. */
class FailingStore extends MemoryJobShardStore {
  failing = false;
  override async persistRemove(shardKey: number, jobId: string): Promise<void> {
    if (this.failing) throw new Error("remove down");
    return super.persistRemove(shardKey, jobId);
  }
  override async persistRunStart(shardKey: number, jobId: string, runId: string): Promise<void> {
    if (this.failing) throw new Error("run-start down");
    return super.persistRunStart(shardKey, jobId, runId);
  }
  override async persistRunComplete(
    shardKey: number,
    jobId: string,
    runId: string,
  ): Promise<void> {
    if (this.failing) throw new Error("run-complete down");
    return super.persistRunComplete(shardKey, jobId, runId);
  }
  override async releaseShard(shardKey: number, owner: string): Promise<void> {
    if (this.failing) throw new Error("release down");
    return super.releaseShard(shardKey, owner);
  }
  override async listShards() {
    if (this.listFailing) throw new Error("list down");
    return super.listShards();
  }
  listFailing = false;
}

describe("persist and release failures are logged", () => {
  it("logs a failed remove on cancel with no executor", async () => {
    const store = new FailingStore();
    const { logger, records } = recordingLogger();
    const m = manager(store, new FakeTimeProvider(), ctx("a", "a"), { logger });
    store.failing = true;
    await m.cancelJob({ id: "x", shardKey: 9 });
    expect(records.map((r) => r.message)).toContain("durable job remove failed");
  });

  it("logs failed run-start, run-complete and remove around a completed job", async () => {
    const store = new FailingStore();
    const time = new FakeTimeProvider();
    const { logger, records } = recordingLogger();
    const m = manager(store, time, ctx("a", "a"), { logger });
    await m.scheduleJob(request("j", 1_000));
    store.failing = true;
    time.advance(1_000);
    await flush();
    const messages = records.map((r) => r.message);
    expect(messages).toContain("durable job run-start failed");
    expect(messages).toContain("durable job run-complete failed");
    expect(messages).toContain("durable job remove failed");
    store.failing = false;
    await m.stop();
  });

  it("logs a failed release on stop and a failed periodic check", async () => {
    const store = new FailingStore();
    const time = new FakeTimeProvider();
    const { logger, records } = recordingLogger();
    const view = ctx("a", "a");
    const m = manager(store, time, view, { logger, check: 1_000 });
    await m.scheduleJob(request("j", 600_000));
    await m.refreshOwnership(view);
    store.listFailing = true;
    time.advance(1_000);
    await flush();
    expect(records.map((r) => r.message)).toContain("durable job shard check failed");
    store.listFailing = false;
    store.failing = true;
    await m.stop();
    expect(records.map((r) => r.message)).toContain("durable job shard release failed");
  });
});
