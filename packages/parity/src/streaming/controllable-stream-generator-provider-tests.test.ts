// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ControllableStreamGeneratorProviderTests.cs @ v10.1.0 (MIT).
// Fixture grains: test/Grains/TestGrainInterfaces/IGeneratedEventReporterGrain.cs,
// test/Grains/TestGrains/GeneratedEventReporterGrain.cs, GeneratedEventCollectorGrain.cs,
// GeneratedStreamTestConstants.cs @ v10.1.0 (MIT) — same fixture shapes as
// `stream-generator-provider-tests.test.ts`, copied in-file per that suite's convention.
//
// Both tests configure the `GeneratorAdapterFactory` test-only queue adapter
// live, mid-test, via `IManagementGrain.SendControlCommandToProvider`, then
// poll a dedicated `IGeneratedEventReporterGrain` that tallies per-stream
// event counts across every queue. Unlike the sibling
// `stream-generator-provider-tests.test.ts` (configured up front), the
// provider here starts with `eventsInStream: 0` (nothing generated) and is
// only reconfigured to `EVENTS_IN_STREAM` via the control command mid-test —
// mirroring upstream's `Configure` control command exactly.
//
// Silo count: upstream runs a 2-silo `TestCluster`; generator queues are
// partitioned across silos by the ownership ring, so `results.length` there
// is 2 (one control-command result per silo) while the report still totals
// `TOTAL_QUEUE_COUNT` streams cluster-wide. This port pins `initialSilos: 1`
// (matching the sibling `ValidateGeneratedStreamsTest`) so `results.length`
// is deterministically 1 and the single silo owns all 4 queues outright —
// correctness of the "4 streams, 100 events each" assertion matters more
// than matching upstream's silo count, and a 1-silo cluster removes any
// cross-silo queue-ownership timing from the test.
import { expect } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import {
  STREAM_GENERATOR_COMMAND_CONFIGURE,
  STREAM_SUBSCRIPTION_OBSERVER,
  type StreamHandler,
} from "@thresh/core/stream";
import type { GeneratedEvent } from "@thresh/streams/generator-stream-queue";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

const STREAM_PROVIDER_NAME = "GeneratedStreamProvider";
const STREAM_NAMESPACE = "Generated";
const EVENTS_IN_STREAM = 100;
const TOTAL_QUEUE_COUNT = 4;
const REPORTER_ID = "controllable-generated-stream-reporter";
const INITIAL_SILOS = 1;

interface IGeneratedEventReporterGrain extends GrainKey<string> {
  reportResult(
    streamKey: string,
    streamProvider: string,
    streamNamespace: string,
    count: number,
  ): Promise<void>;
  getReport(streamProvider: string, streamNamespace: string): Promise<Map<string, number>>;
  reset(): Promise<void>;
}
const IGeneratedEventReporterGrain = defineGrainInterface<IGeneratedEventReporterGrain>(
  "IGeneratedEventReporterGrain",
);

@grain()
class GeneratedEventReporterGrain extends Grain implements IGeneratedEventReporterGrain {
  private reports = new Map<string, Map<string, number>>();

  async reportResult(
    streamKey: string,
    streamProvider: string,
    streamNamespace: string,
    count: number,
  ): Promise<void> {
    const reportKey = `${streamProvider}/${streamNamespace}`;
    const counts = this.reports.get(reportKey) ?? new Map<string, number>();
    counts.set(streamKey, count);
    this.reports.set(reportKey, counts);
  }

  async getReport(streamProvider: string, streamNamespace: string): Promise<Map<string, number>> {
    return this.reports.get(`${streamProvider}/${streamNamespace}`) ?? new Map();
  }

  async reset(): Promise<void> {
    this.reports = new Map();
  }
}

interface IGeneratedEventCollectorGrain extends GrainKey<string> {
  marker(): Promise<string>;
}
const IGeneratedEventCollectorGrain = defineGrainInterface<IGeneratedEventCollectorGrain>(
  "IGeneratedEventCollectorGrain",
);

@grain()
@implicitStreamSubscription(STREAM_NAMESPACE)
class GeneratedEventCollectorGrain extends Grain implements IGeneratedEventCollectorGrain {
  private accumulated = 0;

  async marker(): Promise<string> {
    return "collector";
  }

  [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, key: string): StreamHandler<unknown> {
    return {
      onNext: async (event) => {
        this.accumulated += 1;
        if ((event as GeneratedEvent).eventType !== "report") return;
        const reporter = this.getGrain(IGeneratedEventReporterGrain, REPORTER_ID);
        await reporter.reportResult(
          `${namespace}/${key}`,
          STREAM_PROVIDER_NAME,
          STREAM_NAMESPACE,
          this.accumulated,
        );
      },
    };
  }
}

async function validateControllableGeneratedStreams(
  reporter: IGeneratedEventReporterGrain,
): Promise<void> {
  await waitFor(
    async () => {
      const report = await reporter.getReport(STREAM_PROVIDER_NAME, STREAM_NAMESPACE);
      return (
        report.size === TOTAL_QUEUE_COUNT &&
        [...report.values()].every((count) => count === EVENTS_IN_STREAM)
      );
    },
    { timeoutMs: 30_000 },
  );

  const report = await reporter.getReport(STREAM_PROVIDER_NAME, STREAM_NAMESPACE);
  expect(report.size).toBe(TOTAL_QUEUE_COUNT);
  for (const count of report.values()) {
    expect(count).toBe(EVENTS_IN_STREAM);
  }
}

async function withControllableGeneratedStreamsCluster(
  fn: (cluster: TestCluster, reporter: IGeneratedEventReporterGrain) => Promise<void>,
): Promise<void> {
  const cluster = await TestCluster.start({
    initialSilos: INITIAL_SILOS,
    grains: [
      { ctor: GeneratedEventCollectorGrain, interfaces: [IGeneratedEventCollectorGrain] },
      { ctor: GeneratedEventReporterGrain, interfaces: [IGeneratedEventReporterGrain] },
    ],
    configureSilo: (builder) => {
      // Starts generating nothing; the test drives generation live via
      // `SendControlCommandToProvider`, mirroring upstream's `Configure`.
      builder.addGeneratorStreams(
        STREAM_PROVIDER_NAME,
        { streamNamespace: STREAM_NAMESPACE, eventsInStream: 0 },
        { queueCount: TOTAL_QUEUE_COUNT, pollIntervalMs: 5 },
      );
    },
  });
  try {
    const reporter = cluster.getGrain(IGeneratedEventReporterGrain, REPORTER_ID);
    await fn(cluster, reporter);
  } finally {
    await cluster.dispose();
  }
}

orleansTest(
  "UnitTests.StreamingTests.ControllableStreamGeneratorProviderTests.ValidateControllableGeneratedStreamsTest",
  async () => {
    await withControllableGeneratedStreamsCluster(async (cluster, reporter) => {
      try {
        const mgmt = cluster.getGrain(IManagementGrain, 0n);
        const results = await mgmt.sendControlCommandToProvider(
          "PersistentStreamProvider",
          STREAM_PROVIDER_NAME,
          STREAM_GENERATOR_COMMAND_CONFIGURE,
          { streamNamespace: STREAM_NAMESPACE, eventsInStream: EVENTS_IN_STREAM },
        );
        expect(results.length).toBe(INITIAL_SILOS);
        for (const result of results) {
          expect(result).toBe(true);
        }

        await validateControllableGeneratedStreams(reporter);
      } finally {
        await reporter.reset();
      }
    });
  },
  35_000,
);

orleansTest(
  "UnitTests.StreamingTests.ControllableStreamGeneratorProviderTests.Validate2ControllableGeneratedStreamsTest",
  async () => {
    await withControllableGeneratedStreamsCluster(async (cluster, reporter) => {
      const mgmt = cluster.getGrain(IManagementGrain, 0n);
      for (let i = 0; i < 2; i++) {
        try {
          const results = await mgmt.sendControlCommandToProvider(
            "PersistentStreamProvider",
            STREAM_PROVIDER_NAME,
            STREAM_GENERATOR_COMMAND_CONFIGURE,
            { streamNamespace: STREAM_NAMESPACE, eventsInStream: EVENTS_IN_STREAM },
          );
          expect(results.length).toBe(INITIAL_SILOS);
          for (const result of results) {
            expect(result).toBe(true);
          }

          await validateControllableGeneratedStreams(reporter);
        } finally {
          await reporter.reset();
        }
      }
    });
  },
  35_000,
);
