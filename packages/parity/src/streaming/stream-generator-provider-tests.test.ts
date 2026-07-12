// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StreamGeneratorProviderTests.cs @ v10.1.0 (MIT).
// Fixture grains: test/Grains/TestGrainInterfaces/IGeneratedEventReporterGrain.cs,
// test/Grains/TestGrains/GeneratedEventReporterGrain.cs, GeneratedEventCollectorGrain.cs,
// GeneratedStreamTestConstants.cs @ v10.1.0 (MIT).
//
// Upstream configures `GeneratorAdapterFactory` (a test-only queue adapter
// that synthesizes events instead of reading a real backing store) up front
// via a keyed `IStreamGeneratorConfig`, partitioned across
// `HashRingBasedStreamQueueMapper`'s `TotalQueueCount` physical queues; each
// queue's `SimpleGenerator` mints one random-guid stream and produces
// `EventsInStream` events for it. `GeneratedEventCollectorGrain` — implicitly
// subscribed to the generator's namespace — accumulates each stream's count
// and, on the final ("Report") event, forwards it to a well-known
// `GeneratedEventReporterGrain`; the test polls that reporter until every
// queue's stream shows the configured count.
//
// Ported onto `GeneratorPullingStreamProvider` (`packages/streams/src/generator-pulling-stream-provider.ts`)
// and `GeneratorStreamQueue` (`packages/streams/src/generator-stream-queue.ts`),
// which plug into the same `QueuePullingAgent`/queue-ownership stack
// `RedisPullingStreamProvider` uses. Collector delivery uses this framework's
// implicit-subscription model (`STREAM_SUBSCRIPTION_OBSERVER`, no explicit
// `subscribe()` call — see `implicit-subscription-key-type-grain-tests.test.ts`)
// rather than upstream's `OnActivateAsync` + `SubscribeAsync`.
import { expect } from "vitest";
import { grain, implicitStreamSubscription } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@tsva/core/stream";
import type { GeneratedEvent } from "@tsva/streams/generator-stream-queue";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";

const STREAM_PROVIDER_NAME = "GeneratedStreamProvider";
const STREAM_NAMESPACE = "Generated";
const EVENTS_IN_STREAM = 100;
const TOTAL_QUEUE_COUNT = 4;
const REPORTER_ID = "generated-stream-reporter";

interface IGeneratedEventReporterGrain extends GrainWithStringKey {
  reportResult(streamKey: string, streamProvider: string, streamNamespace: string, count: number): Promise<void>;
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

interface IGeneratedEventCollectorGrain extends GrainWithStringKey {
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

orleansTest(
  "UnitTests.StreamingTests.StreamGeneratorProviderTests.ValidateGeneratedStreamsTest",
  async () => {
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: GeneratedEventCollectorGrain, interfaces: [IGeneratedEventCollectorGrain] },
        { ctor: GeneratedEventReporterGrain, interfaces: [IGeneratedEventReporterGrain] },
      ],
      configureSilo: (builder) => {
        builder.addGeneratorStreams(
          STREAM_PROVIDER_NAME,
          { streamNamespace: STREAM_NAMESPACE, eventsInStream: EVENTS_IN_STREAM },
          { queueCount: TOTAL_QUEUE_COUNT, pollIntervalMs: 5 },
        );
      },
    });
    try {
      const reporter = cluster.getGrain(IGeneratedEventReporterGrain, REPORTER_ID);
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

      // one stream per queue
      const report = await reporter.getReport(STREAM_PROVIDER_NAME, STREAM_NAMESPACE);
      expect(report.size).toBe(TOTAL_QUEUE_COUNT);
      for (const count of report.values()) {
        expect(count).toBe(EVENTS_IN_STREAM);
      }
    } finally {
      await cluster.dispose();
    }
  },
  35_000,
);
