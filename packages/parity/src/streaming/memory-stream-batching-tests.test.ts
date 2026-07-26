// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamBatchingTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamBatchingTestRunner.cs @ v10.1.0 (MIT).
// Grain: test/Grains/TestGrainInterfaces/IStreamBatchingTestConsumerGrain.cs,
// test/Grains/TestGrains/StreamBatchingTestConsumerGrain.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";
import {
  STREAM_SUBSCRIPTION_OBSERVER,
  type BatchedStreamItem,
  type StreamHandler,
} from "@thresh/core/stream";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { randomGuidKey } from "@thresh/parity/support/keys";

const BATCHING_NAMESPACE = "batching";
const NON_BATCHING_NAMESPACE = "nonbatching";

interface ConsumptionReport {
  consumed: number;
  maxBatchSize: number;
}

interface IStreamBatchingTestConsumerGrain extends GrainWithGuidKey {
  getConsumptionReport(): Promise<ConsumptionReport>;
}
const IStreamBatchingTestConsumerGrain = defineGrainInterface<IStreamBatchingTestConsumerGrain>(
  "IStreamBatchingTestConsumerGrain",
  { options: { getConsumptionReport: { readOnly: true } } },
);

// Upstream's `BatchingStreamBatchingTestConsumerGrain` picks a batch or
// single-item handler per namespace (`OnSubscribed` inspects the stream's
// namespace). This framework's implicit-subscription model resolves one
// observer per (namespace, key) pair (`STREAM_SUBSCRIPTION_OBSERVER`), so the
// same switch happens there instead of in an `OnSubscribed` callback.
@grain()
@implicitStreamSubscription(BATCHING_NAMESPACE)
@implicitStreamSubscription(NON_BATCHING_NAMESPACE)
class BatchingStreamBatchingTestConsumerGrain
  extends Grain
  implements IStreamBatchingTestConsumerGrain
{
  private report: ConsumptionReport = { consumed: 0, maxBatchSize: 0 };

  async getConsumptionReport(): Promise<ConsumptionReport> {
    return this.report;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, _key: string): StreamHandler<string> {
    if (namespace === BATCHING_NAMESPACE) {
      return {
        onNext: async () => {
          throw new Error("expected onNextBatch for the batching namespace");
        },
        onNextBatch: async (items: readonly BatchedStreamItem<string>[]) => {
          this.report = {
            consumed: this.report.consumed + items.length,
            maxBatchSize: Math.max(this.report.maxBatchSize, items.length),
          };
        },
      };
    }
    return {
      onNext: async () => {
        this.report = { consumed: this.report.consumed + 1, maxBatchSize: 1 };
      },
    };
  }
}

describe("UnitTests.StreamingTests.MemoryStreamBatchingTests", () => {
  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5649)",
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.SingleSendBatchConsume",
  );

  orleansTest(
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.BatchSendSingleConsume",
    async () => {
      const cluster = await TestCluster.start({
        grains: [
          {
            ctor: BatchingStreamBatchingTestConsumerGrain,
            interfaces: [IStreamBatchingTestConsumerGrain],
          },
        ],
      });
      try {
        const batchesSent = 3;
        const itemsPerBatch = 10;
        const expectedConsumed = batchesSent * itemsPerBatch;
        const streamKey = randomGuidKey();

        const streamProvider = cluster.getStreamProvider();
        if (streamProvider === undefined) throw new Error("no default stream provider configured");
        const stream = streamProvider.getStream<string>(NON_BATCHING_NAMESPACE, streamKey);
        for (let i = 0; i < batchesSent; i++) {
          const batch = Array.from({ length: itemsPerBatch }, (_unused, j) => String(i + j));
          await stream.publishBatch!(batch);
        }

        const consumer = cluster.getGrain(IStreamBatchingTestConsumerGrain, streamKey);
        const report = await consumer.getConsumptionReport();
        expect(report.consumed).toBe(expectedConsumed);
        expect(report.maxBatchSize).toBeGreaterThanOrEqual(1);
      } finally {
        await cluster.dispose();
      }
    },
  );

  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5632)",
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.BatchSendBatchConsume",
  );
});
