// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StatelessWorkersStreamTests.cs @ v10.1.0 (MIT).
// Grain: test/Grains/TestGrains/StatelessWorkerStreamConsumerGrain.cs @ v10.1.0 (MIT).
//
// Asserts that a stateless-worker grain (`IStatelessWorkerStreamConsumerGrain`,
// `[StatelessWorker]`) throws when it tries to become a stream consumer —
// Orleans forbids subscribing from a grain that may have multiple concurrent
// activations, since a stream subscription must bind to a single activation.
// `GrainRuntime.getStreamProvider` (`packages/runtime/src/grain-runtime-impl.ts`)
// now rejects the call outright for a `[StatelessWorker]` activation (mirrors
// Orleans' `SiloStreamProviderRuntime.BindExtension`, which throws
// `InvalidOperationException` for the same reason); this framework has no CLR
// exception hierarchy (see `exception-grain.ts`), so a plain `Error` with a
// matching message stands in, same as every other ported "throws
// InvalidOperationException" assertion in this codebase.
import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { Guid } from "@tsva/core/guid";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";

const STREAM_NAMESPACE = "StatelessWorkerStreamingNamespace";
const STREAM_PROVIDER = "StreamProvider";

interface IStatelessWorkerStreamConsumerGrain extends GrainWithIntegerKey {
  becomeConsumer(streamId: Guid, providerName: string): Promise<void>;
}
const IStatelessWorkerStreamConsumerGrain =
  defineGrainInterface<IStatelessWorkerStreamConsumerGrain>(
    "UnitTests.GrainInterfaces.IStatelessWorkerStreamConsumerGrain",
  );

/** Ported from `StatelessWorkerStreamConsumerGrain`: `[StatelessWorker(1)]`. */
@grain({
  name: "UnitTests.Grains.StatelessWorkerStreamConsumerGrain",
  stateless: true,
  maxLocalWorkers: 1,
})
class StatelessWorkerStreamConsumerGrain
  extends Grain
  implements IStatelessWorkerStreamConsumerGrain
{
  async becomeConsumer(streamId: Guid, providerName: string): Promise<void> {
    const stream = this.runtime
      .getStreamProvider(providerName)
      .getStream<string>(STREAM_NAMESPACE, streamId);
    await stream.subscribe({
      onNext: async () => undefined,
    });
  }
}

describe("UnitTests.StreamingTests.StatelessWorkersStreamTests", () => {
  orleansTest(
    "UnitTests.StreamingTests.StatelessWorkersStreamTests.SubscribeToStream_FromStatelessWorker_Fail",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 1,
        grains: [
          {
            ctor: StatelessWorkerStreamConsumerGrain,
            interfaces: [IStatelessWorkerStreamConsumerGrain],
          },
        ],
      });
      try {
        const consumer = cluster.getGrain(IStatelessWorkerStreamConsumerGrain, 0n);
        await expect(consumer.becomeConsumer(Guid.newGuid(), STREAM_PROVIDER)).rejects.toThrow(
          /StatelessWorker/,
        );
      } finally {
        await cluster.dispose();
      }
    },
  );

  // vitest requires at least one runtime test per file; kept in case the
  // suite above is ever emptied again.
  it.skip("(placeholder — see the orleansTest above)", () => undefined);
});
