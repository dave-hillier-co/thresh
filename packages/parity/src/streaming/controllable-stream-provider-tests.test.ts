// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ControllableStreamProviderTests.cs
// and test/Orleans.Streaming.Tests/TestStreamProviders/Controllable/ControllableTestStreamProvider.cs @ v10.1.0 (MIT).
//
// Both tests drive `IManagementGrain.SendControlCommandToProvider` — the
// control-command bus (see `controllable-stream-generator-provider-tests.test.ts`
// and `packages/runtime/src/cluster-node.ts`) — to reach a running stream
// provider's `IControllable.ExecuteCommand` on every silo and have it echo a
// command back. Upstream splits the two echo commands across a queue adapter
// (`AdapterEcho`) and its adapter factory (`AdapterFactoryEcho`); this port has
// no adapter/adapter-factory split, so a single controllable provider echoes
// both. The provider does no actual streaming — it exists only to be addressed
// by name and echo a command, exactly as upstream's `ControllableTestAdapterFactory`.
import { expect } from "vitest";
import type { GrainKey } from "@tsva/core/grain-key";
import { IManagementGrain } from "@tsva/core/management-grain";
import type { AsyncStream, Controllable, StreamProvider } from "@tsva/core/stream";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";

const StreamProviderName = "ControllableTestStreamProvider";

// Orleans `ControllableTestStreamProviderCommands`: `AdapterEcho` sits in the
// adapter command range (`AdapterCommandStartRange` = 10000) and
// `AdapterFactoryEcho` in the factory range (`AdapterFactoryCommandStartRange`
// = 20000), each `+ 1`.
const AdapterEcho = 10001;
const AdapterFactoryEcho = 20001;

/**
 * A test-only stream provider that does no streaming — it exists solely to echo
 * a control command back through `IControllable.executeCommand`, mirroring
 * upstream's `ControllableTestAdapterFactory`. `getStream` throws because
 * nothing subscribes or publishes; the provider is only ever reached by name
 * via the management-grain control-command bus.
 */
class ControllableTestStreamProvider implements StreamProvider, Controllable {
  getStream<T>(_namespace: string, _key: GrainKey): AsyncStream<T> {
    throw new Error("ControllableTestStreamProvider does not support streaming");
  }

  async executeCommand(command: number, arg: unknown): Promise<unknown> {
    if (command === AdapterEcho || command === AdapterFactoryEcho) {
      return [command, arg];
    }
    throw new Error(`ControllableTestStreamProvider: unsupported command ${command}`);
  }
}

async function runEchoTest(command: number): Promise<void> {
  const echoArg = "blarg";
  const cluster = await TestCluster.start({
    initialSilos: 2,
    configureSilo: (builder) => {
      builder.addStreamProvider(StreamProviderName, new ControllableTestStreamProvider());
    },
  });
  try {
    const mgmt = cluster.getGrain(IManagementGrain, 0n);
    const results = await mgmt.sendControlCommandToProvider(
      "PersistentStreamProvider",
      StreamProviderName,
      command,
      echoArg,
    );
    // One echo per silo.
    expect(results).toHaveLength(2);
    for (const echo of results as [number, unknown][]) {
      expect(echo[0]).toBe(command);
      expect(echo[1]).toBe(echoArg);
    }
  } finally {
    await cluster.dispose();
  }
}

orleansTest(
  "UnitTests.StreamingTests.ControllableStreamProviderTests.ControllableAdapterEchoTest",
  async () => {
    await runEchoTest(AdapterEcho);
  },
);

orleansTest(
  "UnitTests.StreamingTests.ControllableStreamProviderTests.ControllableAdapterFactoryEchoTest",
  async () => {
    await runEchoTest(AdapterFactoryEcho);
  },
);
