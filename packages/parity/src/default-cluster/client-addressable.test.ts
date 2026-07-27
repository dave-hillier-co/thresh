// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ClientAddressableTests.cs @ v10.1.0 (MIT).
//
// Client-addressable objects are the same underlying mechanism as grain
// observers (see observer.test.ts): a plain client-side object registered via
// `ClientNode.createObjectReference` so a grain can call back into it through
// an arbitrary interface. The difference from an observer callback is that
// these client-hosted methods return values — the grain awaits a response
// from the client object (request/response), rather than firing a one-way
// notification.
//
// Single-silo cluster: the client's gateway is the only silo, so there is no
// cross-silo gossip-propagation race to contend with — see observer.test.ts
// for that rationale.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import type { ClientNode } from "@thresh/client/client-node";
import { ClientAddressableTestGrain } from "@thresh/parity/grains/impl/client-addressable-test-grain";
import { ClientAddressableTestConsumerGrain } from "@thresh/parity/grains/impl/client-addressable-test-consumer-grain";
import { ClientAddressableTestRendezvousGrain } from "@thresh/parity/grains/impl/client-addressable-test-rendezvous-grain";
import {
  IClientAddressableTestClientObject,
  IClientAddressableTestConsumer,
  IClientAddressableTestGrain,
  IClientAddressableTestProducer,
  IClientAddressableTestRendezvousGrain,
} from "@thresh/parity/grains/interfaces/client-addressable-interfaces";
import { randomIntegerKey } from "@thresh/parity/support/keys";
import { createClusterClient } from "@thresh/parity/support/client";

describe("DefaultCluster.Tests.ClientAddressableTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  const grainRegistrations = [
    { ctor: ClientAddressableTestGrain, interfaces: [IClientAddressableTestGrain] },
    { ctor: ClientAddressableTestConsumerGrain, interfaces: [IClientAddressableTestConsumer] },
    {
      ctor: ClientAddressableTestRendezvousGrain,
      interfaces: [IClientAddressableTestRendezvousGrain],
    },
  ];

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: grainRegistrations,
    });
    client = await createClusterClient(cluster, grainRegistrations);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  function getProxy(): IClientAddressableTestGrain {
    return client.getGrain(IClientAddressableTestGrain, randomIntegerKey());
  }

  orleansTest(
    "DefaultCluster.Tests.ClientAddressableTests.TestClientAddressableHappyPath",
    async () => {
      const clientObject = {
        onHappyPath: async (message: string) => {
          if (!message) throw new Error("target");
          return message;
        },
        onSadPath: async (message: string) => {
          if (!message) throw new Error("target");
          throw new Error(message);
        },
        onSerialStress: async (n: number) => 10000 + n,
        onParallelStress: async (n: number) => 10000 + n,
      };
      const ref = client.createObjectReference(IClientAddressableTestClientObject, clientObject);
      const proxy = getProxy();
      const expected = "o hai!";
      await proxy.setTarget(ref);
      const actual = await proxy.happyPath(expected);
      expect(actual).toBe(expected);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ClientAddressableTests.TestClientAddressableSadPath",
    async () => {
      const message = "o hai!";
      const clientObject = {
        onHappyPath: async (m: string) => {
          if (!m) throw new Error("target");
          return m;
        },
        onSadPath: async (m: string) => {
          if (!m) throw new Error("target");
          throw new Error(m);
        },
        onSerialStress: async (n: number) => 10000 + n,
        onParallelStress: async (n: number) => 10000 + n,
      };
      const ref = client.createObjectReference(IClientAddressableTestClientObject, clientObject);
      const proxy = getProxy();
      await proxy.setTarget(ref);

      await expect(proxy.sadPath(message)).rejects.toThrow(message);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ClientAddressableTests.GrainShouldSuccessfullyPullFromClientObject",
    async () => {
      let counter = 0;
      const producerObject = {
        poll: async () => {
          counter += 1;
          return counter;
        },
      };
      const ref = client.createObjectReference(IClientAddressableTestProducer, producerObject);
      const rendez = client.getGrain(IClientAddressableTestRendezvousGrain, 0n);
      const consumer = client.getGrain(IClientAddressableTestConsumer, 0n);

      await rendez.setProducer(ref);
      await consumer.setup();
      const n = await consumer.pollProducer();
      expect(n).toBe(1);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ClientAddressableTests.MicroClientAddressableSerialStressTest",
    async () => {
      const iterationCount = 1000;
      let counter = 0;
      const clientObject = {
        onHappyPath: async (m: string) => m,
        onSadPath: async () => undefined,
        onSerialStress: async (n: number) => {
          if (counter !== n) {
            throw new Error(`serial stress: expected ${counter}, got ${n}`);
          }
          counter += 1;
          return 10000 + n;
        },
        onParallelStress: async (n: number) => 10000 + n,
      };
      const ref = client.createObjectReference(IClientAddressableTestClientObject, clientObject);
      const proxy = getProxy();
      await proxy.setTarget(ref);
      await proxy.microSerialStressTest(iterationCount);

      client.deleteObjectReference(ref);
    },
    30_000,
  );

  orleansTest(
    "DefaultCluster.Tests.ClientAddressableTests.MicroClientAddressableParallelStressTest",
    async () => {
      const iterationCount = 1000;
      const numbers: number[] = [];
      const clientObject = {
        onHappyPath: async (m: string) => m,
        onSadPath: async () => undefined,
        onSerialStress: async (n: number) => 10000 + n,
        onParallelStress: async (n: number) => {
          numbers.push(n);
          return 10000 + n;
        },
      };
      const ref = client.createObjectReference(IClientAddressableTestClientObject, clientObject);
      const proxy = getProxy();
      await proxy.setTarget(ref);
      await proxy.microParallelStressTest(iterationCount);

      client.deleteObjectReference(ref);

      expect(numbers).toHaveLength(iterationCount);
      const sorted = [...numbers].sort((a, b) => a - b);
      for (let i = 0; i < sorted.length; ++i) {
        expect(sorted[i]).toBe(i);
      }
    },
    30_000,
  );
});
