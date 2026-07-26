// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/RequestContextTest.cs @ v10.1.0 (MIT).
// Upstream's namespace is literally `UnitTDefaultCluster.Tests.General` (a typo in the
// original source, preserved here so the id traces back exactly).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import type { ClientNode } from "@thresh/client/client-node";
import { RequestContext } from "@thresh/core/request-context";
import {
  ISimplePersistentGrain,
  SimplePersistentGrain,
} from "@thresh/parity/grains/impl/simple-persistent-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";
import { createClusterClient } from "@thresh/parity/support/client";

describe("UnitTDefaultCluster.Tests.General.RequestContextTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: SimplePersistentGrain, interfaces: [ISimplePersistentGrain] }],
    });
    client = await createClusterClient(cluster, [
      { ctor: SimplePersistentGrain, interfaces: [ISimplePersistentGrain] },
    ]);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  // Upstream sets `RequestContext.Set(...)` directly from the test method (i.e. from a
  // non-grain "client" caller) before invoking the grain, then reads `RequestContext.Keys`
  // back afterwards on the same ambient (.NET AsyncLocal) context. `@thresh/core`'s
  // `RequestContext` (backed by `AsyncLocalStorage.enterWith`) gives a non-grain caller the
  // same ambient-set/read semantics, and the client's `GrainFactory` now injects that ambient
  // bag onto its outgoing calls (same mechanism a grain-to-grain call already used).
  orleansTest(
    "UnitTDefaultCluster.Tests.General.RequestContextTests.RequestContextCallerToCalleeFlow",
    async () => {
      const grain = client.getGrain(ISimplePersistentGrain, randomIntegerKey());
      // Set context to send to the grain.
      RequestContext.set("GrainInfo", "10");
      // This grain method reads the context and returns it.
      const infoFromGrain = await grain.getRequestContext();
      expect(infoFromGrain).toBe("10");

      expect(RequestContext.keys()).toContain("GrainInfo");
    },
  );

  orleansTest.excluded(
    'skipped upstream: [Fact(Skip = "Was failing before (just masked as a Pass), needs fixing or removing")]',
    "UnitTDefaultCluster.Tests.General.RequestContextTests.RequestContextCalleeToCallerFlow",
  );
});
