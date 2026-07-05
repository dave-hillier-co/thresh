// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/ClientDirectoryTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// ClientDirectory is the gateway-side directory that tracks which silo hosts
// each connected client's message-routing endpoint and gossips that mapping
// to peer silos via IRemoteClientDirectory system-target calls, driven by a
// DelegateAsyncTimerFactory/Channel test harness and a SystemTargetShared
// bundle of runtime internals (ActivationDirectory, scheduling options,
// grain-reference activator). This framework has no gateway/client-directory
// concept, no IConnectedClientCollection, no system-target RPC layer, and no
// per-silo timer-driven gossip publish loop to substitute for — clients here
// address grains directly through the same request path used cluster-wide
// (packages/directory), with no separate hosted-client routing table.
const NS = "NonSilo.Tests.Directory.ClientDirectoryTests";
const REASON =
  ".NET-specific mechanism: ClientDirectory gossips connected-client routing " +
  "tables between silos via IRemoteClientDirectory system-target RPCs, an " +
  "internal DelegateAsyncTimerFactory-driven publish loop, and a " +
  "SystemTargetShared bundle of runtime internals — this framework has no " +
  "gateway/hosted-client concept, no system-target RPC layer, and no " +
  "per-silo client-routing gossip table to exercise.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.TryLocalLookupTests`);
  orleansTest.excluded(REASON, `${NS}.LocalLookupTests`);
  orleansTest.excluded(REASON, `${NS}.RemoteLookupSuccessTests`);
  orleansTest.excluded(REASON, `${NS}.RemoteLookupFailureTests`);
  orleansTest.excluded(REASON, `${NS}.PublishChangesSuccessTests`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
