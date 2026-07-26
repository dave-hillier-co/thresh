// Ported from dotnet/orleans test/Orleans.Streaming.Tests/OrleansRuntime/Streams/ObjectPoolTests.cs @ v10.1.0 (MIT).
//
// Every test white-box-tests `ObjectPool<T>` / `PooledResource<T>` — Orleans'
// internal generic resource pool (`Orleans.Providers.Streams.Common`) used to
// recycle `CachedMessageBlock`/`FixedSizeBuffer` instances in the persistent-
// stream pulling-agent message cache — asserting on allocation/free/reuse
// counts. This framework has no pulling-agent message cache and no generic
// object-pooling utility of any kind, so there is no equivalent internal
// component to port these tests to.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const REASON =
  "white-box unit tests of Orleans' internal ObjectPool<T>/PooledResource<T> generic resource pool (Orleans.Providers.Streams.Common), used to recycle persistent-stream pulling-agent message-cache buffers; this framework has no pulling-agent message cache or generic object-pooling utility to test-access";

const NS = "UnitTests.OrleansRuntime.Streams.ObjectPoolTests";

orleansTest.excluded(REASON, `${NS}.Alloc1Free1Test`);
orleansTest.excluded(REASON, `${NS}.Alloc10Free1Test`);
orleansTest.excluded(REASON, `${NS}.ReuseResourceTest`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
