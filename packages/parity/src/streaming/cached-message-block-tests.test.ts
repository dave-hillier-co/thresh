// Ported from dotnet/orleans test/Orleans.Streaming.Tests/OrleansRuntime/Streams/CachedMessageBlockTests.cs @ v10.1.0 (MIT).
//
// Every test white-box-tests `CachedMessageBlock` — an internal fixed-capacity
// ring buffer of `CachedMessage` structs used by Orleans' persistent-stream
// pulling-agent message cache (`Orleans.Providers.Streams.Common`) — directly
// against a test `ICacheDataAdapter`/`IObjectPool<CachedMessageBlock>`,
// asserting on its internal index bookkeeping (`OldestMessageIndex`,
// `NewestMessageIndex`, `HasCapacity`, `TryFindFirstMessage`,
// `TryFindNextMessage`, `GetIndexOfFirstMessageLessThanOrEqualTo`). This
// framework has no pulling-agent message cache at all — `MemoryStreamProvider`
// is a direct push-based fan-out with no pooled ring-buffer cache — so there
// is no equivalent internal data structure to port these tests to.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const REASON =
  "white-box unit tests of Orleans' internal CachedMessageBlock ring buffer (Orleans.Providers.Streams.Common), part of the persistent-stream pulling-agent message cache; this framework's MemoryStreamProvider has no pulling agent or pooled message cache to test-access";

const NS = "UnitTests.OrleansRuntime.Streams.CachedMessageBlockTests";

orleansTest.excluded(REASON, `${NS}.Add1Remove1Test`);
orleansTest.excluded(REASON, `${NS}.Add2Remove1UntilFull`);
orleansTest.excluded(REASON, `${NS}.FirstMessageWithSequenceNumberTest`);
orleansTest.excluded(REASON, `${NS}.NextInStreamTest`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
