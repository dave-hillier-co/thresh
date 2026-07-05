// Ported from dotnet/orleans test/Orleans.Streaming.Tests/OrleansRuntime/Streams/FixedSizeBufferTests.cs @ v10.1.0 (MIT).
//
// Every test white-box-tests `FixedSizeBuffer` — an internal pooled
// `ArraySegment<byte>` allocator backing `CachedMessage` payload storage in
// Orleans' persistent-stream pulling-agent message cache
// (`Orleans.Providers.Streams.Common`) — asserting on `TryGetSegment`'s byte-
// offset bookkeeping as it fills. This framework has no pulling-agent message
// cache and no byte-buffer pooling of any kind — `MemoryStreamProvider` holds
// events as plain in-memory JS values, not serialized byte segments — so
// there is no equivalent internal data structure to port these tests to.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const REASON =
  "white-box unit tests of Orleans' internal FixedSizeBuffer pooled byte-segment allocator (Orleans.Providers.Streams.Common), part of the persistent-stream pulling-agent message cache; this framework's MemoryStreamProvider holds events as plain in-memory values with no byte-buffer pooling to test-access";

const NS = "UnitTests.OrleansRuntime.Streams.FixedSizeBufferTests";

orleansTest.excluded(REASON, `${NS}.EmptyBlockGetSegmentTooLargeBvt`);
orleansTest.excluded(REASON, `${NS}.EmptyBlockTryGetMaxSegmentBvt`);
orleansTest.excluded(REASON, `${NS}.FillBlockTestBvt`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
