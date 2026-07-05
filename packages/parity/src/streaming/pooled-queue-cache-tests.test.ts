// Ported from dotnet/orleans test/Orleans.Streaming.Tests/OrleansRuntime/Streams/PooledQueueCacheTests.cs @ v10.1.0 (MIT).
//
// Every test white-box-tests `PooledQueueCache` — Orleans' internal per-queue
// pulling-agent message cache (`Orleans.Providers.Streams.Common`), backed by
// pooled `FixedSizeBuffer`/`CachedMessageBlock` instances, a
// `ChronologicalEvictionStrategy`, and cursor-based per-stream replay
// (`GetCursor`/`TryGetNextMessage`/`RemoveOldestMessage`) — asserting on
// cache-miss avoidance and drain/eviction behaviour. This framework has no
// pulling-agent cache at all: `MemoryStreamProvider` is a direct push-based
// fan-out over an unbounded in-memory event log per stream (see
// `packages/streams/src/memory-stream-provider.ts`), with every subscription
// replaying from its own cursor forever — there is no shared pooled cache, no
// eviction strategy, and so no "cache miss" concept to reproduce.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const REASON =
  "white-box unit tests of Orleans' internal PooledQueueCache pulling-agent message cache (pooled buffers, ChronologicalEvictionStrategy, cursor replay); this framework's MemoryStreamProvider replays each subscription from an unbounded per-stream event log with no shared pooled cache or eviction strategy, so there is no cache-miss concept to test";

const NS = "UnitTests.OrleansRuntime.Streams.PooledQueueCacheTests";

orleansTest.excluded(REASON, `${NS}.GoldenPathTest`);
orleansTest.excluded(REASON, `${NS}.CacheDrainTest`);
orleansTest.excluded(REASON, `${NS}.AvoidCacheMissNotEmptyCache`);
orleansTest.excluded(REASON, `${NS}.AvoidCacheMissEmptyCache`);
orleansTest.excluded(REASON, `${NS}.AvoidCacheMissMultipleStreamsActive`);
orleansTest.excluded(REASON, `${NS}.SimpleCacheMiss`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
