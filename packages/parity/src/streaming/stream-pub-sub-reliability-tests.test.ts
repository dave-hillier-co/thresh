// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StreamPubSubReliabilityTests.cs @ v10.1.0 (MIT).
//
// Every test drives an `ErrorInjectionStorageProvider` wired in as the
// "PubSubStore" grain-storage provider behind Orleans' persistent-stream
// pub/sub registry (a system grain that persists subscription state and is
// exercised here via `BeforeRead`/`BeforeWrite` fault injection), then asserts
// on a `StreamingDiagnosticObserver` to detect when a torn-down subscription's
// removal has propagated. This framework's `MemoryStreamProvider` (see
// `packages/streams/src/memory-stream-provider.ts`) keeps subscriptions as
// plain in-memory state on the provider itself — there is no persistent
// storage-backed pub/sub system grain to fault-inject reads/writes into, no
// `ErrorInjectionStorageProvider`/`IControllable` wiring, and no diagnostic
// observer to synchronize on subscription-removal propagation. The whole
// mechanism under test — grain-storage-backed pub/sub with injectable
// storage faults — does not exist in this framework's streaming architecture.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const REASON =
  "exercises Orleans' persistent-storage-backed pub/sub system grain (ErrorInjectionStorageProvider fault injection into the \"PubSubStore\" provider) and StreamingDiagnosticObserver; this framework's MemoryStreamProvider keeps subscriptions as in-memory provider state with no persistent-storage-backed pub/sub grain to fault-inject and no diagnostic observer";

const NS = "UnitTests.StreamingTests.StreamPubSubReliabilityTests";

orleansTest.excluded(REASON, `${NS}.PubSub_Store_Baseline`);
orleansTest.excluded(REASON, `${NS}.PubSub_Store_ReadError`);
orleansTest.excluded(REASON, `${NS}.PubSub_Store_WriteError`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
