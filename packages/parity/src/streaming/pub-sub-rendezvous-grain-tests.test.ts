// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/PubSubRendezvousGrainTests.cs @ v10.1.0 (MIT).
//
// Every test drives `IPubSubRendezvousGrain` directly — Orleans' internal
// persistent-storage-backed rendezvous grain that brokers stream producer/
// consumer registration — via `RegisterConsumer`/`UnregisterConsumer`/
// `RegisterProducer`/`UnregisterProducer`, injecting storage faults through
// `IStorageFaultGrain` (`AddFaultOnWrite`/`AddFaultOnClear`) to assert the
// grain surfaces an `OrleansException` and recovers on the next call. This
// framework has no `PubSubRendezvousGrain` (or any persistent system grain
// backing stream subscription state) to address, register against, or
// fault-inject storage errors into — `MemoryStreamProvider` keeps
// subscriptions as plain in-memory provider state (see
// `packages/streams/src/memory-stream-provider.ts`), so there is no grain
// reference, no per-stream consumer/producer counts, and no storage-fault
// grain to wire up.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const REASON =
  "exercises Orleans' internal PubSubRendezvousGrain (persistent-storage-backed stream subscription broker) via IStorageFaultGrain fault injection; this framework's MemoryStreamProvider keeps subscriptions as in-memory provider state with no persistent rendezvous grain or storage-fault grain to address";

const NS = "UnitTests.StreamingTests.PubSubRendezvousGrainTests";

orleansTest.excluded(REASON, `${NS}.RegisterConsumerFaultTest`);
orleansTest.excluded(REASON, `${NS}.UnregisterConsumerFaultTest`);

orleansTest.excluded(
  "skipped upstream: producer management calls require the producer to be a grain reference, which IStreamProducerExtension does not imply (rendezvous implementation TODO upstream)",
  `${NS}.RegisterProducerFaultTest`,
);
orleansTest.excluded(
  "skipped upstream: producer management calls require the producer to be a grain reference, which IStreamProducerExtension does not imply (rendezvous implementation TODO upstream)",
  `${NS}.UnregisterProducerFaultTest`,
);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
