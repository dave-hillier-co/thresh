// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/MaxHeapTests.cs @ v10.1.0 (MIT).
//
// `HeapPropertyIsMaintained` inserts random elements into the repartitioner's
// internal `MaxHeap<T>`/`IHeapElement<T>` and asserts extraction always
// returns them in descending order. This framework has no repartitioning
// subsystem at all, so `MaxHeap` does not exist
// (GAP-ACTIVATION-REPARTITIONING, see `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.MaxHeapTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.MaxHeapTests.HeapPropertyIsMaintained",
  );
});
