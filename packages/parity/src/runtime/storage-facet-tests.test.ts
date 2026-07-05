// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StorageFacet/StorageFacetTests.cs @ v10.1.0 (MIT).
//
// Orleans "grain facets" let a grain constructor declare
// `[ExampleStorage("Blob")] IExampleStorage<T> state` parameters that DI
// resolves per-property to independently-configured storage implementations
// (Blob vs Table factories registered via `hostBuilder.UseBlobExampleStorage`
// / `UseTableExampleStorage`, with one nominated the process-wide default via
// `UseAsDefaultExampleStorage<T>`). This framework has `PersistentState<T>`
// as a single built-in facet (see packages/core/src/persistent-state.ts) but
// no general facet-extensibility mechanism for third parties to register
// their own named, DI-resolved per-parameter storage implementations — so
// none of these four cases have anywhere to attach.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.StorageFacetTests", () => {
  orleansTest.gap("GAP-STORAGE-FACET", "Tester.StorageFacetTests.ExampleStorageFacetHappyPath");
  orleansTest.gap("GAP-STORAGE-FACET", "Tester.StorageFacetTests.ExampleStorageFactoryHappyPath");
  orleansTest.gap("GAP-STORAGE-FACET", "Tester.StorageFacetTests.ExampleStorageFacetDefaultPath");
  orleansTest.gap("GAP-STORAGE-FACET", "Tester.StorageFacetTests.ExampleStorageFactoryDefaultPath");
});
