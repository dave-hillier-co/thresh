// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StorageFacet/{Feature.Abstractions,
// Feature.Implementations,Feature.Infrastructure}/*.cs @ v10.1.0 (MIT).
//
// A worked example of a *third-party* grain facet plugin, built entirely on
// the general `@grainFacet`/`addGrainFacetFactory` extensibility point (see
// `packages/core/src/decorators.ts` and
// `packages/hosting/src/silo-builder.ts`) — nothing here is product code.
// Upstream wires this through constructor-parameter DI
// (`[ExampleStorage("Blob")] IExampleStorage<T> state`, resolved by an
// `IAttributeToFactoryMapper<ExampleStorageAttribute>`); this framework has no
// constructor DI, so the plugin instead exposes an `@exampleStorage` FIELD
// decorator — a thin wrapper around `@grainFacet` — bound by the same
// activator that binds `@persistentState`/`@reducerState` fields.
import type { GrainId } from "@thresh/core/grain-id";
import { grainFacet } from "@thresh/core/decorators";
import type { GrainFacetFactory } from "@thresh/persistence/grain-facet-registry";
import type { SiloBuilder } from "@thresh/hosting/silo-builder";

/** The facet family name `addGrainFacetFactory`/`@grainFacet` namespace this plugin's providers under. */
export const EXAMPLE_STORAGE_KIND = "exampleStorage";

/**
 * Feature configuration information an application can provide to the
 * facet per field (Orleans `IExampleStorageConfig`).
 */
export interface ExampleStorageConfig {
  readonly stateName: string;
}

/**
 * Primary storage feature interface — the actual functionality a grain uses
 * (Orleans `IExampleStorage<TState>`).
 */
export interface ExampleStorage<TState> {
  state: TState | undefined;
  save(): Promise<void>;
  // Test-only accessors used to verify facet wiring, mirroring upstream's
  // `Name`/`GetExtendedInfo` (kept on the interface rather than the concrete
  // classes below since both Blob and Table implement them identically).
  readonly name: string;
  getExtendedInfo(): string;
}

/**
 * Injects an `ExampleStorage<TState>` facet into a grain field, resolved by
 * the named factory registered via `useBlobExampleStorage`/
 * `useTableExampleStorage`/`useAsDefaultExampleStorage` below (Orleans'
 * `[ExampleStorage(providerName, stateName)]` constructor-parameter facet).
 * `stateName` defaults to the field name, matching upstream's
 * `ExampleStorageAttributeMapper` (which defaults `StateName` to the
 * constructor parameter's name when not given explicitly).
 */
export function exampleStorage(providerName?: string, stateName?: string) {
  return function (value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("@exampleStorage must decorate a field");
    const config: ExampleStorageConfig = { stateName: stateName ?? String(context.name) };
    grainFacet<ExampleStorageConfig>(EXAMPLE_STORAGE_KIND, providerName, config)(value, context);
  };
}

/**
 * Blob-backed implementation (Orleans `BlobExampleStorage<TState>` /
 * `BlobExampleStorageFactory`) — a no-op store; only proves the facet was
 * bound with the right name.
 */
export const blobExampleStorageFactory: GrainFacetFactory = (
  config: unknown,
): ExampleStorage<unknown> => {
  const name = (config as ExampleStorageConfig).stateName;
  return {
    state: undefined,
    name,
    async save(): Promise<void> {},
    getExtendedInfo(): string {
      return `Blob:${name}, StateType:String`;
    },
  };
};

/**
 * Table-backed implementation (Orleans `TableExampleStorage<TState>` /
 * `TableExampleStorageFactory`) — additionally participates in a "load" step
 * on activation (upstream subscribes `LoadState` to
 * `GrainLifecycleStage.SetupState` via `ILifecycleParticipant<IGrainLifecycle>`;
 * here the factory itself runs as part of activation, before `onActivate`,
 * so it can just perform the load inline).
 */
export const tableExampleStorageFactory: GrainFacetFactory = async (
  config: unknown,
  _grainId: GrainId,
): Promise<ExampleStorage<unknown>> => {
  const name = (config as ExampleStorageConfig).stateName;
  const activated = await Promise.resolve(true); // mirrors upstream's async LoadState
  return {
    state: undefined,
    name,
    async save(): Promise<void> {},
    getExtendedInfo(): string {
      return `Table:${name}-ActivateCalled:${activated}, StateType:String`;
    },
  };
};

/** Registers the Blob implementation as a named provider (Orleans `UseBlobExampleStorage`). */
export function useBlobExampleStorage(builder: SiloBuilder, name: string): SiloBuilder {
  return builder.addGrainFacetFactory(EXAMPLE_STORAGE_KIND, name, blobExampleStorageFactory);
}

/** Registers the Table implementation as a named provider (Orleans `UseTableExampleStorage`). */
export function useTableExampleStorage(builder: SiloBuilder, name: string): SiloBuilder {
  return builder.addGrainFacetFactory(EXAMPLE_STORAGE_KIND, name, tableExampleStorageFactory);
}

/**
 * Nominates `factory` as the process-wide default for an unnamed
 * `@exampleStorage()` field (Orleans `UseAsDefaultExampleStorage<TFactoryType>`).
 */
export function useAsDefaultExampleStorage(
  builder: SiloBuilder,
  factory: GrainFacetFactory,
): SiloBuilder {
  return builder.addGrainFacetFactory(EXAMPLE_STORAGE_KIND, "default", factory);
}
