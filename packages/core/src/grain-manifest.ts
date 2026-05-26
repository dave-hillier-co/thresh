import type { GrainType } from "./grain-type";
import type { SiloAddress } from "./silo-address";

/**
 * The interface versions a silo's registered grain implementations support — the
 * unit a silo advertises to peers so placement can be version-aware (Orleans'
 * `GrainVersionManifest` / `IClusterManifestProvider`). See
 * [ADR 0014](../../docs/adr/0014-grain-interface-versioning.md).
 */
export interface InterfaceVersionEntry {
  /** Name-derived interface id (stable across versions of one interface). */
  interfaceId: number;
  /** The version this silo implements for that interface. */
  version: number;
  /** The grain type that hosts the interface here. */
  grainType: GrainType;
}

export interface SiloManifest {
  silo: SiloAddress;
  entries: readonly InterfaceVersionEntry[];
}

/** The interface version a silo implements, or `undefined` if it hosts none. */
export function supportedVersion(manifest: SiloManifest, interfaceId: number): number | undefined {
  return manifest.entries.find((e) => e.interfaceId === interfaceId)?.version;
}
