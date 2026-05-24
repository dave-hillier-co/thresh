import { Guid } from "./guid";

/** A grain key is one of three kinds, mirroring Orleans' key kinds. */
export type GrainKey = string | bigint | Guid;

export type GrainKeyKind = "string" | "integer" | "guid";

export function keyKindOf(key: GrainKey): GrainKeyKind {
  if (typeof key === "string") return "string";
  if (typeof key === "bigint") return "integer";
  return "guid";
}

export function keyToString(key: GrainKey): string {
  return typeof key === "string" ? key : key.toString();
}

export function parseKey(kind: GrainKeyKind, s: string): GrainKey {
  switch (kind) {
    case "string":
      return s;
    case "integer":
      return BigInt(s);
    case "guid":
      return Guid.parse(s);
  }
}
