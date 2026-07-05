import { Guid } from "./guid";

/**
 * A compound key: a primary Guid/integer key plus a string "key extension",
 * mirroring Orleans' `IGrainWithGuidCompoundKey` / `IGrainWithIntegerCompoundKey`.
 * Different extensions over the same primary key (or different primaries with
 * the same extension) address distinct grains.
 *
 * Wire encoding (`toString`/`keyToString`): `<primary>+<extension>`, split on
 * the *first* `+` on parse. Neither a Guid's canonical form nor a bigint's
 * decimal form ever contains `+`, so the split is unambiguous regardless of
 * what the extension itself contains (it may contain further `+` characters).
 */
export class CompoundKey<TPrimary extends Guid | bigint = Guid | bigint> {
  readonly primary: TPrimary;
  readonly extension: string;

  constructor(primary: TPrimary, extension: string) {
    if (extension === null || extension === undefined || extension.trim().length === 0) {
      throw new Error(
        `grain key extension must not be null, empty, or whitespace-only (got ${JSON.stringify(extension)})`,
      );
    }
    this.primary = primary;
    this.extension = extension;
  }

  toString(): string {
    return `${this.primary.toString()}+${this.extension}`;
  }

  equals(other: CompoundKey<TPrimary>): boolean {
    return this.toString() === other.toString();
  }
}

/** A grain key is one of Orleans' three plain kinds, or a compound key. */
export type GrainKey = string | bigint | Guid | CompoundKey<Guid> | CompoundKey<bigint>;

export type GrainKeyKind = "string" | "integer" | "guid" | "guid-compound" | "integer-compound";

export function keyKindOf(key: GrainKey): GrainKeyKind {
  if (typeof key === "string") return "string";
  if (typeof key === "bigint") return "integer";
  if (key instanceof CompoundKey)
    return key.primary instanceof Guid ? "guid-compound" : "integer-compound";
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
    case "guid-compound":
    case "integer-compound": {
      const sep = s.indexOf("+");
      if (sep < 0) throw new Error(`invalid compound key: ${s}`);
      const primary = s.slice(0, sep);
      const extension = s.slice(sep + 1);
      return kind === "guid-compound"
        ? new CompoundKey(Guid.parse(primary), extension)
        : new CompoundKey(BigInt(primary), extension);
    }
  }
}
