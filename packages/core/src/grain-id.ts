import { keyKindOf, keyToString, parseKey, type GrainKey, type GrainKeyKind } from "./grain-key";
import type { GrainType } from "./grain-type";
import { stableHash32 } from "./hash";

/**
 * A grain's stable identity: a (grain type, key text) pair, as in Orleans, whose
 * `GrainId` is a type plus an opaque key span. The string form is "Type/key"; the
 * uniform hash maps the identity onto the directory ring; equality, the catalog
 * key and the directory all use that same text.
 *
 * The key kind is NOT part of identity. `key` and `keyKind` describe how this
 * instance was built, which may differ from the kind the grain type declares;
 * `asKind` reads the key text as the declared kind, as Orleans decides how to
 * read a key from the grain type rather than from the id.
 */
export class GrainId {
  readonly type: GrainType;
  readonly key: GrainKey;
  readonly keyKind: GrainKeyKind;
  private _hash: number | undefined;

  constructor(type: GrainType, key: GrainKey) {
    this.type = type;
    this.key = key;
    this.keyKind = keyKindOf(key);
  }

  toString(): string {
    return `${this.type}/${keyToString(this.key)}`;
  }

  /** Reconstruct from "Type/key"; the key kind disambiguates the key value. */
  static parse(s: string, keyKind: GrainKeyKind | undefined = "string"): GrainId {
    const slash = s.indexOf("/");
    if (slash < 0) throw new Error(`invalid GrainId: ${s}`);
    return new GrainId(s.slice(0, slash), parseKey(keyKind, s.slice(slash + 1)));
  }

  /**
   * This identity with its key read as `kind`. The same grain: only the typed
   * view of the key text changes.
   */
  asKind(kind: GrainKeyKind): GrainId {
    if (kind === this.keyKind) return this;
    const text = keyToString(this.key);
    try {
      return new GrainId(this.type, parseKey(kind, text));
    } catch (cause) {
      throw new Error(`grain key of ${this.toString()} cannot be read as a ${kind} key`, { cause });
    }
  }

  equals(other: GrainId): boolean {
    return this.type === other.type && keyToString(this.key) === keyToString(other.key);
  }

  getUniformHashCode(): number {
    return (this._hash ??= stableHash32(this.toString()));
  }
}
