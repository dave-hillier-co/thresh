import { keyKindOf, keyToString, parseKey, type GrainKey, type GrainKeyKind } from "./grain-key";
import type { GrainType } from "./grain-type";
import { stableHash32 } from "./hash";

/**
 * A grain's stable identity: a (grain type, key) pair. The string form is
 * "Type/key"; the uniform hash maps the identity onto the directory ring.
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
  static parse(s: string, keyKind: GrainKeyKind = "string"): GrainId {
    const slash = s.indexOf("/");
    if (slash < 0) throw new Error(`invalid GrainId: ${s}`);
    return new GrainId(s.slice(0, slash), parseKey(keyKind, s.slice(slash + 1)));
  }

  equals(other: GrainId): boolean {
    return this.keyKind === other.keyKind && this.toString() === other.toString();
  }

  getUniformHashCode(): number {
    return (this._hash ??= stableHash32(this.toString()));
  }
}
