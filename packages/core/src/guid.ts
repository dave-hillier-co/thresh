const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** A GUID/UUID value type, used as one of the three grain key kinds. */
export class Guid {
  private constructor(readonly value: string) {}

  static parse(s: string): Guid {
    if (!UUID_RE.test(s)) throw new Error(`invalid Guid: ${s}`);
    return new Guid(s.toLowerCase());
  }

  static newGuid(): Guid {
    return new Guid(crypto.randomUUID());
  }

  toString(): string {
    return this.value;
  }

  equals(other: Guid): boolean {
    return this.value === other.value;
  }
}
