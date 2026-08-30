/**
 * A silo's network identity, derived from its Kubernetes pod. `podName` is
 * stable across restarts (used for the directory ring); `podUid` distinguishes
 * a fresh incarnation so stale entries from a dead pod are recognised.
 */
export class SiloAddress {
  constructor(
    readonly podName: string,
    readonly podUid: string,
    readonly endpoint: string,
  ) {}

  /** Stable ring key: the pod name, which survives restarts. */
  get ringKey(): string {
    return this.podName;
  }

  toString(): string {
    return `${this.podName}#${this.podUid}@${this.endpoint}`;
  }

  /**
   * A total order over silo addresses, for any caller that needs a
   * deterministic position in a candidate list: `podName` first (the
   * restart-stable identity `ringKey` already commits to), then `podUid`, then
   * `endpoint`. Mirrors Orleans' `SiloAddress.CompareTo`, which likewise
   * compares component-by-component with a documented precedence.
   *
   * Comparison is ORDINAL (UTF-16 code-unit order), never `localeCompare`:
   * every silo must derive the same order from the same membership view, so
   * the result cannot depend on the host's locale. Returns 0 exactly when
   * {@link equals} is true, so a sorted list never holds the same silo twice
   * under two orderings.
   *
   * Declared as an arrow-function static so it can be passed straight to
   * `Array.prototype.sort` without binding.
   */
  static readonly compare = (a: SiloAddress, b: SiloAddress): number => {
    if (a.podName !== b.podName) return a.podName < b.podName ? -1 : 1;
    if (a.podUid !== b.podUid) return a.podUid < b.podUid ? -1 : 1;
    if (a.endpoint !== b.endpoint) return a.endpoint < b.endpoint ? -1 : 1;
    return 0;
  };

  /** Instance form of {@link SiloAddress.compare} (Orleans `IComparable<SiloAddress>`). */
  compareTo(other: SiloAddress): number {
    return SiloAddress.compare(this, other);
  }

  equals(other: SiloAddress): boolean {
    return (
      this.podName === other.podName &&
      this.podUid === other.podUid &&
      this.endpoint === other.endpoint
    );
  }
}
